use std::{
    collections::VecDeque,
    io, mem,
    net::{SocketAddr, UdpSocket},
    os::fd::{AsFd, AsRawFd},
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU64},
    },
};

use core_affinity::CoreId;
use crossbeam_channel::{Sender, TrySendError};
use io_uring::{
    IoUring,
    cqueue::{self, more, notif},
    opcode::SendZc,
    squeue::Entry,
    types,
};
use socket2::SockAddr;

use crate::io_uring::stable_slab::StableSlab;

///
/// A buffer that is registered with the kernel for zero-copy sending.
///
struct RegisteredBuffer {
    index: u16,
    capacity: usize,
    buf: libc::iovec,
}

impl RegisteredBuffer {
    unsafe fn unsafe_clone(&self) -> RegisteredBuffer {
        RegisteredBuffer {
            index: self.index,
            capacity: self.capacity,
            buf: self.buf,
        }
    }
}

unsafe impl Send for RegisteredBuffer {}

///
/// A permit for sending data using a registered buffer.
///
/// Only one thread can own a permit.
///
pub struct FreshSendPermit {
    rbuf: Option<RegisteredBuffer>,
    free_rbuf_tx: crossbeam_channel::Sender<FreshSendPermit>,
}

impl Drop for FreshSendPermit {
    ///
    /// Drop the permit and return the buffer to the free buffer channel.
    ///
    fn drop(&mut self) {
        if let Some(rbuf) = self.rbuf.take() {
            // Return the buffer to the free buffer channel
            let new_permit = FreshSendPermit {
                rbuf: Some(rbuf),
                free_rbuf_tx: self.free_rbuf_tx.clone(),
            };
            if let Err(TrySendError::Full(_)) = self.free_rbuf_tx.try_send(new_permit) {
                unreachable!("Free buffer channel should not be full");
            }
        }
    }
}

///
/// A permit for sending data using a registered buffer.
/// This permit is filled with data and can be sent to multiple destinations.
///
struct FilledSendPermit {
    rbuf: Option<RegisteredBuffer>,
    free_rbuf_tx: crossbeam_channel::Sender<FreshSendPermit>,
}

impl Drop for FilledSendPermit {
    fn drop(&mut self) {
        if let Some(rbuf) = self.rbuf.take() {
            // Return the buffer to the free buffer channel
            let new_permit = FreshSendPermit {
                rbuf: Some(rbuf),
                free_rbuf_tx: self.free_rbuf_tx.clone(),
            };
            if let Err(TrySendError::Full(_)) = self.free_rbuf_tx.try_send(new_permit) {
                unreachable!("Free buffer channel should not be full");
            }
        }
    }
}

impl FilledSendPermit {
    fn send(
        mut self,
        dests: &[SocketAddr],
        send_req_tx: &Sender<SendReqBatch>,
    ) -> Result<(), SendError> {
        let rbuf = self.rbuf.take().expect("send");
        let send_req = SendReq {
            dests: Vec::from_iter(dests.iter().cloned()),
            rbuf,
        };
        let send_req_batch = SendReqBatch {
            batch: vec![send_req],
        };
        send_req_tx
            .send(send_req_batch)
            .map_err(|_| SendError::Disconnected)?;
        Ok(())
    }
}

impl FreshSendPermit {
    fn store<Src>(mut self, src: Src) -> Result<FilledSendPermit, WriteError>
    where
        Src: AsRef<[u8]>,
    {
        if let Some(rbuf) = &mut self.rbuf {
            rbuf.store(src)?;
        } else {
            unreachable!("MulticastSendPermit should always have a buffer");
        }

        let rbuf = self.rbuf.take().expect("store");
        let stored_permit = FilledSendPermit {
            rbuf: Some(rbuf),
            free_rbuf_tx: self.free_rbuf_tx.clone(),
        };
        Ok(stored_permit)
    }

    fn send<Src>(
        self,
        data: Src,
        dests: &[SocketAddr],
        send_req_tx: &Sender<SendReqBatch>,
    ) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let stored_permit = self.store(data)?;
        stored_permit.send(dests, send_req_tx)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteError {
    #[error("attempt to write more than the buffer capacity")]
    Overflow,
}

impl RegisteredBuffer {
    pub fn store<Src>(&mut self, src: Src) -> Result<(), WriteError>
    where
        Src: AsRef<[u8]>,
    {
        let src_len = src.as_ref().len();
        if src_len > self.capacity {
            return Err(WriteError::Overflow);
        }
        unsafe {
            let slice = std::slice::from_raw_parts_mut(self.buf.iov_base as *mut u8, self.capacity);
            slice[..src_len].copy_from_slice(src.as_ref());
            self.buf.iov_len = src_len as _;
        }
        Ok(())
    }
}

///
/// A batch of send requests.
///
pub struct SendReqBatch {
    batch: Vec<SendReq>,
}

///
/// A send request.
///
pub struct SendReq {
    pub dests: Vec<SocketAddr>,
    rbuf: RegisteredBuffer,
}

#[derive(Debug)]
struct SendReqContext {
    #[allow(dead_code)]
    seq_id: u64,
    dest: SockAddr,
    rbuf_index: u16,
}

///
/// Maintain the reference-count to a registered buffer.
/// Does not count its own reference to the [`RegisteredBuffer`].
///
struct RegisteredBufferRc {
    rbuf: RegisteredBuffer,
    rc: u16, /*the number of inflight send req that reference this registered buffer */
}

struct IoUringEvLoop {
    ring: IoUring,
    #[allow(dead_code)]
    send_socket: UdpSocket,
    send_socket_fd: types::Fd,
    backpressured_submissions: VecDeque<Entry>,
    free_buffer_tx: crossbeam_channel::Sender<FreshSendPermit>,
    send_req_ctx_slab: StableSlab<SendReqContext>,
    send_req_rx: crossbeam_channel::Receiver<SendReqBatch>,
    registered_buffers_refcount_vec: Vec<RegisteredBufferRc>,
    linger: usize,
    stats: Arc<GlobalSendStats>,
    disconnected: bool,
    seq_id: u64,
}

#[derive(Debug, Default)]
pub struct GlobalSendStats {
    pub bytes_sent: AtomicU64,
    pub errno_count: AtomicU64,
    pub last_errno: AtomicI32,
    pub zc_send_count: AtomicU64,
    pub copied_send_count: AtomicU64,
    pub submit_count: AtomicU64,
    pub submit_wait_cumu_time_ms: AtomicU64,
    pub inflight_send: AtomicU64,
}

impl IoUringEvLoop {
    fn push_send_req_batch(&mut self, send_req: SendReqBatch) -> usize {
        send_req
            .batch
            .into_iter()
            .fold(0, |acc, req| acc + self.push_send_req(req))
    }

    fn next_seq_id(&mut self) -> u64 {
        let ret = self.seq_id;
        self.seq_id += 1;
        ret
    }

    fn push_send_req(&mut self, send_req: SendReq) -> usize {
        let mut total_submissions = 0;
        let SendReq { dests, rbuf } = send_req;
        let buf_idx = rbuf.index;
        let rbuf_ref_counter = dests.len();
        // Copied from io-uring crate private source code.
        const IORING_SEND_ZC_REPORT_USAGE: u16 = 8;

        for dest in dests {
            let sockaddr = SockAddr::from(dest);
            let ctx = SendReqContext {
                dest: sockaddr,
                rbuf_index: buf_idx,
                seq_id: self.next_seq_id(),
            };
            let slab_key = self.send_req_ctx_slab.insert(ctx);
            let ctx_ref = self.send_req_ctx_slab.get(slab_key).expect("get");
            let entry = SendZc::new(
                self.send_socket_fd,
                rbuf.buf.iov_base as *const _,
                rbuf.buf.iov_len as _,
            )
            .buf_index(Some(buf_idx))
            .dest_addr(ctx_ref.dest.as_ptr() as *const _)
            .dest_addr_len(ctx_ref.dest.len())
            .zc_flags(IORING_SEND_ZC_REPORT_USAGE)
            .build();

            let entry = entry.user_data(slab_key as u64);

            let submission_result = unsafe { self.ring.submission().push(&entry) };

            match submission_result {
                Ok(_) => {
                    // Successfully pushed the operation
                    self.stats
                        .inflight_send
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    total_submissions += 1;
                }
                Err(_e) => {
                    // If we can't push, we need to handle backpressure
                    self.backpressured_submissions.push_back(entry);
                }
            }
        }
        let rbuf_idx = rbuf.index as usize;
        self.registered_buffers_refcount_vec[rbuf_idx].rc += rbuf_ref_counter as u16;
        total_submissions
    }

    fn try_drain_backpressured_submissions(&mut self) -> usize {
        let mut total_submissions = 0;
        loop {
            if self.ring.completion().is_full() {
                // If the completion queue is full, we can't push more operations
                break;
            }
            let Some(op) = self.backpressured_submissions.pop_front() else {
                // No more backpressured submissions to process
                break;
            };
            match unsafe { self.ring.submission().push(&op) } {
                Ok(_) => {
                    // Successfully pushed the operation
                    self.stats
                        .inflight_send
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    total_submissions += 1;
                }
                Err(_e) => {
                    // If we can't push, we need to stop trying
                    self.backpressured_submissions.push_front(op);
                    break;
                }
            }
        }
        total_submissions
    }

    fn dec_used_buffer_ref_cnt(&mut self, buf_index: u16) {
        let rbuf = &mut self.registered_buffers_refcount_vec[buf_index as usize];
        // Decrease the reference counter
        rbuf.rc = rbuf.rc.checked_sub(1).expect("release_used_buffer");
        log::trace!("registered buf {} - {} refs", buf_index, rbuf.rc);
        if rbuf.rc == 0 {
            log::trace!("Releasing used buffer: {}", rbuf.rbuf.index);
            // The channel capacity should be enough to hold all buffers
            // so we can safely send the buffer back to the free buffer channel
            let rbuf2 = unsafe { rbuf.rbuf.unsafe_clone() };
            let new_permit = FreshSendPermit {
                rbuf: Some(rbuf2),
                free_rbuf_tx: self.free_buffer_tx.clone(),
            };
            match self.free_buffer_tx.try_send(new_permit) {
                Ok(_) => {}
                Err(TrySendError::Disconnected(_)) => {
                    self.disconnected = true;
                    return;
                }
                Err(TrySendError::Full(_)) => {
                    panic!("Free buffer channel should not be full");
                }
            }
        }
    }

    fn process_completion_queue(&mut self) {
        enum CqeResult {
            More {
                byte_sent: u32,
                #[allow(dead_code)]
                slab_key: usize,
                err: Option<std::io::Error>,
            },
            Completed {
                slab_key: usize,
                zero_copy: bool,
                err: Option<std::io::Error>,
            },
        }

        impl From<cqueue::Entry> for CqeResult {
            fn from(cqe: cqueue::Entry) -> Self {
                let res = cqe.result();
                let user_data = cqe.user_data();

                if more(cqe.flags()) {
                    let err = if res < 0 {
                        Some(std::io::Error::from_raw_os_error(-res))
                    } else {
                        None
                    };
                    let byte_sent = if res < 0 { 0 } else { res as _ };
                    let slab_key = user_data as usize;
                    CqeResult::More {
                        byte_sent,
                        err,
                        slab_key,
                    }
                } else if notif(cqe.flags()) {
                    // ZC report usage doc: https://github.com/axboe/liburing/blob/e755a5f8614add34f44f0e89d1d1c1b2d21f1384/src/include/liburing/io_uring.h#L360
                    const IORING_NOTIF_USAGE_ZC_COPIED: u32 = 1u32 << 31;
                    let zero_copy = (res as u32) & IORING_NOTIF_USAGE_ZC_COPIED == 0;
                    let slab_key = user_data as usize;
                    // Strip zero-copy flag
                    let res2 = ((res as u32) & !IORING_NOTIF_USAGE_ZC_COPIED) as i32;
                    let err = if res2 < 0 {
                        Some(std::io::Error::from_raw_os_error(-res2))
                    } else {
                        None
                    };
                    CqeResult::Completed {
                        slab_key,
                        zero_copy,
                        err,
                    }
                } else {
                    panic!("cqe.flags(): {:?}", cqe.flags())
                }
            }
        }

        let cqe_vec = {
            let mut cq = self.ring.completion();
            cq.sync();
            // TODO: create a slab for this
            let mut cqe_vec = Vec::with_capacity(cq.len());
            for cqe in cq {
                let cqe = cqe.into();
                cqe_vec.push(cqe);
            }
            cqe_vec
        };

        for cqe in cqe_vec {
            match cqe {
                CqeResult::More {
                    byte_sent,
                    err,
                    slab_key: _,
                } => {
                    log::trace!("SendZc more: {} bytes sent", byte_sent);
                    self.stats
                        .bytes_sent
                        .fetch_add(byte_sent as _, std::sync::atomic::Ordering::Relaxed);
                    if let Some(e) = err {
                        self.stats.last_errno.store(
                            e.raw_os_error().unwrap_or(0),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        self.stats
                            .errno_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        log::error!("SendZc failed with -errno: {e:?}");
                    }
                }
                CqeResult::Completed {
                    slab_key,
                    zero_copy,
                    err,
                } => {
                    let ctx = self.send_req_ctx_slab.remove(slab_key);
                    self.dec_used_buffer_ref_cnt(ctx.rbuf_index);
                    self.stats
                        .inflight_send
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    if zero_copy {
                        log::trace!("SendZc completed with zero-copy");
                        self.stats
                            .zc_send_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else if err.is_none() {
                        log::debug!("sendzc failed to used zero-copy");
                        self.stats
                            .copied_send_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    if let Some(e) = err {
                        log::error!("SendZc failed with -errno: {e:?}");
                        self.stats
                            .errno_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn fill_submissions(&mut self) -> usize {
        if self.ring.completion().is_full() {
            return 0;
        }
        let mut total_submissions = 0;
        self.try_drain_backpressured_submissions();

        while !self.ring.submission().is_full() {
            let send_req = match self.send_req_rx.try_recv() {
                Ok(req) => req,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    return total_submissions;
                }
            };
            total_submissions += self.push_send_req_batch(send_req);
        }
        total_submissions
    }

    fn drain_process_cqe(&mut self) {
        log::debug!("Draining completion queue");
        while self
            .stats
            .inflight_send
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            self.process_completion_queue();
        }
    }

    pub fn ev_loop(mut self) {
        while !self.disconnected {
            self.process_completion_queue();
            let total_submission = self.fill_submissions();
            if total_submission > 0 {
                log::trace!("Filled {total_submission} submissions");
            }
            if self.ring.submission().len() >= self.linger {
                let t = std::time::Instant::now();
                match self.ring.submit_and_wait(1) {
                    Ok(submitted) => {
                        // You need to sync here otherwise you get unknown OS error codes.
                        // the doc does not say why, but it is likely due to the way the kernel handles
                        // the submission queue.
                        self.ring.submission().sync();
                        let elapsed = t.elapsed();
                        self.stats
                            .submit_count
                            .fetch_add(submitted as u64, std::sync::atomic::Ordering::Relaxed);
                        self.stats.submit_wait_cumu_time_ms.fetch_add(
                            elapsed.as_millis() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    Err(e) => {
                        if e.raw_os_error() == Some(libc::EBUSY) {
                            log::debug!("EBUSY");
                            // need to remove some completion queue entry next loop.
                        } else {
                            log::error!("Failed to submit  {e:?}");
                        }
                    }
                }
            }
        }

        self.drain_process_cqe();
    }
}

///
/// Zero-Copy Multicast Sender.
///
/// This struct is responsible for sending multicast messages using zero-copy techniques.
/// It uses io-uring SendZc with registered buffers for optimal efficiency.
///
/// In case your NIC does not support Zero-copy, fallback to traditional send.
///
/// # Blocking
///
/// The sender avoids blocking as much as possible.
/// As long as there are free registered buffers to use, the sender should not block.
///
/// A dedicated thread is responsible for filling the submission queue and submitting requests for io-uring, with no-sleep, no IRQ and least amount of of syscalls.
///
#[derive(Debug, Clone)]
pub struct ZcMulticastSender {
    free_buffer_rx: crossbeam_channel::Receiver<FreshSendPermit>,
    send_req_tx: crossbeam_channel::Sender<SendReqBatch>,
    stats: Arc<GlobalSendStats>,
}

pub const DEFAULT_NUM_REGISTERED_BUFFERS: u16 = 10000;
pub const DEFAULT_MTU_SIZE: usize = 1500;
pub const DEFAULT_IORING_QUEUE_SIZE: u32 = 1024;
pub const DEFAULT_LINGER: usize = 1;
pub const DEFAULT_SEND_REQ_CTX_SLAB_MULTIPLER: usize = 15;

#[derive(Debug, Clone)]
pub struct ZcMulticastSenderConfig {
    pub num_registered_buffers: u16,
    pub registered_buffer_size: usize,
    ///
    /// Queue depth of the submission queue.
    /// Note: must be a power of two.
    pub ioring_queue_size: u32,
    ///
    /// Mimimim size to wait for submission queue to be before submitting to the kernel.
    ///
    pub linger: usize,

    ///
    /// Core ID to pin [`IoUring`]'s submission/completion event loop.
    ///
    pub core_id: CoreId,
    ///
    /// Internally, we use a slab to store inflight send req, this is because
    /// io-uring still need to access dest address information from a stable pointer.
    /// the slab is fixed size and panic if we try to send more data then it can chew.
    /// The [`ZcMulticastSenderConfig::send_req_ctx_slab_mult`] field controls the size of the slab.
    /// The slab size is the number of `num_registered_buffers` * `send_req_ctx_slab_mult`.
    ///
    /// Sizing hint: use a multipler that fit the number of destination address you want to support.
    ///
    pub send_req_ctx_slab_mult: usize,
}

impl Default for ZcMulticastSenderConfig {
    fn default() -> Self {
        ZcMulticastSenderConfig {
            num_registered_buffers: DEFAULT_NUM_REGISTERED_BUFFERS,
            registered_buffer_size: DEFAULT_MTU_SIZE,
            ioring_queue_size: DEFAULT_IORING_QUEUE_SIZE,
            linger: DEFAULT_LINGER,
            core_id: core_affinity::get_core_ids().unwrap()[0],
            send_req_ctx_slab_mult: DEFAULT_SEND_REQ_CTX_SLAB_MULTIPLER,
        }
    }
}

impl ZcMulticastSenderConfig {
    pub fn num_registered_buffers(mut self, num: u16) -> Self {
        self.num_registered_buffers = num;
        self
    }

    pub fn registered_buffer_size(mut self, size: usize) -> Self {
        self.registered_buffer_size = size;
        self
    }

    pub fn linger(mut self, linger: usize) -> Self {
        self.linger = linger;
        self
    }

    pub fn core_id(mut self, core_id: CoreId) -> Self {
        self.core_id = core_id;
        self
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    #[error("disconnected from io-uring event loop")]
    Disconnected,
    #[error(transparent)]
    WriteError(#[from] WriteError),
}

struct SendReqBatcher {
    batch: Vec<FilledSendPermit>,
    dests: Vec<SocketAddr>,
}

impl SendReqBatcher {
    fn send(self, send_req_tx: &Sender<SendReqBatch>) -> Result<(), SendError> {
        let batch = self
            .batch
            .into_iter()
            .map(|mut stored_permit| {
                let rbuf = stored_permit.rbuf.take().expect("into_send_req_batch");
                SendReq {
                    dests: self.dests.clone(),
                    rbuf,
                }
            })
            .collect();
        send_req_tx
            .send(SendReqBatch { batch })
            .map_err(|_| SendError::Disconnected)
    }
}

impl ZcMulticastSender {
    pub fn send<Src>(&self, src: Src, dests: &[SocketAddr]) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let data = src.as_ref();
        let permit = self
            .free_buffer_rx
            .recv()
            .map_err(|_| SendError::Disconnected)?;
        permit.send(data, dests, &self.send_req_tx)
    }

    ///
    /// Cross-product between the src_vec and dests.
    /// Each src in src_vec is sent to each dest in dests.
    /// This function will block until all messages are sent.
    ///
    /// Try to build the biggest batch possible if enough permits are available to do so.
    pub fn cross_batch_send<Src>(
        &self,
        src_vec: &[Src],
        dests: &[SocketAddr],
    ) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let mut num_msg = src_vec.len();

        let mut permits = Vec::with_capacity(num_msg);
        let mut src_i: usize = 0;
        while num_msg > 0 {
            'inner: while num_msg > 0 {
                match self.free_buffer_rx.try_recv() {
                    Ok(permit) => {
                        permits.push(permit);
                        num_msg -= 1;
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        break 'inner;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        return Err(SendError::Disconnected);
                    }
                }
            }

            if permits.is_empty() {
                // prepare to block until at least one permit is available
                match self.free_buffer_rx.recv() {
                    Ok(permit) => {
                        permits.push(permit);
                        num_msg -= 1;
                    }
                    Err(_) => {
                        return Err(SendError::Disconnected);
                    }
                }
            }
            let mut batcher = SendReqBatcher {
                batch: Vec::with_capacity(permits.len()),
                dests: Vec::from_iter(dests.iter().cloned()),
            };
            for permit in permits.drain(..) {
                let data = src_vec.get(src_i).expect("batch_send");
                src_i += 1;
                let stored = permit.store(data)?;
                batcher.batch.push(stored);
            }
            batcher.send(&self.send_req_tx)?;
        }
        Ok(())
    }

    ///
    /// Returns a owned reference to the global send stats.
    ///
    pub fn global_shared_stats(&self) -> Arc<GlobalSendStats> {
        Arc::clone(&self.stats)
    }
}

pub fn zc_multicast_sender(send_socket: UdpSocket) -> io::Result<ZcMulticastSender> {
    zc_multicast_sender_with_config(send_socket, ZcMulticastSenderConfig::default())
}

pub fn zc_multicast_sender_with_config(
    send_socket: UdpSocket,
    config: ZcMulticastSenderConfig,
) -> io::Result<ZcMulticastSender> {
    let ZcMulticastSenderConfig {
        num_registered_buffers,
        registered_buffer_size,
        ioring_queue_size,
        linger,
        core_id,
        send_req_ctx_slab_mult,
    } = config;

    let ring = IoUring::builder()
        .setup_cqsize(ioring_queue_size * 2)
        .setup_sqpoll(100)
        .build(ioring_queue_size)?;
    log::trace!(
        "zc_multicast_sender_with_config: io_uring queue size: {}",
        ioring_queue_size
    );
    let (send_req_tx, send_req_rx) = crossbeam_channel::bounded(1000);
    let (free_buffer_tx, free_buffer_rx) = crossbeam_channel::bounded(num_registered_buffers as _);

    let mut rbuf_vec = Vec::with_capacity(num_registered_buffers as _);
    for i in 0..num_registered_buffers {
        // intermediate registered buffer
        let mut irbuf = vec![0u8; registered_buffer_size];
        let rbuf = RegisteredBuffer {
            index: i,
            capacity: registered_buffer_size,
            buf: libc::iovec {
                iov_base: irbuf.as_mut_ptr() as *mut _,
                iov_len: registered_buffer_size,
            },
        };
        mem::forget(irbuf); // Prevent the buffer from being dropped
        rbuf_vec.push(rbuf);
    }

    let mut iovecs = Vec::with_capacity(num_registered_buffers as _);
    for rbuf in rbuf_vec.iter() {
        let iovec = rbuf.buf;
        iovecs.push(iovec);
    }

    unsafe { ring.submitter().register_buffers(&iovecs)? };
    log::trace!(
        "zc_multicast_sender_with_config: registered {} buffers of size {}",
        num_registered_buffers,
        registered_buffer_size
    );

    let mut rbuf_rc_vec = vec![];
    for rbuf in rbuf_vec {
        rbuf_rc_vec.push(RegisteredBufferRc {
            rbuf: unsafe { rbuf.unsafe_clone() },
            rc: 0,
        });
        let permit = FreshSendPermit {
            rbuf: Some(rbuf),
            free_rbuf_tx: free_buffer_tx.clone(),
        };
        free_buffer_tx
            .try_send(permit)
            .expect("Failed to send registered buffer");
    }

    let send_socket_fd = types::Fd(send_socket.as_fd().as_raw_fd());
    let stats = Arc::new(GlobalSendStats::default());
    let slab_fixed_cap = num_registered_buffers as usize * send_req_ctx_slab_mult;
    let send_req_ctx_slab = StableSlab::with_capacity(slab_fixed_cap);
    let ev_loop = IoUringEvLoop {
        ring,
        send_socket,
        send_socket_fd,
        backpressured_submissions: VecDeque::with_capacity(num_registered_buffers as _),
        free_buffer_tx,
        send_req_rx,
        send_req_ctx_slab,
        registered_buffers_refcount_vec: rbuf_rc_vec,
        linger,
        stats: Arc::clone(&stats),
        disconnected: false,
        seq_id: 0,
    };

    log::trace!(
        "zc_multicast_sender_with_config: starting event loop on core {:?}",
        core_id
    );
    let _jh = std::thread::spawn(move || {
        core_affinity::set_for_current(core_id);
        ev_loop.ev_loop();
    });

    Ok(ZcMulticastSender {
        free_buffer_rx,
        send_req_tx,
        stats,
    })
}

#[cfg(test)]
mod tests {
    use log::{LevelFilter, Metadata, Record, SetLoggerError};
    use rand::{RngCore, rng};
    use solana_net_utils::bind_to_localhost;
    use solana_streamer::{packet::Packet, recvmmsg::recv_mmsg};

    use crate::io_uring::sendmmsg::zc_multicast_sender;

    #[allow(dead_code)]
    static LOGGER: SimpleLogger = SimpleLogger;

    #[allow(dead_code)]
    pub fn init() -> Result<(), SetLoggerError> {
        log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Trace))
    }

    #[allow(dead_code)]
    struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= log::max_level()
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!(
                    "{} - line_no: {} - {}",
                    record.level(),
                    record.line().unwrap_or(0),
                    record.args()
                );
            }
        }

        fn flush(&self) {}
    }

    #[test]
    pub fn test_send_one_msg() {
        log::error!("Starting multicast message test");
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender_socket = bind_to_localhost().expect("bind");
        let mc_sender = zc_multicast_sender(sender_socket).expect("zc_multicast_sender");
        let packet = Packet::default();
        mc_sender
            .send(packet.data(..).unwrap(), &[addr])
            .expect("send multicast");

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap();
        assert_eq!(1, recv);
    }

    #[test]
    pub fn test_multicast() {
        init().expect("init logger");
        log::error!("Starting multicast message test");
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = bind_to_localhost().expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let reader3 = bind_to_localhost().expect("bind");
        let addr3 = reader3.local_addr().unwrap();

        let reader4 = bind_to_localhost().expect("bind");
        let addr4 = reader4.local_addr().unwrap();

        let sender_socket = bind_to_localhost().expect("bind");
        let mc_sender = zc_multicast_sender(sender_socket).expect("zc_multicast_sender");
        let mut expected_packet = vec![0u8; 1232];

        let mut rng = rng();
        rng.fill_bytes(expected_packet.as_mut_slice());

        mc_sender
            .send(&expected_packet, &[addr, addr2, addr3, addr4])
            .expect("send multicast");
        for rdr in [reader, reader2, reader3, reader4].iter() {
            let mut packets = vec![Packet::default(); 32];
            log::trace!("Receiving packets on {:?}", rdr.local_addr());
            let recv = recv_mmsg(rdr, &mut packets[..]).unwrap();
            assert_eq!(1, recv);
            let actual = packets[0].data(..).unwrap();
            assert_eq!(actual.len(), expected_packet.len());
            assert_eq!(*actual, expected_packet[..]);
        }
    }
}
