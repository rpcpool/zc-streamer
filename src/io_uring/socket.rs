use crate::io_uring::stable_slab::StableSlab;
use bytes::Bytes;
use core_affinity::CoreId;
use crossbeam_channel::{Sender, TrySendError};
use io_uring::{
    IoUring,
    cqueue::{self, more, notif},
    opcode,
    squeue::Entry,
    types,
};
use socket2::SockAddr;
use solana_packet::Packet;
use solana_streamer::recvmmsg::recv_mmsg;
use std::{
    alloc::{Layout, alloc},
    collections::VecDeque,
    io,
    net::{SocketAddr, UdpSocket},
    os::fd::{AsFd, AsRawFd},
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU64},
    },
    thread::{JoinHandle, yield_now},
    time::{Duration, Instant},
};

pub const IPV4_COMMON_UDP_MTU: usize = 1300;
pub const IPV4_UDP_METADATA_SIZE: usize = 28; // 20 (IP header) + 8 (UDP header)

pub const IPV4_PACKET_MTU: usize = IPV4_COMMON_UDP_MTU - IPV4_UDP_METADATA_SIZE; // 1300 - 20 (UDP + IP headers)

static URING_EV_LOOP_THREAD_COUNTER: AtomicU64 = AtomicU64::new(0);

///
/// A buffer that is registered with the kernel for zero-copy sending.
///
#[derive(Debug)]
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
            buf: BufferFlavor::Registered(rbuf),
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

#[derive(Debug)]
enum BufferFlavor {
    Registered(RegisteredBuffer),
    Userspace(Bytes),
}

///
/// A send request.
///
pub struct SendReq {
    pub dests: Vec<SocketAddr>,
    buf: BufferFlavor,
}

impl From<SendReq> for SendReqBatch {
    fn from(req: SendReq) -> Self {
        SendReqBatch { batch: vec![req] }
    }
}

#[derive(Debug)]
struct SendReqContext {
    dest: SockAddr,
    buffer: BufferFlavor,
}

///
/// Maintain the reference-count to a registered buffer.
/// Does not count its own reference to the [`RegisteredBuffer`].
///
struct RegisteredBufferRc {
    rbuf: RegisteredBuffer,
    rc: u16, /*the number of inflight send req that reference this registered buffer */
}

enum CqeResult {
    ZcMore {
        byte_sent: u32,
        #[allow(dead_code)]
        slab_key: usize,
        err: Option<std::io::Error>,
    },
    ZcCompleted {
        slab_key: usize,
        zero_copy: bool,
        err: Option<std::io::Error>,
    },
    SendResult {
        slab_key: usize,
        result: Result<usize, std::io::Error>,
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
            CqeResult::ZcMore {
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
            CqeResult::ZcCompleted {
                slab_key,
                zero_copy,
                err,
            }
        } else {
            // Normal Send
            let slab_key = user_data as usize;
            let result = if res < 0 {
                Err(std::io::Error::from_raw_os_error(-res))
            } else {
                Ok(res as usize)
            };
            CqeResult::SendResult { slab_key, result }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmitLingerConfig {
    ///
    /// The ratio of the submission queue fill level to the total capacity
    /// at which we call [`IoUring::submit`].
    ///
    pub threshold_ratio: f64,
    ///
    /// The duration to wait before submitting the queued requests since last [`IoUring::submit`] call.
    ///
    pub tick_duration: Duration,
}
pub const DEFAULT_SUBMIT_THRESHOLD_RATIO: f64 = 0.25;
pub const DEFAULT_SUBMIT_TICK_DURATION: Duration = Duration::from_millis(1);

impl Default for SubmitLingerConfig {
    fn default() -> Self {
        SubmitLingerConfig {
            threshold_ratio: DEFAULT_SUBMIT_THRESHOLD_RATIO,
            tick_duration: DEFAULT_SUBMIT_TICK_DURATION,
        }
    }
}

struct UringEvLoop {
    ring: IoUring,
    #[allow(dead_code)]
    send_socket: UdpSocket,
    send_socket_fd: types::Fd,
    backpressured_submissions: VecDeque<Entry>,
    free_buffer_tx: crossbeam_channel::Sender<FreshSendPermit>,
    send_req_ctx_slab: StableSlab<SendReqContext>,
    send_req_rx: crossbeam_channel::Receiver<SendReqBatch>,
    registered_buffers_refcount_vec: Vec<RegisteredBufferRc>,
    unprocess_completion_queue: Vec<CqeResult>,
    submit_linger_config: SubmitLingerConfig,
    stats: Arc<GlobalSendStats>,
    disconnected: bool,
    #[allow(dead_code)]
    _rbuf_raw_ptr_vec: Vec<Box<u8>>, /*DROP WHEN IoUringEvLoop is dropped */
}

#[derive(Debug, Default)]
pub struct GlobalSendStats {
    pub bytes_sent: AtomicU64,
    pub errno_count: AtomicU64,
    pub last_errno: AtomicI32,
    pub zc_send_count: AtomicU64,
    pub copied_send_count: AtomicU64,
    pub submit_wait_cumu_time_ms: AtomicU64,
    pub inflight_send: AtomicU64,
}

impl UringEvLoop {
    #[inline]
    fn push_send_req_batch(&mut self, send_req: SendReqBatch) -> usize {

        let mut acc = 0;
        for req in send_req.batch {
            let SendReq { dests, buf } = req;
            acc += match buf {
                BufferFlavor::Registered(rbuf) => self.push_send_req_zc(dests, rbuf),
                BufferFlavor::Userspace(buf) => self.push_send_req_copied(dests, buf),
            };
        }

        acc
    }
    
    #[inline]
    fn push_send_req_copied(&mut self, dests: Vec<SocketAddr>, buf: Bytes) -> usize {
        let mut total_submissions = 0;

        for dest in dests {
            let sockaddr = SockAddr::from(dest);
            let ctx = SendReqContext {
                dest: sockaddr,
                buffer: BufferFlavor::Userspace(buf.clone()),
            };
            let slab_key = self.send_req_ctx_slab.insert(ctx);
            let ctx_ref = self.send_req_ctx_slab.get(slab_key).expect("get");
            let entry = opcode::Send::new(
                self.send_socket_fd,
                buf.as_ptr() as *const _,
                buf.len() as _,
            )
            .dest_addr(ctx_ref.dest.as_ptr() as *const _)
            .dest_addr_len(ctx_ref.dest.len())
            .build();

            let entry = entry.user_data(slab_key as u64);

            let submission_result = unsafe { self.ring.submission().push(&entry) };

            match submission_result {
                Ok(_) => {
                    self.ring.submission().sync();
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
        total_submissions
    }

    fn push_send_req_zc(&mut self, dests: Vec<SocketAddr>, rbuf: RegisteredBuffer) -> usize {
        let mut total_submissions = 0;
        let buf_idx = rbuf.index;
        let rbuf_ref_counter = dests.len();
        // Copied from io-uring crate private source code.
        const IORING_SEND_ZC_REPORT_USAGE: u16 = 8;

        for dest in dests {
            let sockaddr = SockAddr::from(dest);
            let ctx = SendReqContext {
                dest: sockaddr,
                buffer: BufferFlavor::Registered(unsafe { rbuf.unsafe_clone() }),
            };
            let slab_key = self.send_req_ctx_slab.insert(ctx);
            let ctx_ref = self.send_req_ctx_slab.get(slab_key).expect("get");
            let entry = opcode::SendZc::new(
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
                    self.ring.submission().sync();
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

        while !self.ring.submission().is_full() && !self.ring.completion().is_full() {
            let Some(op) = self.backpressured_submissions.pop_front() else {
                // No more backpressured submissions to process
                break;
            };
            match unsafe { self.ring.submission().push(&op) } {
                Ok(_) => {
                    self.ring.submission().sync();
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

    fn drain_completion_queue(&mut self) {
        let mut cq = self.ring.completion();
        cq.sync();
        // TODO: create a slab for this
        for cqe in cq {
            self.unprocess_completion_queue.push(cqe.into());
        }
    }

    fn drain_and_process_cqe(&mut self) {
        self.drain_completion_queue();
        self.process_cqe_results();
    }

    fn process_cqe_results(&mut self) {
        let mut unprocessed_cq = std::mem::take(&mut self.unprocess_completion_queue);
        while let Some(cqe) = unprocessed_cq.pop() {
            match cqe {
                CqeResult::ZcMore {
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
                CqeResult::ZcCompleted {
                    slab_key,
                    zero_copy,
                    err,
                } => {
                    let ctx = self.send_req_ctx_slab.remove(slab_key);
                    let BufferFlavor::Registered(rbuf) = ctx.buffer else {
                        panic!("should be rbuf")
                    };
                    self.dec_used_buffer_ref_cnt(rbuf.index);
                    if zero_copy {
                        log::trace!("SendZc completed with zero-copy");
                        self.stats
                            .zc_send_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else if err.is_none() {
                        log::trace!("sendzc failed to used zero-copy");
                        self.stats
                            .copied_send_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    self.stats
                        .inflight_send
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

                    if let Some(e) = err {
                        log::error!("SendZc failed with -errno: {e:?}");
                        self.stats
                            .errno_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                CqeResult::SendResult { slab_key, result } => {
                    let ctx = self.send_req_ctx_slab.remove(slab_key);
                    let BufferFlavor::Userspace(_) = ctx.buffer else {
                        panic!("should be userspace buf")
                    };
                    match result {
                        Ok(bytes_sent) => {
                            log::trace!("Send completed: {} bytes sent", bytes_sent);
                            self.stats
                                .bytes_sent
                                .fetch_add(bytes_sent as _, std::sync::atomic::Ordering::Relaxed);
                            self.stats
                                .copied_send_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            log::error!("Send failed with -errno: {e:?}");
                            self.stats
                                .errno_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.stats.last_errno.store(
                                e.raw_os_error().unwrap_or(0),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                    }
                    self.stats
                        .inflight_send
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        let _ = std::mem::replace(&mut self.unprocess_completion_queue, unprocessed_cq);
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
                    log::info!("send_req_rx disconnected");
                    self.disconnected = true;
                    return total_submissions;
                }
            };
            total_submissions += self.push_send_req_batch(send_req);
        }
        total_submissions
    }

    fn drain_process_cqe(&mut self) {

        let _ = self.ring.submit();
        self.ring.submission().sync();
        while self
            .stats
            .inflight_send
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            self.drain_and_process_cqe();
        }
    }

    pub fn ev_loop(mut self) {
        let submit_linger_config = self.submit_linger_config.clone();
        let max_sq_capacity = self.ring.submission().capacity();
        let sq_fill_threshold =
            (max_sq_capacity as f64 * submit_linger_config.threshold_ratio) as usize;
        let mut last_submit = Instant::now();
        while !self.disconnected {
            self.drain_and_process_cqe();
            let total_submission = self.fill_submissions();
            if total_submission > 0 {
                log::trace!("Filled {total_submission} submissions");
            }
            let elapsed = last_submit.elapsed();
            if self.ring.submission().is_full()
                || self.ring.submission().len() >= sq_fill_threshold
                || elapsed >= submit_linger_config.tick_duration
            {
                last_submit = Instant::now();
                match self.ring.submit() {
                    Ok(_submitted) => {
                        // You need to sync here otherwise you get unknown OS error codes.
                        // the doc does not say why, but it is likely due to the way the kernel handles
                        // the submission queue.
                        // self.ring.submission().sync();
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
        log::info!("UringEvLoop disconnected, draining remaining completions, inflights: {}", self.stats.inflight_send.load(std::sync::atomic::Ordering::Relaxed));
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
pub struct UringSocket {
    free_rbuffer_rx: crossbeam_channel::Receiver<FreshSendPermit>,
    send_req_tx: crossbeam_channel::Sender<SendReqBatch>,
    stats: Arc<GlobalSendStats>,
    packet_mtu: usize,
    ev_loop_thread_idx: u64,
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
    /// See [`SubmitLingerConfig`] for more details.
    ///
    pub linger: SubmitLingerConfig,

    ///
    /// Core ID to pin [`IoUring`]'s submission/completion event loop.
    ///
    pub core_id: Option<CoreId>,
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

    pub packet_mtu: usize,
}

impl Default for ZcMulticastSenderConfig {
    fn default() -> Self {
        ZcMulticastSenderConfig {
            num_registered_buffers: DEFAULT_NUM_REGISTERED_BUFFERS,
            registered_buffer_size: DEFAULT_MTU_SIZE,
            ioring_queue_size: DEFAULT_IORING_QUEUE_SIZE,
            linger: Default::default(),
            core_id: core_affinity::get_core_ids().and_then(|ids| ids.first().cloned()),
            send_req_ctx_slab_mult: DEFAULT_SEND_REQ_CTX_SLAB_MULTIPLER,
            packet_mtu: IPV4_PACKET_MTU,
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

    pub fn linger(mut self, linger: SubmitLingerConfig) -> Self {
        self.linger = linger;
        self
    }

    pub fn core_id(mut self, core_id: Option<CoreId>) -> Self {
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
                    buf: BufferFlavor::Registered(rbuf),
                }
            })
            .collect();
        send_req_tx
            .send(SendReqBatch { batch })
            .map_err(|_| SendError::Disconnected)
    }
}

impl UringSocket {
    pub fn new(config: ZcMulticastSenderConfig, udp_socket: UdpSocket) -> io::Result<Self> {
        zc_multicast_sender_with_config(config, udp_socket).map(|(sender, _ev_loop_handle)| sender)
    }

    pub fn send_zc<Src>(&self, src: Src, dests: &[SocketAddr]) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let data = src.as_ref();
        let permit = self
            .free_rbuffer_rx
            .recv()
            .map_err(|_| SendError::Disconnected)?;
        permit.send(data, dests, &self.send_req_tx)
    }

    pub fn send<T>(&self, buf: T, dests: &[SocketAddr]) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        assert!(
            buf.as_ref().len() <= self.packet_mtu,
            "Buffer size {} exceeds MTU {}",
            buf.as_ref().len(),
            self.packet_mtu
        );
        let buf = Bytes::from_owner(buf);
        let send_req = SendReq {
            dests: dests.to_vec(),
            buf: BufferFlavor::Userspace(buf),
        };
        self.send_req_tx
            .send(send_req.into())
            .map_err(|_| SendError::Disconnected)
    }

    pub fn send_auto<T>(&self, buf: T, dests: &[SocketAddr]) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let buf = Bytes::from_owner(buf);
        assert!(
            buf.as_ref().len() <= self.packet_mtu,
            "Buffer size {} exceeds MTU {}",
            buf.as_ref().len(),
            self.packet_mtu
        );
        match self.free_rbuffer_rx.try_recv() {
            Ok(permit) => permit.send(buf, dests, &self.send_req_tx),
            Err(crossbeam_channel::TryRecvError::Empty) => self.send(buf, dests),
            Err(crossbeam_channel::TryRecvError::Disconnected) => Err(SendError::Disconnected),
        }
    }

    pub fn batch_send_copied<T>(
        &self,
        bufvec: impl IntoIterator<Item = T>,
        dests: &[SocketAddr],
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let send_reqs: Vec<SendReq> = bufvec
            .into_iter()
            .map(|buf| {
                assert!(
                    buf.as_ref().len() <= self.packet_mtu,
                    "Buffer size {} exceeds MTU {}",
                    buf.as_ref().len(),
                    self.packet_mtu
                );
                SendReq {
                    dests: dests.to_vec(),
                    buf: BufferFlavor::Userspace(Bytes::from_owner(buf)),
                }
            })
            .collect();
        self.send_req_tx
            .send(SendReqBatch { batch: send_reqs })
            .map_err(|_| SendError::Disconnected)
    }

    ///
    /// Cross-product between the src_vec and dests.
    /// Each src in src_vec is sent to each dest in dests.
    /// This function will block until all messages are sent.
    ///
    /// Try to build the biggest batch possible if enough permits are available to do so.
    pub fn batch_send_zc<Src>(&self, src_vec: &[Src], dests: &[SocketAddr]) -> Result<(), SendError>
    where
        Src: AsRef<[u8]>,
    {
        let mut num_msg = src_vec.len();

        let mut permits = Vec::with_capacity(num_msg);
        let mut src_i: usize = 0;
        while num_msg > 0 {
            'inner: while num_msg > 0 {
                match self.free_rbuffer_rx.try_recv() {
                    Ok(permit) => {
                        permits.push(permit);
                        num_msg -= 1;
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        // println!("No free buffer available");
                        yield_now();
                        break 'inner;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        return Err(SendError::Disconnected);
                    }
                }
            }

            if permits.is_empty() {
                // prepare to block until at least one permit is available
                match self.free_rbuffer_rx.recv() {
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
                assert!(
                    data.as_ref().len() <= self.packet_mtu,
                    "Buffer size {} exceeds MTU {}",
                    data.as_ref().len(),
                    self.packet_mtu
                );
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

pub fn usk(send_socket: UdpSocket) -> io::Result<(UringSocket, JoinHandle<()>)> {
    zc_multicast_sender_with_config(ZcMulticastSenderConfig::default(), send_socket)
}

pub fn zc_multicast_sender_with_config(
    config: ZcMulticastSenderConfig,
    send_socket: UdpSocket,
) -> io::Result<(UringSocket, JoinHandle<()>)> {
    let ZcMulticastSenderConfig {
        num_registered_buffers,
        registered_buffer_size,
        ioring_queue_size,
        linger,
        core_id,
        send_req_ctx_slab_mult,
        packet_mtu,
    } = config;

    let cq_size = ioring_queue_size * 2;
    let ring = IoUring::builder()
        .setup_cqsize(cq_size)
        .setup_sqpoll(100)
        .build(ioring_queue_size)?;
    log::trace!(
        "zc_multicast_sender_with_config: io_uring queue size: {}",
        ioring_queue_size
    );
    let (send_req_tx, send_req_rx) = crossbeam_channel::bounded(1000);
    let (free_buffer_tx, free_buffer_rx) = crossbeam_channel::bounded(num_registered_buffers as _);
    const PAGE_SIZE: usize = 4096;
    let layout = Layout::from_size_align(registered_buffer_size, PAGE_SIZE).expect("layout");

    let mut rbuf_vec = Vec::with_capacity(num_registered_buffers as _);
    let mut rbuf_raw_ptr_vec = Vec::with_capacity(num_registered_buffers as _);
    for i in 0..num_registered_buffers {
        // intermediate registered buffer
        let ptr = unsafe { alloc(layout) };
        let rbuf = RegisteredBuffer {
            index: i,
            capacity: registered_buffer_size,
            buf: libc::iovec {
                iov_base: ptr as _,
                iov_len: registered_buffer_size,
            },
        };
        rbuf_raw_ptr_vec.push(unsafe { Box::from_raw(ptr) });
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
    let ev_loop = UringEvLoop {
        ring,
        send_socket,
        send_socket_fd,
        backpressured_submissions: VecDeque::with_capacity(num_registered_buffers as _),
        free_buffer_tx,
        send_req_rx,
        send_req_ctx_slab,
        registered_buffers_refcount_vec: rbuf_rc_vec,
        unprocess_completion_queue: Vec::with_capacity(cq_size as usize),
        submit_linger_config: linger,
        stats: Arc::clone(&stats),
        disconnected: false,
        _rbuf_raw_ptr_vec: rbuf_raw_ptr_vec,
    };

    log::trace!(
        "zc_multicast_sender_with_config: starting event loop on core {:?}",
        core_id
    );

    let ev_loop_idx = URING_EV_LOOP_THREAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let jh = std::thread::Builder::new()
        .name(format!("uring-socket-ev-loop-{ev_loop_idx}"))
        .spawn(move || {
            if let Some(core_id) = core_id {
                core_affinity::set_for_current(core_id);
            }
            ev_loop.ev_loop();
        })?;

    Ok((UringSocket {
        free_rbuffer_rx: free_buffer_rx,
        send_req_tx,
        stats,
        packet_mtu,
        ev_loop_thread_idx: ev_loop_idx,
    }, jh))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread::sleep, time::Duration};

    use log::{LevelFilter, Metadata, Record, SetLoggerError};
    use rand::{RngCore, rng};
    use solana_net_utils::bind_to_localhost;
    use solana_streamer::{packet::Packet, recvmmsg::recv_mmsg};

    use crate::io_uring::socket::{recv_mmsg_exact, usk};

    #[allow(dead_code)]
    static LOGGER: SimpleLogger = SimpleLogger;

    #[allow(dead_code)]
    pub fn init() -> Result<(), SetLoggerError> {
        log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Debug))
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
    pub fn test_send_one_zc_msg() {
        let _ = init();
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender_socket = bind_to_localhost().expect("bind");
        let (mc_sender, jh) = usk(sender_socket).expect("zc_multicast_sender");
        let packet = Packet::default();
        mc_sender
            .send_zc(packet.data(..).unwrap(), &[addr])
            .expect("send multicast");
        log::info!("test_send_one_zc_msg: id {}", mc_sender.ev_loop_thread_idx);
        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap();
        assert_eq!(1, recv);
        drop(mc_sender);
        jh.join().expect("join ev loop thread");
    }

    #[test]
    pub fn test_send_one_copied_msg() {
        let _ = init();
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender_socket = bind_to_localhost().expect("bind");
        let (mc_sender, jh) = usk(sender_socket).expect("zc_multicast_sender");
        log::info!("test_send_one_copied_msg: id {}", mc_sender.ev_loop_thread_idx);
        let packet = vec![0u8; 1232];
        let box_packet = packet.into_boxed_slice();
        let packet: Arc<[u8]> = Arc::from(box_packet);
        mc_sender.send(packet, &[addr]).expect("send multicast");

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap();
        assert_eq!(1, recv);
        drop(mc_sender);
        jh.join().expect("join ev loop thread");
    }

    #[test]
    pub fn test_batch_send_copied() {
        let _ = init();
        const NUM_READERS: usize = 1;
        const NUM_PACKETS: usize = 64;
        let mut reader_socket_vec = Vec::with_capacity(NUM_READERS);
        let mut reader_addr_vec = Vec::with_capacity(NUM_READERS);
        for _ in 0..NUM_READERS {
            let reader = bind_to_localhost().expect("bind");
            reader_addr_vec.push(reader.local_addr().unwrap());
            reader_socket_vec.push(reader);
        }

        let sender_socket = bind_to_localhost().expect("bind");

        let (mc_sender, jh) = usk(sender_socket).expect("zc_multicast_sender");
        log::info!("test_batch_send_copied: id {}", mc_sender.ev_loop_thread_idx);
        let packet = vec![0u8; 1232].into_boxed_slice();
        let bufvec = vec![packet; NUM_PACKETS];
        mc_sender
            .batch_send_copied(bufvec, &reader_addr_vec)
            .expect("send multicast");

        for reader_sock in reader_socket_vec {
            let mut packets: Vec<Packet> = vec![Packet::default(); NUM_PACKETS];
            recv_mmsg_exact(&reader_sock, &mut packets[..], NUM_PACKETS).unwrap();
        }
        drop(mc_sender);
        jh.join().expect("join ev loop thread");
    }

    #[test]
    pub fn test_multicast_zc() {
        let _ = init();
        let reader = bind_to_localhost().expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = bind_to_localhost().expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let reader3 = bind_to_localhost().expect("bind");
        let addr3 = reader3.local_addr().unwrap();

        let reader4 = bind_to_localhost().expect("bind");
        let addr4 = reader4.local_addr().unwrap();

        let sender_socket = bind_to_localhost().expect("bind");
        let (mc_sender, jh) = usk(sender_socket).expect("zc_multicast_sender");
        log::info!("test_multicast_zc: id {}", mc_sender.ev_loop_thread_idx);
        let mut expected_packet = vec![0u8; 1232];

        let mut rng = rng();
        rng.fill_bytes(expected_packet.as_mut_slice());

        mc_sender
            .send_zc(&expected_packet, &[addr, addr2, addr3, addr4])
            .expect("send multicast");
        for rdr in [reader, reader2, reader3, reader4].iter() {
            let mut packets = vec![Packet::default(); 32];
            let recv = recv_mmsg(rdr, &mut packets[..]).unwrap();
            assert_eq!(1, recv);
            let actual = packets[0].data(..).unwrap();
            assert_eq!(actual.len(), expected_packet.len());
            assert_eq!(*actual, expected_packet[..]);
        }
        log::info!("All receivers got the expected packet");
        drop(mc_sender);
        jh.join().expect("join ev loop thread");
    }

    #[test]
    pub fn test_batch_multicast_zc() {
        let _ = init();
        const NUM_READERS: usize = 1;
        const NUM_PACKETS: usize = 64;
        let mut readers = Vec::with_capacity(NUM_READERS);
        let mut addrs = Vec::with_capacity(NUM_READERS);
        for _ in 0..NUM_READERS {
            let reader = bind_to_localhost().expect("bind");
            addrs.push(reader.local_addr().unwrap());
            readers.push(reader);
        }
        let sender_socket = bind_to_localhost().expect("bind");
        let (mc_sender, jh) = usk(sender_socket).expect("zc_multicast_sender");
        log::info!("test_batch_multicast_zc: id {}", mc_sender.ev_loop_thread_idx);
        let mut expected_packet = vec![0u8; 1232];
        let mut rng = rng();
        rng.fill_bytes(expected_packet.as_mut_slice());

        let expected_packet = Arc::from(expected_packet.into_boxed_slice());

        let bufvec = vec![expected_packet; NUM_PACKETS];

        mc_sender
            .batch_send_zc(&bufvec, &addrs)
            .expect("send multicast");
        log::info!("Sent batch multicast zc");
        for rdr in readers.iter() {
            let mut buf = vec![Packet::default(); NUM_PACKETS];
            recv_mmsg_exact(rdr, &mut buf[..], NUM_PACKETS).unwrap();
        }
        log::info!("All receivers got the expected packets");
        sleep(Duration::from_secs(1));
        drop(mc_sender);
        jh.join().expect("join ev loop thread");
        log::info!("Event loop thread joined");
    }
}

pub fn recv_mmsg_exact(socket: &UdpSocket, dest: &mut [Packet], n: usize) -> io::Result<usize> {
    let mut start = 0;
    let mut total = 0;
    while dest.len() < n {
        total += recv_mmsg(socket, &mut dest[start..])?;
        log::trace!("recv {total} packets");
        start += total;
    }
    Ok(total)
}
