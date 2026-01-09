use libc::{
    AF_INET, MSG_ERRQUEUE, MSG_ZEROCOPY, POLLOUT, SO_ZEROCOPY, SOL_SOCKET, c_int, eventfd, iovec,
    msghdr, poll, pollfd, recvmsg, sendmsg, setsockopt, sockaddr_in, sockaddr_storage,
};
use std::net::{SocketAddr, SocketAddrV4};
use std::os::unix::io::AsRawFd;
use std::thread::yield_now;
use std::{io, net::UdpSocket};
use std::{mem, ptr};

fn main2() {
    const SIZE_OF_SOCKADDR_IN: usize = mem::size_of::<sockaddr_in>();
    const SIZE_OF_SOCKADDR_STORAGE: usize = mem::size_of::<sockaddr_storage>();
    const SOCKADDR_IN_PADDING: usize = SIZE_OF_SOCKADDR_STORAGE - SIZE_OF_SOCKADDR_IN;

    unsafe {
        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let addr = sock.local_addr().unwrap();
        sock.connect("10.43.4.114:60599").unwrap();
        let fd = sock.as_raw_fd();

        let one: c_int = 1;
        if setsockopt(
            fd,
            SOL_SOCKET,
            SO_ZEROCOPY,
            &one as *const _ as *const _,
            std::mem::size_of::<c_int>() as u32,
        ) < 0
        {
            eprintln!("setsockopt SO_ZEROCOPY failed");
            return;
        }
        let mut socketaddr_in: sockaddr_in = std::mem::zeroed();
        let ptr: *mut sockaddr_in = &mut socketaddr_in;
        let SocketAddr::V4(addr_v4) = addr else {
            panic!("not ipv4");
        };
        ptr::write(ptr, *nix::sys::socket::SockaddrIn::from(addr_v4).as_ref());
        ptr::write_bytes(
            (ptr as *mut u8).add(SIZE_OF_SOCKADDR_IN),
            0,
            SOCKADDR_IN_PADDING,
        );

        let msg = b"Hello MSG_ZEROCOPY!";
        let iov = iovec {
            iov_base: msg.as_ptr() as *mut _,
            iov_len: msg.len(),
        };

        let mut mhdr: msghdr = std::mem::zeroed();
        mhdr.msg_name = ptr as _;
        mhdr.msg_namelen = std::mem::size_of::<sockaddr_in>() as u32;
        mhdr.msg_iov = &iov as *const iovec as *mut iovec;
        mhdr.msg_iovlen = 1;
        mhdr.msg_flags = 0;

        let ret = sendmsg(fd, &mhdr, MSG_ZEROCOPY);
        if ret < 0 {
            panic!("sendmsg failed: {}", io::Error::last_os_error());
        }
        println!("sendmsg returned {} bytes", ret);

        let mut pfd = pollfd {
            fd,
            events: POLLOUT,
            revents: 0,
        };

        println!("Waiting for zero-copy notification...");
        let p = poll(&mut pfd, 1, -1); // 5s timeout
        if p < 0 {
            panic!("poll failed: {}", io::Error::last_os_error());
        } else if p == 0 {
            println!("Timeout, zero-copy not confirmed");
        } else {
            println!("Zero-copy likely completed (POLLOUT triggered)");
        }
    }
}

fn main() {
    let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
    sock.connect("10.43.4.114:60599").unwrap();
    let fd = sock.as_raw_fd();

    let one: c_int = 1;
    unsafe {
        if setsockopt(
            fd,
            SOL_SOCKET,
            SO_ZEROCOPY,
            &one as *const _ as *const _,
            std::mem::size_of::<c_int>() as u32,
        ) < 0
        {
            eprintln!("setsockopt SO_ZEROCOPY failed");
            return;
        }
    }

    let buf = vec![0xFFu8; 1024 * 50];
    unsafe {
        let ret = libc::send(fd, buf.as_ptr() as *const _, buf.len(), MSG_ZEROCOPY);
        if ret < 0 {
            let lasterr = io::Error::last_os_error();
            eprintln!("send failed: {:?}", lasterr);
            return;
        }
    }
    println!("sent {} bytes", buf.len());

    // let mut cmsg_space = [0u8; 128];
    // let mut iov = iovec {
    //     iov_base: buf.as_ptr() as *mut _,
    //     iov_len: buf.len(),
    // };
    let mut msg: msghdr = unsafe { std::mem::zeroed() };
    // msg.msg_iov = &mut iov;
    // msg.msg_iovlen = 1;
    // msg.msg_control = cmsg_space.as_mut_ptr() as *mut _;
    // msg.msg_controllen = cmsg_space.len();

    let mut pfd: libc::pollfd = unsafe { std::mem::zeroed() };
    pfd.fd = fd;
    pfd.events = 0;

    println!("Waiting for zero-copy notification...");
    let p = unsafe { libc::poll(&mut pfd, 1, -1) };
    if p != 1 {
        let lasterr = io::Error::last_os_error();
        panic!("poll : {:?}", lasterr);
    }
    if pfd.revents & libc::POLLERR == 0 {
        let lasterr = io::Error::last_os_error();
        panic!("poll revents: {},  error: {:?}", pfd.revents, lasterr);
    }

    loop {
        let ret = unsafe { recvmsg(fd, &mut msg, MSG_ERRQUEUE) };
        if ret == 0 {
            yield_now();
            continue;
        }
        if ret < 0 {
            let lasterr = io::Error::last_os_error();
            println!("recvmsg failed: {:?}", lasterr);
            // break;
        }
        if ret > 0 {
            println!("recvmsg returned {} bytes", ret);
            println!("zero-copy notification received (works!)");
            read_notification(msg);
        }
    }
}

fn read_notification(msg: msghdr) {
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        println!("Control message: {:?}", cmsg);
        if !cmsg.is_null() {
            let level = (*cmsg).cmsg_level;
            let type_ = (*cmsg).cmsg_type;
            let data = libc::CMSG_DATA(cmsg);
            let len = (*cmsg).cmsg_len;

            println!("Received control message:");
            println!("  Level: {}", level);
            println!("  Type: {}", type_);
            println!(
                "  Data: {:?}",
                std::slice::from_raw_parts(data as *const u8, len as usize)
            );
        }
    }
}
