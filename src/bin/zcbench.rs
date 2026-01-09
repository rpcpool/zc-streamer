use clap::{Arg, Command, Parser};
use ioring_streamer::io_uring::socket::{
    GlobalSendStats, UringSocket, ZcMulticastSenderConfig, zc_multicast_sender_with_config
};
use log::{LevelFilter, Metadata, Record, SetLoggerError};
use rand::{RngCore, rng};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::spawn;
use std::time::Instant;
use std::{net::SocketAddr, str::FromStr, time::Duration};

static LOGGER: SimpleLogger = SimpleLogger;

pub fn init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Trace))
}

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

#[derive(Debug, Parser, Clone)]
struct Args {
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Parser, Clone)]
enum Action {
    // Run multicast benchmark
    #[command(name = "mc")]
    Multicast(MulticastArgs),

    // Run Solana streaming benchmark
    #[command(name = "sendmmsg")]
    SolanaStream(MulticastArgs),
}

#[derive(Debug, Clone)]
pub struct Interval {
    dur: Duration,
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        humantime::parse_duration(s)
            .map(|dur| Interval { dur })
            .map_err(|e| format!("Failed to parse duration: {}", e))
    }
}

#[derive(Debug, Clone, Parser)]
struct MulticastArgs {
    ///
    /// Message per second to send
    ///
    #[clap(long, short, default_value = "1000")]
    rate: u32,

    /// Interval to print the stats
    ///
    #[clap(long, short, default_value = "1s")]
    print_interval: Interval,

    /// How many samples to send, 0 means no limit
    #[clap(long, short, default_value = "0")]
    sample: i32,

    #[clap(long, short, default_value = "0.0.0.0:0")]
    bind: SocketAddr,

    ///
    /// Socket address to send packets to
    ///
    dests: Vec<String>,
}

fn run_solana_streamer_benchmark(args: MulticastArgs) {
    const MINIMUM_CORE_NUM: usize = 4;
    const IORING_CORE_IDX: usize = 3;
    const SENDER_CORE_IDX: usize = 2;
    const MAIN_CORE_IDX: usize = 1;

    let dests: Vec<SocketAddr> = args
        .dests
        .iter()
        .map(|dest| dest.parse().expect("invalid dest address"))
        .collect();

    let core_list = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if core_list.len() < MINIMUM_CORE_NUM {
        writeln!(
            std::io::stderr(),
            "Not enough CPU cores available, requires at least 3"
        )
        .unwrap();
        std::process::exit(1);
    }

    let sender_socket =
        solana_net_utils::bind_to(args.bind.ip(), args.bind.port(), false).expect("bind");
    println!("binded to {}", sender_socket.local_addr().unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let sender_stop = Arc::clone(&stop);
    let send_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let send_wait_cumu_time = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sender_send_wait_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sender_send_count = Arc::clone(&send_count);
    let sender_core_idx = core_list[SENDER_CORE_IDX];
    let sender_jh = spawn(move || {
        core_affinity::set_for_current(sender_core_idx);
        let mut rng = rng();
        let dests = dests;
        let mut packets = Vec::with_capacity(1024);

        for _ in 0..1024 {
            let mut packet = vec![0u8; 1232];
            rng.fill_bytes(&mut packet);
            packets.push(packet);
        }
        while !sender_stop.load(std::sync::atomic::Ordering::Relaxed) {
            // Send multicast packets
            for dest in dests.iter() {
                let packets_with_dest = packets
                    .iter()
                    .map(|p| (p.as_slice(), dest))
                    .collect::<Vec<_>>();
                let total = packets_with_dest.len();
                let t = std::time::Instant::now();
                solana_streamer::sendmmsg::batch_send(&sender_socket, packets_with_dest)
                    .expect("multi_target_send");
                let elapsed = t.elapsed();
                sender_send_wait_count.fetch_add(
                    elapsed.as_millis() as usize,
                    std::sync::atomic::Ordering::Relaxed,
                );
                sender_send_count.fetch_add(total, std::sync::atomic::Ordering::Relaxed);
            }
        }
    });

    core_affinity::set_for_current(core_list[MAIN_CORE_IDX]);
    let mut i = 0;
    while !stop.load(std::sync::atomic::Ordering::Relaxed) && !sender_jh.is_finished() {
        if i >= args.sample && args.sample > 0 {
            break;
        }
        std::thread::sleep(args.print_interval.dur);
        println!(
            "solana_streamer::sendmmsg stats -- total_sender_cnt: {}, wait time cumu : {}",
            send_count.load(std::sync::atomic::Ordering::Relaxed),
            send_wait_cumu_time.load(std::sync::atomic::Ordering::Relaxed)
        );
        i += 1;
    }
}

fn run_multicast_benchmark(args: MulticastArgs) {
    pretty_env_logger::init();
    // Here you would implement the logic to run the multicast benchmark
    // using the provided arguments.
    const MINIMUM_CORE_NUM: usize = 4;
    const IORING_CORE_IDX: usize = 3;
    const SENDER_CORE_IDX: usize = 2;
    const MAIN_CORE_IDX: usize = 1;

    let dests: Vec<SocketAddr> = args
        .dests
        .iter()
        .map(|dest| dest.parse().expect("invalid dest address"))
        .collect();

    let core_list = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if core_list.len() < MINIMUM_CORE_NUM {
        writeln!(
            std::io::stderr(),
            "Not enough CPU cores available, requires at least 3"
        )
        .unwrap();
        std::process::exit(1);
    }

    let zc_sender_config = ZcMulticastSenderConfig {
        core_id: Some(core_list[IORING_CORE_IDX]),
        registered_buffer_size: 50_000,
        ..Default::default()
    };
    let sender_socket =
        solana_net_utils::bind_to(args.bind.ip(), args.bind.port(), false).expect("bind");
    println!("binded to {}", sender_socket.local_addr().unwrap());
    let zc_sender =
        UringSocket::new(zc_sender_config, sender_socket).expect("zc_sender");
    let stats = zc_sender.global_shared_stats();
    let sender_core_id = core_list[SENDER_CORE_IDX];
    let stop = Arc::new(AtomicBool::new(false));

    let ctrlc_stop = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        ctrlc_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    let sender_stop = Arc::clone(&stop);
    let send_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let send_wait_cumu = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sender_send_count = Arc::clone(&send_count);
    let sender_send_wait_count = Arc::clone(&send_wait_cumu);
    let zc_sender_jh = spawn(move || {
        core_affinity::set_for_current(sender_core_id);
        let mut rng = rng();
        let dests = dests;
        let mut packet = vec![0u8; 1232];
        rng.fill_bytes(&mut packet);
        let packets = vec![packet; 1024];
        while !sender_stop.load(std::sync::atomic::Ordering::Relaxed) {
            // Send multicast packets
            let t = Instant::now();
            zc_sender
                .batch_send_copied(packets.clone(), dests.as_slice())
                .expect("send");
            let elapsed = t.elapsed();
            sender_send_wait_count.fetch_add(
                elapsed.as_millis() as usize,
                std::sync::atomic::Ordering::Relaxed,
            );
            sender_send_count.fetch_add(1024 * dests.len(), std::sync::atomic::Ordering::Relaxed);
        }
    });

    core_affinity::set_for_current(core_list[MAIN_CORE_IDX]);
    let mut i = 0;
    while !stop.load(std::sync::atomic::Ordering::Relaxed) && !zc_sender_jh.is_finished() {
        if i >= args.sample && args.sample > 0 {
            break;
        }
        std::thread::sleep(args.print_interval.dur);
        let stats = stats.clone();
        println!(
            "Multicast stats: {:?}, total_sender_cnt: {}, send wait cumu: {}",
            pretty_stats(&stats),
            send_count.load(std::sync::atomic::Ordering::Relaxed),
            send_wait_cumu.load(std::sync::atomic::Ordering::Relaxed)
        );
        i += 1;
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    let _ = zc_sender_jh.join();
}

fn pretty_stats(stats: &GlobalSendStats) -> String {
    format!(
        "Bytes sent: {}, Last Errno: {}. Errno count: {}, ZC send count: {}, Copied send count: {}, Submit wait cumulative time (ms): {}, Inflight send: {}",
        stats.bytes_sent.load(std::sync::atomic::Ordering::Relaxed),
        stats.last_errno.load(std::sync::atomic::Ordering::Relaxed),
        stats.errno_count.load(std::sync::atomic::Ordering::Relaxed),
        stats
            .zc_send_count
            .load(std::sync::atomic::Ordering::Relaxed),
        stats
            .copied_send_count
            .load(std::sync::atomic::Ordering::Relaxed),
        stats
            .submit_wait_cumu_time_ms
            .load(std::sync::atomic::Ordering::Relaxed),
        stats
            .inflight_send
            .load(std::sync::atomic::Ordering::Relaxed)
    )
}

fn main() {
    let args = Args::parse();

    match args.action {
        Action::Multicast(mc_args) => {
            run_multicast_benchmark(mc_args);
        }
        Action::SolanaStream(mc_args) => {
            run_solana_streamer_benchmark(mc_args);
        }
    }
}
