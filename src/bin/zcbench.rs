use clap::{Arg, Command, Parser};
use log::{LevelFilter, Metadata, Record, SetLoggerError};
use rand::{RngCore, rng};
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::spawn;
use std::{net::SocketAddr, str::FromStr, time::Duration};
use zc_streamer::io_uring::sendmmsg::{
    GlobalSendStats, ZcMulticastSenderConfig, zc_multicast_sender_with_config,
};

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

    ///
    /// Socket address to send packets to
    ///
    dests: Vec<String>,
}

fn run_multicast_benchmark(args: MulticastArgs) {
    pretty_env_logger::init();
    // Here you would implement the logic to run the multicast benchmark
    // using the provided arguments.
    const MINIMUM_CORE_NUM: usize = 3;
    const IORING_CORE_IDX: usize = 2;
    const SENDER_CORE_IDX: usize = 1;
    const MAIN_CORE_IDX: usize = 0;

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
        core_id: core_list[IORING_CORE_IDX],
        ..Default::default()
    };
    let sender_socket = solana_net_utils::bind_to_unspecified().expect("bind");
    let zc_sender =
        zc_multicast_sender_with_config(sender_socket, zc_sender_config).expect("zc_sender");
    let stats = zc_sender.global_shared_stats();
    let sender_core_id = core_list[SENDER_CORE_IDX];
    let stop = Arc::new(AtomicBool::new(false));

    let ctrlc_stop = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        ctrlc_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    let sender_stop = Arc::clone(&stop);
    let zc_sender_jh = spawn(move || {
        core_affinity::set_for_current(sender_core_id);
        let mut rng = rng();
        let dests = dests;
        while !sender_stop.load(std::sync::atomic::Ordering::Relaxed) {
            // Send multicast packets
            let mut packet = vec![0u8; 1232];
            rng.fill_bytes(&mut packet);
            zc_sender.send(&packet, dests.as_slice()).expect("send");
        }
    });

    core_affinity::set_for_current(core_list[MAIN_CORE_IDX]);
    let mut i = 0;
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        if i >= args.sample && args.sample > 0 {
            break;
        }
        std::thread::sleep(args.print_interval.dur);
        let stats = stats.clone();
        println!("Multicast stats: {:?}", pretty_stats(&stats));
        i += 1;
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    let _ = zc_sender_jh.join();
}

fn pretty_stats(stats: &GlobalSendStats) -> String {
    format!(
        "Bytes sent: {}, Last Errno: {}. Errno count: {}, ZC send count: {}, Copied send count: {}, Submit count: {}, Submit wait cumulative time (ms): {}, Inflight send: {}",
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
            .submit_count
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
    }
}
