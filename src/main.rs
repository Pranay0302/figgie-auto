use std::sync::Arc;
use std::path::PathBuf;

pub mod utils;
pub use utils::*;

pub mod models;
pub use models::*;

pub mod logger;
pub use logger::EventLogger;

pub mod match_maker;
pub use match_maker::MatchMaker;

pub mod player;
pub use player::PlayerName;
pub use player::generic::GenericPlayer;
pub use player::event_driven::EventDrivenPlayer;

use crate::player::TiltInventory;
use crate::player::external::{ExternalPlayer, SidecarMsg};


struct CliArgs {
    log_dir: Option<PathBuf>,
    seed: u64,
    max_rounds: Option<u32>,
    round_duration_secs: u64,
    inter_round_sleep_secs: u64,
    deal_warmup_secs: u64,
    quiet: bool,
    external_player_cmd: Option<String>,
    opponent: String,
}

fn parse_args() -> CliArgs {
    let mut args = CliArgs {
        log_dir: None,
        seed: 0,
        max_rounds: None,
        round_duration_secs: 240, // matches the in-game timer (4 min)
        inter_round_sleep_secs: 30,
        deal_warmup_secs: 5,
        quiet: false,
        external_player_cmd: None,
        opponent: String::from("TiltInventory"),
    };
    let mut auto_seed = true;
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        let a = &raw[i];
        let take_val = |idx: &mut usize| -> String {
            *idx += 1;
            raw.get(*idx)
                .cloned()
                .unwrap_or_else(|| panic!("missing value for arg"))
        };
        match a.as_str() {
            "--log-dir" => {
                args.log_dir = Some(PathBuf::from(take_val(&mut i)));
            }
            "--seed" => {
                args.seed = take_val(&mut i).parse().expect("seed must be u64");
                auto_seed = false;
            }
            "--max-rounds" | "--games" => {
                args.max_rounds = Some(take_val(&mut i).parse().expect("max-rounds must be u32"));
            }
            "--round-duration-secs" => {
                args.round_duration_secs = take_val(&mut i).parse().expect("u64");
            }
            "--inter-round-sleep-secs" => {
                args.inter_round_sleep_secs = take_val(&mut i).parse().expect("u64");
            }
            "--deal-warmup-secs" => {
                args.deal_warmup_secs = take_val(&mut i).parse().expect("u64");
            }
            "--quiet" => {
                args.quiet = true;
            }
            "--external-player-cmd" => {
                args.external_player_cmd = Some(take_val(&mut i));
            }
            "--opponent" => {
                args.opponent = take_val(&mut i);
            }
            "-h" | "--help" => {
                eprintln!(
                    "figgie-auto [--log-dir DIR] [--seed N] [--max-rounds N] \
                     [--round-duration-secs N] [--inter-round-sleep-secs N] \
                     [--deal-warmup-secs N] [--quiet] \
                     [--external-player-cmd CMD] [--opponent NAME]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown arg: {}", other),
        }
        i += 1;
    }
    if auto_seed {
        // Deterministic-by-default would be surprising; pick a fresh seed but
        // still log it so the run is reproducible.
        use std::time::{SystemTime, UNIX_EPOCH};
        args.seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xC0FFEE);
    }
    args
}

fn parse_player_name(s: &str) -> PlayerName {
    match s {
        "Spread"        => PlayerName::Spread,
        "Seller"        => PlayerName::Seller,
        "Noisy"         => PlayerName::Noisy,
        "PickOff"       => PlayerName::PickOff,
        "TiltInventory" => PlayerName::TiltInventory,
        "TheHoarder"    => PlayerName::TheHoarder,
        "PrayingMantis" => PlayerName::PrayingMantis,
        other           => panic!("unknown agent: {}", other),
    }
}

async fn spawn_agent(
    name: PlayerName,
    event_sender: tokio::sync::broadcast::Sender<Event>,
    order_sender: Arc<kanal::AsyncSender<Order>>,
) {
    match name {
        PlayerName::TiltInventory => {
            let mut p = TiltInventory::new(PlayerName::TiltInventory, false, 2000, 4000, event_sender, order_sender);
            p.start().await;
        }
        PlayerName::PickOff => {
            let mut p = EventDrivenPlayer::new(PlayerName::PickOff, false, event_sender, order_sender);
            p.start().await;
        }
        other => {
            let mut p = GenericPlayer::new(other, false, 1000, 2000, event_sender, order_sender);
            p.start().await;
        }
    }
}

fn main() {

    const STARTING_BALANCE: usize = 500;

    let cli = parse_args();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build runtime");
    runtime.block_on(async {

        if !cli.quiet {
            println!("");
            println!("{}|==============================================|{}", CL::DimLightBlue.get(), CL::End.get());
            println!("{}|{}{}           Welcome to Figgie Auto!            {}{}|{}", CL::DimLightBlue.get(), CL::End.get(), CL::Teal.get(), CL::End.get(), CL::DimLightBlue.get(), CL::End.get());
            println!("{}|==============================================|{}\n", CL::DimLightBlue.get(), CL::End.get());
            println!("Let the games begin!\n");
        }

        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut players: Vec<PlayerName> = Vec::new();

        let (tx, rx) = kanal::unbounded_async::<Order>();
        let order_rx = Arc::new(rx);
        let order_tx = Arc::new(tx);

        let (event_sender, _) = tokio::sync::broadcast::channel::<Event>(100);

        let sidecar_tx: Option<tokio::sync::mpsc::UnboundedSender<SidecarMsg>>;

        if let Some(cmd) = cli.external_player_cmd.clone() {
            // ── External bot ────────────────────────────────────────────────
            let (sc_tx, sc_rx) = tokio::sync::mpsc::unbounded_channel::<SidecarMsg>();
            sidecar_tx = Some(sc_tx);

            let bot_order_tx = Arc::clone(&order_tx);
            let bot_handle: tokio::task::JoinHandle<()> = tokio::task::spawn(async move {
                let mut player = ExternalPlayer::spawn(&cmd, sc_rx, bot_order_tx)
                    .await
                    .expect("failed to spawn external player");
                player.start().await;
            });
            handles.push(bot_handle);
            players.push(PlayerName::External);

            // ── 4 copies of the named opponent ──────────────────────────────
            let opp_name = parse_player_name(&cli.opponent);
            for _ in 0..4 {
                let n      = opp_name.clone();
                let ev_tx  = event_sender.clone();
                let ord_tx = Arc::clone(&order_tx);
                handles.push(tokio::task::spawn(async move {
                    spawn_agent(n, ev_tx, ord_tx).await;
                }));
                players.push(opp_name.clone());
            }
        } else {
            sidecar_tx = None;

            // ── Original hardcoded 5-player setup ───────────────────────────
            let n = PlayerName::TiltInventory;
            players.push(n.clone());
            let ev = event_sender.clone(); let ord = Arc::clone(&order_tx);
            let verbose = !cli.quiet;
            handles.push(tokio::task::spawn(async move {
                let mut p = TiltInventory::new(n, verbose, 2000, 4000, ev, ord);
                p.start().await;
            }));

            let n = PlayerName::Spread;
            players.push(n.clone());
            let ev = event_sender.clone(); let ord = Arc::clone(&order_tx);
            handles.push(tokio::task::spawn(async move {
                let mut p = GenericPlayer::new(n, false, 1000, 2000, ev, ord);
                p.start().await;
            }));

            let n = PlayerName::Seller;
            players.push(n.clone());
            let ev = event_sender.clone(); let ord = Arc::clone(&order_tx);
            handles.push(tokio::task::spawn(async move {
                let mut p = GenericPlayer::new(n, false, 2000, 4000, ev, ord);
                p.start().await;
            }));

            let n = PlayerName::Noisy;
            players.push(n.clone());
            let ev = event_sender.clone(); let ord = Arc::clone(&order_tx);
            handles.push(tokio::task::spawn(async move {
                let mut p = GenericPlayer::new(n, false, 4000, 8000, ev, ord);
                p.start().await;
            }));

            let n = PlayerName::PickOff;
            players.push(n.clone());
            let ev = event_sender.clone(); let ord = Arc::clone(&order_tx);
            handles.push(tokio::task::spawn(async move {
                let mut p = EventDrivenPlayer::new(n, false, ev, ord);
                p.start().await;
            }));
        }

        // ── MatchMaker ───────────────────────────────────────────────────────
        let log_dir                = cli.log_dir.clone();
        let seed                   = cli.seed;
        let max_rounds             = cli.max_rounds;
        let round_duration_secs    = cli.round_duration_secs;
        let inter_round_sleep_secs = cli.inter_round_sleep_secs;
        let deal_warmup_secs       = cli.deal_warmup_secs;
        let quiet                  = cli.quiet;
        let mm_ev                  = event_sender.clone();
        let mm_handle: tokio::task::JoinHandle<()> = tokio::task::spawn(async move {
            let mut mm = MatchMaker::new(
                STARTING_BALANCE,
                players,
                mm_ev,
                order_rx,
                seed,
                log_dir,
                max_rounds,
                round_duration_secs,
                inter_round_sleep_secs,
                deal_warmup_secs,
                quiet,
                sidecar_tx,
            );
            mm.start().await;
        });
        handles.push(mm_handle);

        for handle in handles {
            handle.await.unwrap();
        }

    });

}
