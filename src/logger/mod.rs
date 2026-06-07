use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::{Card, Inventory, PlayerName};

/// JSONL event logger. One file per game (= one round of Figgie).
/// Thread-safe via a Mutex around the buffered writer.
pub struct EventLogger {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl EventLogger {
    pub fn new(log_dir: &Path, game_id: u64, seed: u64) -> std::io::Result<Self> {
        create_dir_all(log_dir)?;
        let path = log_dir.join(format!("game_{:08}.jsonl", game_id));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let logger = Self {
            writer: Mutex::new(BufWriter::new(file)),
            path,
        };
        // Header line: identifies the game and seed used to drive RNG.
        logger.write_line(&format!(
            r#"{{"type":"header","game_id":{},"seed":{},"schema":1}}"#,
            game_id, seed
        ));
        Ok(logger)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write_line(&self, line: &str) {
        let mut w = self.writer.lock().expect("logger poisoned");
        let _ = w.write_all(line.as_bytes());
        let _ = w.write_all(b"\n");
    }

    pub fn flush(&self) {
        if let Ok(mut w) = self.writer.lock() {
            let _ = w.flush();
        }
    }

    pub fn log_round_start(
        &self,
        t: f64,
        round: u32,
        ante: usize,
        pot: usize,
        players: &[PlayerName],
    ) {
        let players_json = players
            .iter()
            .enumerate()
            .map(|(i, p)| format!(r#"{{"idx":{},"name":"{}"}}"#, i, player_name_str(p)))
            .collect::<Vec<_>>()
            .join(",");
        self.write_line(&format!(
            r#"{{"t":{:.3},"type":"round_start","round":{},"ante":{},"pot":{},"players":[{}]}}"#,
            t, round, ante, pot, players_json
        ));
    }

    pub fn log_deal(
        &self,
        t: f64,
        players: &[PlayerName],
        inventories: &std::collections::HashMap<PlayerName, Inventory>,
        common_suit: &Card,
        goal_suit: &Card,
        counts: &std::collections::HashMap<Card, usize>,
    ) {
        // Ground truth: each player's full hand, plus the goal/common suits and
        // suit counts. Tokenization in Phase 2 will mask the other-player hands
        // when producing a per-perspective view.
        let hands_json = players
            .iter()
            .map(|p| {
                let inv = inventories.get(p).copied().unwrap_or(Inventory::new());
                format!(
                    r#"{{"player":"{}","S":{},"C":{},"H":{},"D":{}}}"#,
                    player_name_str(p),
                    inv.spades,
                    inv.clubs,
                    inv.hearts,
                    inv.diamonds
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        self.write_line(&format!(
            r#"{{"t":{:.3},"type":"deal","common_suit":"{}","goal_suit":"{}","counts":{{"S":{},"C":{},"H":{},"D":{}}},"hands":[{}]}}"#,
            t,
            card_str(common_suit),
            card_str(goal_suit),
            counts.get(&Card::Spade).copied().unwrap_or(0),
            counts.get(&Card::Club).copied().unwrap_or(0),
            counts.get(&Card::Heart).copied().unwrap_or(0),
            counts.get(&Card::Diamond).copied().unwrap_or(0),
            hands_json
        ));
    }

    pub fn log_quote(
        &self,
        t: f64,
        side: &str, // "bid" | "offer"
        player: &PlayerName,
        card: &Card,
        price: usize,
    ) {
        self.write_line(&format!(
            r#"{{"t":{:.3},"type":"{}","player":"{}","suit":"{}","price":{}}}"#,
            t,
            side,
            player_name_str(player),
            card_str(card),
            price
        ));
    }

    pub fn log_order_rejected(&self, t: f64, player: &PlayerName, card: &Card, reason: &str) {
        self.write_line(&format!(
            r#"{{"t":{:.3},"type":"order_rejected","player":"{}","suit":"{}","reason":"{}"}}"#,
            t,
            player_name_str(player),
            card_str(card),
            reason
        ));
    }

    pub fn log_trade(
        &self,
        t: f64,
        card: &Card,
        price: usize,
        buyer: &PlayerName,
        seller: &PlayerName,
    ) {
        self.write_line(&format!(
            r#"{{"t":{:.3},"type":"trade","suit":"{}","price":{},"buyer":"{}","seller":"{}"}}"#,
            t,
            card_str(card),
            price,
            player_name_str(buyer),
            player_name_str(seller)
        ));
    }

    pub fn log_cancel_all(&self, t: f64) {
        self.write_line(&format!(r#"{{"t":{:.3},"type":"cancel_all"}}"#, t));
    }

    pub fn log_round_end(
        &self,
        t: f64,
        players: &[PlayerName],
        goal_suit: &Card,
        common_suit: &Card,
        final_inventories: &std::collections::HashMap<PlayerName, Inventory>,
        initial_points: &std::collections::HashMap<PlayerName, usize>,
        final_points: &std::collections::HashMap<PlayerName, usize>,
    ) {
        let payouts_json = players
            .iter()
            .map(|p| {
                let inv = final_inventories.get(p).copied().unwrap_or(Inventory::new());
                let initial = initial_points.get(p).copied().unwrap_or(0) as i64;
                let final_p = final_points.get(p).copied().unwrap_or(0) as i64;
                format!(
                    r#"{{"player":"{}","final_S":{},"final_C":{},"final_H":{},"final_D":{},"initial_points":{},"final_points":{},"pnl":{}}}"#,
                    player_name_str(p),
                    inv.spades,
                    inv.clubs,
                    inv.hearts,
                    inv.diamonds,
                    initial,
                    final_p,
                    final_p - initial
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        self.write_line(&format!(
            r#"{{"t":{:.3},"type":"round_end","goal_suit":"{}","common_suit":"{}","results":[{}]}}"#,
            t,
            card_str(goal_suit),
            card_str(common_suit),
            payouts_json
        ));
        self.flush();
    }
}

impl Drop for EventLogger {
    fn drop(&mut self) {
        self.flush();
    }
}

fn card_str(c: &Card) -> &'static str {
    match c {
        Card::Spade => "S",
        Card::Club => "C",
        Card::Heart => "H",
        Card::Diamond => "D",
    }
}

fn player_name_str(p: &PlayerName) -> &'static str {
    match p {
        PlayerName::Spread => "Spread",
        PlayerName::Seller => "Seller",
        PlayerName::Taker => "Taker",
        PlayerName::Noisy => "Noisy",
        PlayerName::WildestDreams => "WildestDreams",
        PlayerName::PickOff => "PickOff",
        PlayerName::TiltInventory => "TiltInventory",
        PlayerName::TheHoarder => "TheHoarder",
        PlayerName::PrayingMantis => "PrayingMantis",
        PlayerName::External => "External",
        PlayerName::None => "None",
    }
}
