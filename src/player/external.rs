use super::{Card, Direction, Order, PlayerName};
use kanal::AsyncSender;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc::UnboundedReceiver;

// ── SidecarMsg ───────────────────────────────────────────────────────────────

pub enum SidecarMsg {
    RoundStart { players: Vec<String>, bot_index: usize },
    Deal { s: usize, c: usize, h: usize, d: usize },
    Quote { side: &'static str, player: String, suit: &'static str, price: usize },
    TradeExec { suit: &'static str, price: usize, buyer: String, seller: String },
    CancelAll,
    RoundEnd { goal_suit: &'static str },
}

impl SidecarMsg {
    pub fn to_json(&self) -> String {
        match self {
            SidecarMsg::RoundStart { players, bot_index } => {
                let arr = players
                    .iter()
                    .enumerate()
                    .map(|(i, n)| format!(r#"{{"idx":{},"name":"{}"}}"#, i, n))
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    r#"{{"type":"round_start","players":[{}],"bot_index":{}}}"#,
                    arr, bot_index
                )
            }
            SidecarMsg::Deal { s, c, h, d } => format!(
                r#"{{"type":"deal","hands":[{{"player":"External","S":{},"C":{},"H":{},"D":{}}}]}}"#,
                s, c, h, d
            ),
            SidecarMsg::Quote { side, player, suit, price } => format!(
                r#"{{"type":"{}","player":"{}","suit":"{}","price":{}}}"#,
                side, player, suit, price
            ),
            SidecarMsg::TradeExec { suit, price, buyer, seller } => format!(
                r#"{{"type":"trade","suit":"{}","price":{},"buyer":"{}","seller":"{}"}}"#,
                suit, price, buyer, seller
            ),
            SidecarMsg::CancelAll => r#"{"type":"cancel_all"}"#.to_string(),
            SidecarMsg::RoundEnd { goal_suit } => {
                format!(r#"{{"type":"round_end","goal_suit":"{}"}}"#, goal_suit)
            }
        }
    }

    pub fn needs_your_turn(&self) -> bool {
        matches!(
            self,
            SidecarMsg::Quote { .. } | SidecarMsg::TradeExec { .. } | SidecarMsg::CancelAll
        )
    }
}

// ── Utility fns (pub so MatchMaker can use them) ─────────────────────────────

pub fn card_to_suit(c: &Card) -> &'static str {
    match c {
        Card::Spade   => "S",
        Card::Club    => "C",
        Card::Heart   => "H",
        Card::Diamond => "D",
    }
}

pub fn player_name_to_str(p: &PlayerName) -> &'static str {
    match p {
        PlayerName::Spread        => "Spread",
        PlayerName::Seller        => "Seller",
        PlayerName::Taker         => "Taker",
        PlayerName::Noisy         => "Noisy",
        PlayerName::WildestDreams => "WildestDreams",
        PlayerName::PickOff       => "PickOff",
        PlayerName::TiltInventory => "TiltInventory",
        PlayerName::TheHoarder    => "TheHoarder",
        PlayerName::PrayingMantis => "PrayingMantis",
        PlayerName::External      => "External",
        PlayerName::None          => "None",
    }
}

// ── ExternalPlayer ───────────────────────────────────────────────────────────

pub struct ExternalPlayer {
    sidecar_rx: UnboundedReceiver<SidecarMsg>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    order_sender: Arc<AsyncSender<Order>>,
    _child: Child,
}

impl ExternalPlayer {
    pub async fn spawn(
        cmd: &str,
        sidecar_rx: UnboundedReceiver<SidecarMsg>,
        order_sender: Arc<AsyncSender<Order>>,
    ) -> std::io::Result<Self> {
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        let (prog, args) = parts.split_first().expect("empty --external-player-cmd");
        let mut child = Command::new(prog)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
        Ok(Self { sidecar_rx, stdin, stdout, order_sender, _child: child })
    }

    async fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await
    }

    pub async fn start(&mut self) {
        while let Some(msg) = self.sidecar_rx.recv().await {
            let json = msg.to_json();
            let prompt = msg.needs_your_turn();
            if let Err(e) = self.write_line(&json).await {
                eprintln!("[ExternalPlayer] stdin write error: {:?}", e);
                return;
            }
            if !prompt {
                continue;
            }
            if let Err(e) = self.write_line(r#"{"type":"your_turn"}"#).await {
                eprintln!("[ExternalPlayer] stdin write error: {:?}", e);
                return;
            }
            let mut line = String::new();
            let result = tokio::time::timeout(
                tokio::time::Duration::from_millis(200),
                self.stdout.read_line(&mut line),
            )
            .await;
            if let Ok(Ok(_)) = result {
                if let Some(order) = parse_bot_order(line.trim()) {
                    let _ = self.order_sender.send(order).await;
                }
            }
        }
    }
}

fn parse_bot_order(s: &str) -> Option<Order> {
    if s.is_empty() {
        return None;
    }
    let action = json_str(s, "action")?;
    if action == "pass" {
        return None;
    }
    let suit_str = json_str(s, "suit")?;
    let price = json_num(s, "price")?;
    let card = match suit_str.as_str() {
        "S" => Card::Spade,
        "C" => Card::Club,
        "H" => Card::Heart,
        "D" => Card::Diamond,
        _ => return None,
    };
    let direction = match action.as_str() {
        "bid"   => Direction::Buy,
        "offer" => Direction::Sell,
        _ => return None,
    };
    Some(Order { player_name: PlayerName::External, price, direction, card })
}

fn json_str(s: &str, key: &str) -> Option<String> {
    let needle = format!(r#""{}":""#, key);
    let start = s.find(&needle)? + needle.len();
    let end = s[start..].find('"')? + start;
    Some(s[start..end].to_string())
}

fn json_num(s: &str, key: &str) -> Option<usize> {
    let needle = format!(r#""{}":"#, key);
    let start = s.find(&needle)? + needle.len();
    let rest = s[start..].trim_start_matches(' ');
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bid_order() {
        let s = r#"{"action":"bid","suit":"S","price":12}"#;
        let order = parse_bot_order(s).expect("should parse");
        assert_eq!(order.price, 12);
        assert!(matches!(order.direction, Direction::Buy));
        assert!(matches!(order.card, Card::Spade));
        assert!(matches!(order.player_name, PlayerName::External));
    }

    #[test]
    fn parse_offer_order() {
        let s = r#"{"action":"offer","suit":"H","price":8}"#;
        let order = parse_bot_order(s).expect("should parse");
        assert_eq!(order.price, 8);
        assert!(matches!(order.direction, Direction::Sell));
        assert!(matches!(order.card, Card::Heart));
    }

    #[test]
    fn parse_pass_returns_none() {
        let s = r#"{"action":"pass"}"#;
        assert!(parse_bot_order(s).is_none());
    }

    #[test]
    fn parse_empty_returns_none() {
        assert!(parse_bot_order("").is_none());
    }

    #[test]
    fn parse_garbage_returns_none() {
        assert!(parse_bot_order("not json").is_none());
    }

    #[test]
    fn quote_to_json() {
        let m = SidecarMsg::Quote {
            side: "bid",
            player: "External".to_string(),
            suit: "S",
            price: 12,
        };
        assert_eq!(m.to_json(), r#"{"type":"bid","player":"External","suit":"S","price":12}"#);
    }

    #[test]
    fn round_start_to_json() {
        let m = SidecarMsg::RoundStart {
            players: vec!["External".to_string(), "Noisy".to_string()],
            bot_index: 0,
        };
        let expected = r#"{"type":"round_start","players":[{"idx":0,"name":"External"},{"idx":1,"name":"Noisy"}],"bot_index":0}"#;
        assert_eq!(m.to_json(), expected);
    }

    #[test]
    fn cancel_all_needs_your_turn() {
        assert!(SidecarMsg::CancelAll.needs_your_turn());
    }

    #[test]
    fn round_start_does_not_need_your_turn() {
        let m = SidecarMsg::RoundStart { players: vec![], bot_index: 0 };
        assert!(!m.needs_your_turn());
    }
}
