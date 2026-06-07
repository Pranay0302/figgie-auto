use super::{Card, Direction, Book, Trade, Inventory, Order, Event, CL};

pub mod event_driven;
pub use event_driven::*;

pub mod generic;
pub use generic::GenericPlayer;

pub mod tilt;
pub use tilt::TiltInventory;

pub mod external;
pub use external::{ExternalPlayer, SidecarMsg, card_to_suit, player_name_to_str};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum PlayerName {
    Spread,
    Seller,
    Taker,
    Noisy,
    WildestDreams,
    PickOff,
    TiltInventory,
    TheHoarder,
    PrayingMantis,
    External,
    None,
}
