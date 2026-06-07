use super::{Card, Book, Inventory, Order, Event, Update, Trade, Direction, CL, PlayerName};
use super::EventLogger;
use crate::player::external::{SidecarMsg, card_to_suit, player_name_to_str};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::UnboundedSender;
use rand::prelude::SliceRandom;
use kanal::AsyncReceiver;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::path::PathBuf;
use std::sync::Arc;
use rand::Rng;

// Gate all output on self.quiet without touching every call site.
macro_rules! qprintln {
    ($quiet:expr, $($arg:tt)*) => {
        if !$quiet { println!($($arg)*); }
    };
}
use std::collections::HashMap;

pub struct MatchMaker {
    pub round: u32,
    pub player_names: Vec<PlayerName>,
    pub suits: [Card; 4],
    pub goal_suit: Card,
    pub common_suit: Card,
    pub player_points: HashMap<PlayerName, usize>,
    pub books: HashMap<Card, Book>,
    pub player_inventories: HashMap<PlayerName, Inventory>,
    pub event_sender: Sender<Event>,
    pub order_receiver: Arc<AsyncReceiver<Order>>,
    pub rng: StdRng,
    pub base_seed: u64,
    pub log_dir: Option<PathBuf>,
    pub max_rounds: Option<u32>,
    pub round_duration_secs: u64,
    pub inter_round_sleep_secs: u64,
    pub deal_warmup_secs: u64,
    pub quiet: bool,
    pub sidecar_tx: Option<UnboundedSender<SidecarMsg>>,
}

impl MatchMaker {
    pub fn new(
        starting_balance: usize,
        player_names: Vec<PlayerName>,
        event_sender: Sender<Event>,
        order_receiver: Arc<AsyncReceiver<Order>>,
        seed: u64,
        log_dir: Option<PathBuf>,
        max_rounds: Option<u32>,
        round_duration_secs: u64,
        inter_round_sleep_secs: u64,
        deal_warmup_secs: u64,
        quiet: bool,
        sidecar_tx: Option<UnboundedSender<SidecarMsg>>,
    ) -> Self {

        let mut player_inventories = HashMap::new();
        let mut player_points = HashMap::new();
        for player_name in &player_names {
            player_points.insert(player_name.clone(), starting_balance);
            player_inventories.insert(player_name.clone(), Inventory::new());
        }

        let mut books = HashMap::new();
        books.insert(Card::Spade, Book::new());
        books.insert(Card::Club, Book::new());
        books.insert(Card::Diamond, Book::new());
        books.insert(Card::Heart, Book::new());


        Self {
            round: 0,
            player_names,
            suits: [Card::Spade, Card::Club, Card::Diamond, Card::Heart],
            goal_suit: Card::Spade,
            common_suit: Card::Club,
            player_points,
            books,
            player_inventories,
            event_sender,
            order_receiver,
            rng: StdRng::seed_from_u64(seed),
            base_seed: seed,
            log_dir,
            max_rounds,
            round_duration_secs,
            inter_round_sleep_secs,
            deal_warmup_secs,
            quiet,
            sidecar_tx,
        }
    }

    pub fn pick_new_common_suit(&mut self) {
        self.common_suit = self.suits[self.rng.gen_range(0..=3)].clone();
    }

    pub fn get_new_inventories(&mut self) -> HashMap<Card, usize> {
        let mut cards: Vec<Card> = Vec::new();
        let (goal_suit, suit_1, suit_2) = self.common_suit.get_other_cards();
        self.goal_suit = goal_suit.clone();

        for _ in 0..12 { cards.push(self.common_suit.clone()) }
        
        let mut starting_inventory = HashMap::new();

        qprintln!(self.quiet, "=---= Card Count =---=");
        qprintln!(self.quiet, "{} - {:?} | 12x{}", CL::Dull.get(), self.common_suit, CL::End.get());
        starting_inventory.insert(self.common_suit.clone(), 12);

        // randomly pick one of the other 3 suits to be the one with 8 cards
        let mut already_lucky = false;
        for (idx, suit) in [suit_1, suit_2, goal_suit].iter().enumerate() {
            let lucky_eight: bool = self.rng.gen();
            if idx == 2 && !already_lucky {
                for _ in 0..8 { cards.push(suit.clone()) }
                qprintln!(self.quiet, "{} - {:?} | 8x{}", CL::Dull.get(), suit, CL::End.get());
                starting_inventory.insert(suit.clone(), 8);
            } else {
                if !already_lucky && lucky_eight {
                    for _ in 0..8 { cards.push(suit.clone()) }
                    qprintln!(self.quiet, "{} - {:?} | 8x{}", CL::Dull.get(), suit, CL::End.get());
                    starting_inventory.insert(suit.clone(), 8);
                    already_lucky = true;
                } else {
                    for _ in 0..10 { cards.push(suit.clone()) }
                    qprintln!(self.quiet, "{} - {:?} | 10x{}", CL::Dull.get(), suit, CL::End.get());
                    starting_inventory.insert(suit.clone(), 10);
                }
            }
        }

        cards.shuffle(&mut self.rng); // randomly shuffle the cards

        let chunk_size = 40 / self.player_names.len();
        let chunks: Vec<&[Card]> = cards.chunks(chunk_size).collect();

        for (i, player_name) in self.player_names.iter().enumerate() {
            let mut player_inventory = Inventory::new();
            player_inventory.count(chunks[i].to_vec());
            self.player_inventories.insert(player_name.clone(), player_inventory.clone());
        }

        starting_inventory
    }



    pub async fn start(&mut self) {
        let round_duration = tokio::time::Duration::from_secs(self.round_duration_secs);

        loop {
            // game_id for the file we'll write this round; mixes seed and round
            // so concurrent runs with different seeds don't collide on disk.
            let game_id: u64 = self.base_seed.wrapping_add(self.round as u64);
            let logger: Option<Arc<EventLogger>> = self.log_dir.as_ref().and_then(|dir| {
                match EventLogger::new(dir, game_id, self.base_seed) {
                    Ok(l) => Some(Arc::new(l)),
                    Err(e) => {
                        eprintln!("[!] failed to open event log: {:?}", e);
                        None
                    }
                }
            });

            let mut pot = 0;
            let ante = 200 / self.player_names.len();

            qprintln!(self.quiet, "{}==================== ROUND {} ===================={}", CL::Purple.get(), self.round, CL::End.get());
            qprintln!(self.quiet, "");
            qprintln!(self.quiet, "=---= Game Details =---=");
            qprintln!(self.quiet, "{} - Players: {}x{}", CL::Dull.get(), self.player_names.len(), CL::End.get());
            qprintln!(self.quiet, "{} - Ante: {}{}", CL::Dull.get(), ante, CL::End.get());
            qprintln!(self.quiet, "{} - Pot: 200{}", CL::Dull.get(), CL::End.get());
            qprintln!(self.quiet, "");
            
            let initial_points = self.player_points.clone();
            for (player, points) in self.player_points.iter_mut() {
                if *points < ante {
                    qprintln!(self.quiet, "[!] Player {:?} does not have enough points to play", player);
                    break;
                }
                *points -= ante;
                pot += ante;
            }

            self.pick_new_common_suit();
            let starting_inventory = self.get_new_inventories();

            qprintln!(self.quiet, "{} - Common suit: {:?}{}", CL::Dull.get(), self.common_suit, CL::End.get());
            qprintln!(self.quiet, "{} - Goal suit: {}{:?}{}{}", CL::Dull.get(), CL::LimeGreen.get(), self.goal_suit, CL::End.get(), CL::End.get());
            qprintln!(self.quiet, "");

            qprintln!(self.quiet, "{}[+] Dealing cards...{}\n", CL::DimLightBlue.get(), CL::End.get());

            // Log round_start + deal with full ground truth. t=0 marks dealing.
            if let Some(l) = &logger {
                l.log_round_start(0.0, self.round, ante, pot, &self.player_names);
                l.log_deal(
                    0.0,
                    &self.player_names,
                    &self.player_inventories,
                    &self.common_suit,
                    &self.goal_suit,
                    &starting_inventory,
                );
            }
            if let Some(tx) = &self.sidecar_tx {
                let names: Vec<String> = self.player_names.iter()
                    .map(|p| player_name_to_str(p).to_string())
                    .collect();
                let _ = tx.send(SidecarMsg::RoundStart { players: names, bot_index: 0 });
                let inv = self.player_inventories
                    .get(&self.player_names[0])
                    .copied()
                    .unwrap_or_else(Inventory::new);
                let _ = tx.send(SidecarMsg::Deal {
                    s: inv.spades, c: inv.clubs, h: inv.hearts, d: inv.diamonds,
                });
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(self.deal_warmup_secs)).await; // give the players a little bit to get ready
            
            if let Err(e) = self.event_sender.send(Event::DealCards(self.player_inventories.clone())) {
                qprintln!(self.quiet, "{}[!] Error sending deal cards event: {:?}{}", CL::Red.get(), e, CL::End.get());
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // give the players some time to order their cards

            // send out the book
            let book_event = Event::Update(Update {
                spades: self.books.get(&Card::Spade).unwrap().clone(),
                clubs: self.books.get(&Card::Club).unwrap().clone(),
                diamonds: self.books.get(&Card::Diamond).unwrap().clone(),
                hearts: self.books.get(&Card::Heart).unwrap().clone(),
                trade: None,
            });
            if let Err(e) = self.event_sender.send(book_event) {
                qprintln!(self.quiet, "[!] Error sending book event: {:?}", e);
            }

            let (spades_color, clubs_color, diamonds_color, hearts_color) = self.goal_suit.get_book_colors();

            let start = tokio::time::Instant::now();
            while start.elapsed() < round_duration {

                // Bound the recv by the time left in the round so a quiet
                // order channel can't keep the round alive past its duration.
                let remaining = round_duration.saturating_sub(start.elapsed());
                let recv_result = tokio::time::timeout(remaining, self.order_receiver.recv()).await;
                if let Ok(Ok(order)) = recv_result {
                    if order.price == 0 { // No free lunches allowed
                        continue;
                    }
                    let t_now = start.elapsed().as_secs_f64();

                    qprintln!(self.quiet, "Processing order: {:?} | Queue: {}x", order, self.order_receiver.len());

                    let book = self.books.get_mut(&order.card).unwrap();
                    let trade: Option<Trade> = match order.direction {
                        Direction::Buy => {
                            if order.price >= book.ask.price {
                                qprintln!(self.quiet, "{}[-] Aggressing Player: {:?} | {:?} |:| Matched buy order!{}", CL::Green.get(), order.player_name, order.card, CL::End.get());


                                // =-= Update the Inventories =-= //
                                let buyer_inventory = self.player_inventories.get_mut(&order.player_name).unwrap();
                                buyer_inventory.change(order.card.clone(), true);

                                let seller_inventory = self.player_inventories.get_mut(&book.ask.player_name).unwrap();
                                seller_inventory.change(order.card.clone(), false);


                                // =-= Update the Points =-= //
                                let buyer_points = self.player_points.get_mut(&order.player_name).unwrap();
                                *buyer_points -= book.ask.price;

                                let seller_points = self.player_points.get_mut(&book.ask.player_name).unwrap();
                                *seller_points += book.ask.price;


                                // =-= Package Trade =-= //
                                book.last_trade = Some(book.ask.price);
                                let trade = Trade {
                                    card: order.card.clone(),
                                    price: book.ask.price,
                                    buyer: order.player_name,
                                    seller: book.ask.player_name.clone(),
                                };
                                Some(trade)

                            } else {
                                // check if this price beats the current best bid
                                if order.price > book.bid.price {
                                    // update the bid price and user_id
                                    book.bid.price = order.price;
                                    book.bid.player_name = order.player_name.clone();
                                    if let Some(l) = &logger {
                                        l.log_quote(t_now, "bid", &order.player_name, &order.card, order.price);
                                    }
                                    if let Some(tx) = &self.sidecar_tx {
                                        let _ = tx.send(SidecarMsg::Quote {
                                            side:   "bid",
                                            player: player_name_to_str(&order.player_name).to_string(),
                                            suit:   card_to_suit(&order.card),
                                            price:  order.price,
                                        });
                                    }
                                }
                                None
                            }
                        },
                        Direction::Sell => {
                            // check if the user has the inventory to sell this Card
                            let seller_inventory = self.player_inventories.get(&order.player_name).unwrap();
                            if seller_inventory.get(&order.card) == 0 {
                                qprintln!(self.quiet, "[!] {:?} | {:?} |:| Player does not have the inventory to sell this Card", order.player_name, order.card);
                                if let Some(l) = &logger {
                                    l.log_order_rejected(t_now, &order.player_name, &order.card, "no_inventory");
                                }
                                continue;
                            }

                            if order.price <= book.bid.price {
                                qprintln!(self.quiet, "{}[-] Aggressing Player: {:?} | {:?} |:| Matched sell order!{}", CL::Red.get(), order.player_name, order.card, CL::End.get());

                                // =-= Update the Inventories =-= //
                                let buyer_inventory = self.player_inventories.get_mut(&book.bid.player_name).unwrap();
                                buyer_inventory.change(order.card.clone(), true);

                                let seller_inventory = self.player_inventories.get_mut(&order.player_name).unwrap();
                                seller_inventory.change(order.card.clone(), false);


                                // =-= Update the Points =-= //
                                let buyer_points = self.player_points.get_mut(&book.bid.player_name).unwrap();
                                *buyer_points -= book.bid.price;

                                let seller_points = self.player_points.get_mut(&order.player_name).unwrap();
                                *seller_points += book.bid.price;


                                // =-= Package Trade =-= //
                                book.last_trade = Some(book.bid.price);
                                let trade = Trade {
                                    card: order.card.clone(),
                                    price: book.bid.price,
                                    buyer: book.bid.player_name.clone(),
                                    seller: order.player_name,
                                };
                                Some(trade)

                            } else {
                                // check if this price beats the current best ask
                                if order.price < book.ask.price {
                                    // update the ask price and user_id
                                    book.ask.price = order.price;
                                    book.ask.player_name = order.player_name.clone();
                                    if let Some(l) = &logger {
                                        l.log_quote(t_now, "offer", &order.player_name, &order.card, order.price);
                                    }
                                    if let Some(tx) = &self.sidecar_tx {
                                        let _ = tx.send(SidecarMsg::Quote {
                                            side:   "offer",
                                            player: player_name_to_str(&order.player_name).to_string(),
                                            suit:   card_to_suit(&order.card),
                                            price:  order.price,
                                        });
                                    }
                                }
                                None
                            }
                        },
                    };

                    if let Some(tr) = trade.clone() {
                        if let Some(l) = &logger {
                            l.log_trade(t_now, &tr.card, tr.price, &tr.buyer, &tr.seller);
                            l.log_cancel_all(t_now);
                        }
                        if let Some(tx) = &self.sidecar_tx {
                            let _ = tx.send(SidecarMsg::TradeExec {
                                suit:   card_to_suit(&tr.card),
                                price:  tr.price,
                                buyer:  player_name_to_str(&tr.buyer).to_string(),
                                seller: player_name_to_str(&tr.seller).to_string(),
                            });
                            let _ = tx.send(SidecarMsg::CancelAll);
                        }
                    }
                    if let Some(_) = trade.clone() {
                        // =-= Reset all the Books =-= //
                        self.books.get_mut(&Card::Spade).unwrap().reset_quotes();
                        self.books.get_mut(&Card::Club).unwrap().reset_quotes();
                        self.books.get_mut(&Card::Diamond).unwrap().reset_quotes();
                        self.books.get_mut(&Card::Heart).unwrap().reset_quotes();

                        // =-= Drain the Order Receiver =-= //
                        let drain_amount = self.order_receiver.len();
                        for _ in 0..drain_amount {
                            let _ = self.order_receiver.try_recv();
                        }
                    }

                    // =-= Print the Game =-= //
                    qprintln!(self.quiet, "\n=---------------------------------------------------------------------------------=");

                    let spades = self.books.get(&Card::Spade).unwrap();
                    let clubs = self.books.get(&Card::Club).unwrap();
                    let diamonds = self.books.get(&Card::Diamond).unwrap();
                    let hearts = self.books.get(&Card::Heart).unwrap();
                    qprintln!(self.quiet, "{}Spades    {}|:| Bid: ({}{:?}{}, {:?}) | Ask: ({}{:?}{}, {:?}) |:|{} Last trade: {}{:?}{}", spades_color.get(), CL::Dull.get(), CL::Green.get(), spades.bid.price,    CL::Dull.get(), spades.bid.player_name,    CL::PeachRed.get(),  spades.ask.price,    CL::Dull.get(),  spades.ask.player_name,    CL::Dull.get(),  CL::DimLightBlue.get(),  spades.last_trade.unwrap_or_default(),    CL::End.get());
                    qprintln!(self.quiet, "{}Clubs     {}|:| Bid: ({}{:?}{}, {:?}) | Ask: ({}{:?}{}, {:?}) |:|{} Last trade: {}{:?}{}", clubs_color.get(), CL::Dull.get(), CL::Green.get(), clubs.bid.price,     CL::Dull.get(), clubs.bid.player_name,     CL::PeachRed.get(),  clubs.ask.price,     CL::Dull.get(),  clubs.ask.player_name,     CL::Dull.get(),  CL::DimLightBlue.get(),  clubs.last_trade.unwrap_or_default(),     CL::End.get());
                    qprintln!(self.quiet, "{}Diamonds  {}|:| Bid: ({}{:?}{}, {:?}) | Ask: ({}{:?}{}, {:?}) |:|{} Last trade: {}{:?}{}", diamonds_color.get(), CL::Dull.get(), CL::Green.get(), diamonds.bid.price,  CL::Dull.get(), diamonds.bid.player_name,  CL::PeachRed.get(),  diamonds.ask.price,  CL::Dull.get(),  diamonds.ask.player_name,  CL::Dull.get(),  CL::DimLightBlue.get(),  diamonds.last_trade.unwrap_or_default(),  CL::End.get());
                    qprintln!(self.quiet, "{}Hearts    {}|:| Bid: ({}{:?}{}, {:?}) | Ask: ({}{:?}{}, {:?}) |:|{} Last trade: {}{:?}{}", hearts_color.get(), CL::Dull.get(), CL::Green.get(), hearts.bid.price,    CL::Dull.get(), hearts.bid.player_name,    CL::PeachRed.get(),  hearts.ask.price,    CL::Dull.get(),  hearts.ask.player_name,    CL::Dull.get(),  CL::DimLightBlue.get(),  hearts.last_trade.unwrap_or_default(),    CL::End.get());
                    
                    let mut inventory_string = format!("{}Points    {}|:|{} ", CL::DullGreen.get(), CL::Dull.get(), CL::DullGreen.get());
                    for player_name in &self.player_names {
                        let player_points = self.player_points.get(player_name).unwrap();
                        inventory_string += &format!("{:?}: {} | ", player_name, player_points);
                    }
                    inventory_string.truncate(inventory_string.len() - 3);

                    qprintln!(self.quiet, "{}{}", inventory_string, CL::End.get());
                    qprintln!(self.quiet, "=---------------------------------------------------------------------------------=\n");

                    let update = Update {
                        spades: self.books.get(&Card::Spade).unwrap().clone(),
                        clubs: self.books.get(&Card::Club).unwrap().clone(),
                        diamonds: self.books.get(&Card::Diamond).unwrap().clone(),
                        hearts: self.books.get(&Card::Heart).unwrap().clone(),
                        trade,
                    };
                    let update_event = Event::Update(update);

                    //qprintln!(self.quiet, "{}[+] Done processing request{}", CL::Green.get(), CL::End.get());

                    if let Err(e) = self.event_sender.send(update_event) {
                        qprintln!(self.quiet, "[!] Error sending update event: {:?}", e);
                    }
                }
            } 

            // =-= End the Round =-= //
            let end_round = Event::EndRound;
            if let Err(e) = self.event_sender.send(end_round) {
                qprintln!(self.quiet, "[!] Error sending end round event: {:?}", e);
            }

            qprintln!(self.quiet, "");
            qprintln!(self.quiet, "{}=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-={}", CL::Pink.get(), CL::End.get());
            qprintln!(self.quiet, "{}=-=-=-=-=-=-=-=-=-=-=-=-=-=-= Round over! =-=-=-=-=-=-=-=-=-=-=-=-=-=-={}", CL::Pink.get(), CL::End.get());
            qprintln!(self.quiet, "{}=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-={}", CL::Pink.get(), CL::End.get());
            qprintln!(self.quiet, "");
            
            qprintln!(self.quiet, "=---= Game Details =---=");
            qprintln!(self.quiet, "{} - Players: {}x{}", CL::Dull.get(), self.player_names.len(), CL::End.get());
            qprintln!(self.quiet, "{} - Ante: {}{}", CL::Dull.get(), ante, CL::End.get());
            qprintln!(self.quiet, "{} - Pot: {}{}", CL::Dull.get(), pot, CL::End.get());
            qprintln!(self.quiet, "");
            qprintln!(self.quiet, "=---= Card Count =---=");
            for (suit, amount) in starting_inventory {
                qprintln!(self.quiet, "{} - {:?} | {}x{}", CL::Dull.get(), suit, amount, CL::End.get());
            }
            qprintln!(self.quiet, "{} - Common suit: {:?}{}", CL::Dull.get(), self.common_suit, CL::End.get());
            qprintln!(self.quiet, "{} - Goal suit: {}{:?}{}{}", CL::Dull.get(), CL::LimeGreen.get(), self.goal_suit, CL::End.get(), CL::End.get());
            qprintln!(self.quiet, "");

            self.round += 1;

            // calculate the scores, each player is awared goal_suit * 10
            // and the player with the most of the goal_suit is awarded 50

            // get each players inventory and if add their points, simulatentously subtracting from pot
            let mut winner: (PlayerName, usize) = (PlayerName::None, 0); // player_id, goal_cards
            let mut tied_winnders: Vec<PlayerName> = Vec::new(); // player_ids

            qprintln!(self.quiet, "=---------------------------- Inventory ----------------------------=");
            for player_name in &self.player_names {
                let inventory = self.player_inventories.get(player_name).unwrap();
                let player_points = self.player_points.get_mut(player_name).unwrap();
                let goal_cards = match self.goal_suit {
                    Card::Spade => inventory.spades,
                    Card::Club => inventory.clubs,
                    Card::Diamond => inventory.diamonds,
                    Card::Heart => inventory.hearts,
                };

                let (spade_color, club_color, diamond_color, heart_color) = match self.goal_suit {
                    Card::Spade => (CL::LimeGreen.get(), CL::Dull.get(), CL::Dull.get(), CL::Dull.get()),
                    Card::Club => (CL::Dull.get(), CL::LimeGreen.get(), CL::Dull.get(), CL::Dull.get()),
                    Card::Diamond => (CL::Dull.get(), CL::Dull.get(), CL::LimeGreen.get(), CL::Dull.get()),
                    Card::Heart => (CL::Dull.get(), CL::Dull.get(), CL::Dull.get(), CL::LimeGreen.get()),
                };

                qprintln!(self.quiet, "{}{}{:?}{} |:| Spades: {}{}x{} | Clubs: {}{}x{} | Diamonds: {}{}x{} | Hearts: {}{}x{}{}", CL::Dull.get(), CL::DimLightBlue.get(), player_name, CL::Dull.get(), spade_color, inventory.spades, CL::Dull.get(), club_color, inventory.clubs, CL::Dull.get(), diamond_color, inventory.diamonds, CL::Dull.get(), heart_color, inventory.hearts, CL::End.get(), CL::End.get());

                if goal_cards >= winner.1 {
                    if goal_cards == winner.1 {
                        tied_winnders.push(player_name.clone());
                    } else {
                        winner = (player_name.clone(), goal_cards);
                        tied_winnders.clear();
                    }
                }

                *player_points += goal_cards * 10;
                pot -= goal_cards * 10;
            }
            qprintln!(self.quiet, "");

            // if there's one winner, award them the pot
            // if there's a tie, split the pot evenly between the winners

            qprintln!(self.quiet, "=----------------------------- Results -----------------------------=");
            if tied_winnders.is_empty() {
                qprintln!(self.quiet, "{}[+] Player '{:?}' wins the whole pot of {} points{}", CL::Green.get(), winner.0, pot, CL::End.get());
                let winner_points = self.player_points.get_mut(&winner.0).unwrap();
                *winner_points += pot;
            } else {
                let split = pot / (tied_winnders.len() + 1);
                qprintln!(self.quiet, "{}[+] Players tie for the pot of {} points{}\n", CL::Teal.get(), pot, CL::End.get());
                qprintln!(self.quiet, "{}------ Tied Players ------{}", CL::Dull.get(), CL::End.get());
                qprintln!(self.quiet, "{}{}{:?}{} | Goal Cards: {}x | Points: {}+{}x{}{}", CL::Dull.get(), CL::DimLightBlue.get(), winner.0, CL::Dull.get(), winner.1, CL::LimeGreen.get(), split, CL::End.get(), CL::End.get());
                for player_name in tied_winnders {
                    qprintln!(self.quiet, "{}{}{:?}{} | Goal Cards: {}x | Points: {}+{}x{}{}", CL::Dull.get(), CL::DimLightBlue.get(), player_name, CL::Dull.get(), winner.1, CL::LimeGreen.get(), split, CL::End.get(), CL::End.get());
                    let player_points = self.player_points.get_mut(&player_name).unwrap();
                    *player_points += split;
                }
            }
            qprintln!(self.quiet, "");

            qprintln!(self.quiet, "=-------------------------- Updated Points -------------------------=");
            let mut inventory_string = String::from("");
            for player_name in &self.player_names {
                let initial_points = initial_points.get(player_name).unwrap();
                let player_points = self.player_points.get(player_name).unwrap();
                let point_change: i32 = *player_points as i32 - *initial_points as i32;

                let change_color = match point_change {
                    x if x > 0 => CL::Green.get(),
                    x if x < 0 => CL::Red.get(),
                    _ => CL::Dull.get(),
                };

                inventory_string += &format!("{:?}: {} {}({}){} | ", player_name, player_points, change_color, point_change, CL::Dull.get());
            }
            inventory_string.truncate(inventory_string.len() - 3);
            qprintln!(self.quiet, "{}{}{}", CL::Dull.get(), inventory_string, CL::End.get());
            qprintln!(self.quiet, "");

            // Round-end ground truth: final hands + per-player P&L.
            if let Some(l) = &logger {
                l.log_round_end(
                    self.round_duration_secs as f64,
                    &self.player_names,
                    &self.goal_suit,
                    &self.common_suit,
                    &self.player_inventories,
                    &initial_points,
                    &self.player_points,
                );
            }
            if let Some(tx) = &self.sidecar_tx {
                let _ = tx.send(SidecarMsg::RoundEnd { goal_suit: card_to_suit(&self.goal_suit) });
            }
            drop(logger);

            if let Some(max) = self.max_rounds {
                if self.round >= max {
                    // Players' tasks are blocked on broadcast::recv, which will
                    // never unblock cleanly. Exit the process so the smoke test
                    // and corpus generation terminate.
                    qprintln!(self.quiet, "[+] reached --max-rounds={}, exiting", max);
                    std::process::exit(0);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(self.inter_round_sleep_secs)).await;

        }

    }

}