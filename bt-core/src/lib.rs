#![deny(clippy::print_stdout, clippy::print_stderr)]
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod bencode;
pub mod dht;
pub mod engine;
pub mod error;
pub mod extensions;
pub mod hex;
pub mod listener;
pub mod magnet;
pub mod metainfo;
pub mod paths;
pub mod peer;
pub mod peer_id;
pub mod percent;
pub mod ratelimit;
pub mod session;
pub mod tracker;
pub mod tracker_udp;
