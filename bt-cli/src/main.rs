use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use bt_core::engine::{State, Torrent};
use bt_core::error::PeerError;
use bt_core::hex;
use bt_core::metainfo::{Content, MetaInfo};
use bt_core::peer::{self, Message, PeerConfig};
use bt_core::peer_id;
use bt_core::tracker::{self, AnnounceRequest, Event};
use tokio::task::JoinSet;

const DEFAULT_PORT: u16 = 6881;
const NUMWANT: u32 = 50;
const PROBE_PEER_LIMIT: usize = 10;

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return ExitCode::FAILURE;
    };
    let result = match command.as_str() {
        "peers" => match args.next() {
            Some(path) => run_peers(&path).await,
            None => {
                print_usage();
                return ExitCode::FAILURE;
            }
        },
        "probe" => match args.next() {
            Some(path) => run_probe(&path).await,
            None => {
                print_usage();
                return ExitCode::FAILURE;
            }
        },
        "download" => match (args.next(), args.next()) {
            (Some(path), Some(output)) => run_download(&path, &output).await,
            _ => {
                print_usage();
                return ExitCode::FAILURE;
            }
        },
        path => run_info(path),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    eprintln!("usage: bt-cli <torrent-file>");
    eprintln!("       bt-cli peers <torrent-file>");
    eprintln!("       bt-cli probe <torrent-file>");
    eprintln!("       bt-cli download <torrent-file> <output-dir>");
}

fn run_info(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read(path)?;
    let meta = MetaInfo::from_bytes(&raw)?;
    println!("name:         {}", meta.info.name);
    let total = meta.info.total_length()?;
    match &meta.info.content {
        Content::Single { length } => println!("size:         {} (single file)", human(*length)),
        Content::Multi { files } => {
            println!("size:         {} ({} files)", human(total), files.len());
            for file in files {
                println!("  - {} ({})", file.path.join("/"), human(file.length));
            }
        }
    }
    println!("piece length: {} B", meta.info.piece_length);
    println!("pieces:       {}", meta.info.pieces.len());
    println!(
        "private:      {}",
        if meta.info.private { "yes" } else { "no" }
    );
    if let Some(announce) = &meta.announce {
        println!("announce:     {announce}");
    }
    for (index, tier) in meta.announce_list.iter().enumerate() {
        println!("tier {index}:      {}", tier.join(", "));
    }
    println!("info hash:    {}", hex::encode(&meta.info_hash));
    Ok(())
}

async fn run_peers(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read(path)?;
    let meta = MetaInfo::from_bytes(&raw)?;
    let client = tracker::http_client()?;
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: DEFAULT_PORT,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length()?,
        numwant: NUMWANT,
        event: Some(Event::Started),
    };
    let outcome = tracker::announce(&client, &meta, &request).await?;
    println!("tracker:      {}", outcome.url);
    println!("interval:     {} s", outcome.response.interval);
    if let Some(min_interval) = outcome.response.min_interval {
        println!("min interval: {min_interval} s");
    }
    println!("complete:     {}", outcome.response.complete);
    println!("incomplete:   {}", outcome.response.incomplete);
    println!("peers ({}):", outcome.response.peers.len());
    for peer in &outcome.response.peers {
        println!("  - {peer}");
    }
    Ok(())
}

struct ProbeInfo {
    client: String,
    completion: Option<f64>,
    unchoked: bool,
}

async fn probe_peer(
    addr: SocketAddr,
    info_hash: [u8; 20],
    our_peer_id: [u8; 20],
    piece_count: usize,
) -> Result<ProbeInfo, PeerError> {
    let config = PeerConfig {
        read_timeout: Duration::from_secs(15),
        keep_alive_interval: Duration::from_secs(10),
        ..PeerConfig::default()
    };
    let mut conn = peer::connect(addr, info_hash, our_peer_id, piece_count, config).await?;
    let client = peer_id::client_name(&conn.remote_peer_id());
    conn.write_message(&Message::Interested).await?;
    let mut completion = None;
    let mut unchoked = false;
    loop {
        match conn.read_message().await {
            Ok(Message::Bitfield(bitfield)) => {
                completion = Some(bitfield.count() as f64 / piece_count as f64 * 100.0);
            }
            Ok(Message::Unchoke) => unchoked = true,
            Ok(_) => {}
            Err(PeerError::Timeout) => break,
            Err(err) => return Err(err),
        }
        if completion.is_some() && unchoked {
            break;
        }
    }
    Ok(ProbeInfo {
        client,
        completion,
        unchoked,
    })
}

async fn run_probe(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read(path)?;
    let meta = MetaInfo::from_bytes(&raw)?;
    let client = tracker::http_client()?;
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: DEFAULT_PORT,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length()?,
        numwant: NUMWANT,
        event: Some(Event::Started),
    };
    let outcome = tracker::announce(&client, &meta, &request).await?;
    let piece_count = meta.info.pieces.len();
    println!("tracker:      {}", outcome.url);
    println!(
        "probing up to {PROBE_PEER_LIMIT} of {} peers ({} pieces)...",
        outcome.response.peers.len(),
        piece_count
    );
    println!(
        "  {:<22} {:<26} {:>9}  unchoked",
        "peer", "client", "bitfield"
    );

    let mut tasks = JoinSet::new();
    for addr in outcome.response.peers.into_iter().take(PROBE_PEER_LIMIT) {
        let info_hash = meta.info_hash;
        let our_peer_id = *peer_id::session();
        tasks.spawn(async move {
            let result = probe_peer(addr, info_hash, our_peer_id, piece_count).await;
            (addr, result)
        });
    }
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((addr, Ok(info))) => {
                let completion = info
                    .completion
                    .map(|value| format!("{value:.1}%"))
                    .unwrap_or_else(|| "n/a".to_string());
                println!(
                    "  {:<22} {:<26} {:>9}  {}",
                    addr,
                    info.client,
                    completion,
                    if info.unchoked { "yes" } else { "no" }
                );
            }
            Ok((addr, Err(err))) => {
                println!("  {addr:<22} error: {err}");
            }
            Err(err) => {
                println!("  task failed: {err}");
            }
        }
    }
    Ok(())
}

async fn run_download(path: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read(path)?;
    let meta = MetaInfo::from_bytes(&raw)?;
    let torrent = Torrent::spawn(meta, output.into()).await?;
    let mut stats = torrent.subscribe();
    let mut last_width = 0usize;
    loop {
        let snapshot = stats.borrow().clone();
        let percent = if snapshot.piece_count > 0 {
            snapshot.verified_pieces as f64 / snapshot.piece_count as f64 * 100.0
        } else {
            0.0
        };
        let line = format!(
            "\r[{:>6.2}%] {:>10}/s  {:>2} peers  {:<11} {:>10} / {}",
            percent,
            human(snapshot.download_rate as u64),
            snapshot.peer_count,
            format!("{:?}", snapshot.state),
            human(snapshot.session_downloaded),
            human(snapshot.total_length),
        );
        let width = line.len();
        print!("{:<width$}", line, width = last_width.max(width));
        std::io::stdout().flush()?;
        last_width = width;
        if matches!(
            snapshot.state,
            State::Completed | State::Stopped | State::Error
        ) {
            println!();
            if snapshot.state == State::Completed {
                torrent.stop().await?;
            }
            break;
        }
        if stats.changed().await.is_err() {
            break;
        }
    }
    Ok(())
}

fn human(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.2} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.2} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}
