use futures::{FutureExt, select};
use futures_timer::Delay;
use matchbox_socket::{PeerState, WebRtcSocket};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::info;

const CHANNEL_ID: usize = 0;
const LOG_FILTER: &str = "info,firestore_signalling=debug,matchbox_socket=debug";

mod firestore_signaller;

#[cfg(target_arch = "wasm32")]
mod browser_url;

#[cfg(target_arch = "wasm32")]
fn main() {
    use tracing::{Level, subscriber::set_global_default};
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    let layer_config = tracing_wasm::WASMLayerConfigBuilder::new()
        .set_max_level(Level::INFO)
        .build();
    let layer = tracing_wasm::WASMLayer::new(layer_config);
    let filter: tracing_subscriber::EnvFilter = LOG_FILTER.parse().unwrap();

    let event_format = tracing_subscriber::fmt::format()
        .with_level(false)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_line_number(false)
        .with_source_location(false)
        .with_file(false)
        .with_ansi(false)
        .compact()
        .without_time();

    let reg = Registry::default().with(layer).with(filter).with(
        tracing_subscriber::fmt::layer()
            .without_time()
            .event_format(event_format),
    );

    console_error_panic_hook::set_once();
    let _ = set_global_default(reg);

    wasm_bindgen_futures::spawn_local(async_main(Some(browser_url::get_or_set_room_hash())));
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| LOG_FILTER.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    async_main(std::env::args().nth(1)).await
}

#[cfg(not(target_arch = "wasm32"))]
fn generate_room_code() -> String {
    use uuid::Uuid;
    // Generate a 4-letter uppercase room code from UUID
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();

    // Use first 4 bytes to generate 4 letters (A-Z)
    let code: String = bytes[..4]
        .iter()
        .map(|&b| {
            let letter = b'A' + (b % 26);
            letter as char
        })
        .collect();

    code
}

async fn async_main(room_id: Option<String>) {
    #[cfg(not(target_arch = "wasm32"))]
    let (room_id, is_host) = match room_id {
        Some(code) => {
            let code = code.to_uppercase();
            info!("╔══════════════════════════════════════╗");
            info!("║        CLIENT MODE - JOINING         ║");
            info!("╠══════════════════════════════════════╣");
            info!("║  Room Code: {:<4}                     ║", code);
            info!("╚══════════════════════════════════════╝");
            (code, false)
        }
        None => {
            let code = generate_room_code();
            info!("╔══════════════════════════════════════╗");
            info!("║         HOST MODE - LISTENING        ║");
            info!("╠══════════════════════════════════════╣");
            info!("║  Room Code: {:<4}                     ║", code);
            info!("║                                      ║");
            info!("║  Share this code with clients!       ║");
            info!("║  Clients join with:                  ║");
            info!("║  cargo run -- {:<4}                   ║", code);
            info!("╚══════════════════════════════════════╝");
            (code, true)
        }
    };

    #[cfg(target_arch = "wasm32")]
    let (room_id, is_host) = {
        let id = room_id.unwrap_or_else(|| format!("room-{}", uuid::Uuid::new_v4()));
        info!("Connecting via Firestore signalling. room={}", id);
        (id, false)
    };

    let _ = is_host; // Mark as used to avoid warnings

    let config = firestore_signaller::FirestoreConfig { room_id };
    let builder = Arc::new(firestore_signaller::FirestoreSignallerBuilder::new(config));

    let (mut socket, loop_fut) = WebRtcSocket::builder(String::new())
        .signaller_builder(builder)
        .add_reliable_channel()
        .build();

    let loop_fut = loop_fut.fuse();
    futures::pin_mut!(loop_fut);

    let timeout = Delay::new(Duration::from_millis(100));
    futures::pin_mut!(timeout);

    // Track peers that just connected and when to send initial messages
    let mut pending_greetings: HashMap<matchbox_protocol::PeerId, web_time::Instant> =
        HashMap::new();

    loop {
        for (peer, state) in socket.update_peers() {
            match state {
                PeerState::Connected => {
                    info!("Peer joined: {peer}");
                    // Schedule greeting for 100ms from now to let data channel open
                    pending_greetings
                        .insert(peer, web_time::Instant::now() + Duration::from_millis(100));
                }
                PeerState::Disconnected => {
                    info!("Peer left: {peer}");
                    pending_greetings.remove(&peer);
                }
            }
        }

        // Send greetings to peers whose data channels should be ready
        let now = web_time::Instant::now();
        let ready_peers: Vec<_> = pending_greetings
            .iter()
            .filter(|(_, ready_at)| now >= **ready_at)
            .map(|(peer, _)| *peer)
            .collect();

        for peer in ready_peers {
            pending_greetings.remove(&peer);

            let packet = "hello friend!".as_bytes().to_vec().into_boxed_slice();
            socket.channel_mut(CHANNEL_ID).send(packet, peer);

            let packet = format!(
                "ping {}",
                web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            )
            .as_bytes()
            .to_vec()
            .into_boxed_slice();
            socket.channel_mut(CHANNEL_ID).send(packet, peer);
        }

        for (peer, packet) in socket.channel_mut(CHANNEL_ID).receive() {
            let message = String::from_utf8_lossy(&packet);
            if message.contains("ping") {
                let ts = message
                    .split(' ')
                    .nth(1)
                    .unwrap_or("0")
                    .parse::<u128>()
                    .unwrap_or(0);
                let packet = format!("pong {ts}").as_bytes().to_vec().into_boxed_slice();
                socket.channel_mut(CHANNEL_ID).send(packet, peer);
            } else if message.contains("pong") {
                let ts = message
                    .split(' ')
                    .nth(1)
                    .unwrap_or("0")
                    .parse::<u128>()
                    .unwrap_or(0);
                let now = web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u128;
                let diff_ms = now.saturating_sub(ts);
                info!("peer {peer} ping: {diff_ms}ms");
            } else {
                info!("Message from {peer}: {message:?}");
            }
        }

        select! {
            _ = (&mut timeout).fuse() => {
                timeout.reset(Duration::from_millis(100));
            }
            _ = &mut loop_fut => break,
        }
    }
}
