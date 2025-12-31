use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use futures::{
    FutureExt, StreamExt,
    channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded},
};
use matchbox_protocol::PeerId;
use matchbox_socket::{
    PeerEvent, PeerRequest, SignalingError, Signaller, SignallerBuilder, async_trait::async_trait,
};

use crate::firestore_api::{
    FirestoreClient, extract_peer_last_seen, now_ms, parse_signal, peer_id_from_string,
    peer_id_to_string,
};
use crate::http_client;

#[derive(Clone, Debug)]
pub struct FirestoreConfig {
    pub room_id: String,
}

#[derive(Debug)]
pub struct FirestoreSignallerBuilder {
    client: FirestoreClient,
    room_id: String,
}

impl FirestoreSignallerBuilder {
    pub fn new(config: FirestoreConfig) -> Self {
        Self {
            client: FirestoreClient::new(),
            room_id: config.room_id,
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SignallerBuilder for FirestoreSignallerBuilder {
    async fn new_signaller(
        &self,
        _attempts: Option<u16>,
        _room_url: String,
    ) -> Result<Box<dyn Signaller>, SignalingError> {
        let our_id = PeerId(uuid::Uuid::new_v4());

        let (event_tx, event_rx) = unbounded::<PeerEvent>();
        let (req_tx, req_rx) = unbounded::<PeerRequest>();

        event_tx
            .unbounded_send(PeerEvent::IdAssigned(our_id))
            .map_err(|e| SignalingError::UserImplementationError(format!("{e:#?}")))?;

        let signaller = FirestoreSignaller { event_rx, req_tx };

        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(run_firestore_loop(
            self.client.clone(),
            self.room_id.clone(),
            our_id,
            event_tx,
            req_rx,
        ));

        #[cfg(not(target_arch = "wasm32"))]
        tokio::spawn(run_firestore_loop(
            self.client.clone(),
            self.room_id.clone(),
            our_id,
            event_tx,
            req_rx,
        ));

        Ok(Box::new(signaller))
    }
}

struct FirestoreSignaller {
    event_rx: UnboundedReceiver<PeerEvent>,
    req_tx: UnboundedSender<PeerRequest>,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl Signaller for FirestoreSignaller {
    async fn send(&mut self, request: PeerRequest) -> Result<(), SignalingError> {
        self.req_tx
            .unbounded_send(request)
            .map_err(|e| SignalingError::UserImplementationError(format!("{e:#?}")))?;
        Ok(())
    }

    async fn next_message(&mut self) -> Result<PeerEvent, SignalingError> {
        self.event_rx
            .next()
            .await
            .ok_or(SignalingError::StreamExhausted)
    }
}

// ============================================================================
// Helper functions for the main loop
// ============================================================================

// Helper to detect peers that should be marked as left
fn detect_left_peers(
    known_peers: &BTreeSet<PeerId>,
    active: &BTreeSet<PeerId>,
    last_seen_by_peer: &BTreeMap<PeerId, u128>,
    missing_polls_by_peer: &mut BTreeMap<PeerId, u32>,
    now: u128,
    stale_ms: u128,
    threshold: u32,
) -> Vec<PeerId> {
    let mut peers_to_remove = Vec::new();

    for &peer in known_peers {
        if active.contains(&peer) {
            missing_polls_by_peer.remove(&peer);
            continue;
        }

        // Only track missing if we've seen this peer before
        if let Some(&last_seen_ms) = last_seen_by_peer.get(&peer) {
            if now.saturating_sub(last_seen_ms) > stale_ms {
                let missing_count = missing_polls_by_peer.entry(peer).or_insert(0);
                *missing_count += 1;
                if *missing_count >= threshold {
                    peers_to_remove.push(peer);
                }
            } else {
                missing_polls_by_peer.remove(&peer);
            }
        }
    }

    peers_to_remove
}

// Helper to retry an async operation
async fn retry_operation<F, Fut, T>(
    mut operation: F,
    retries: u32,
    delay_ms: u64,
) -> Result<T, SignalingError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SignalingError>>,
{
    let mut attempts = retries;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts -= 1;
                if attempts == 0 {
                    return Err(e);
                }
                futures_timer::Delay::new(Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

async fn run_firestore_loop(
    client: FirestoreClient,
    room_id: String,
    our_id: PeerId,
    event_tx: UnboundedSender<PeerEvent>,
    mut req_rx: UnboundedReceiver<PeerRequest>,
) {
    const PRESENCE_INTERVAL: Duration = Duration::from_secs(4);
    const ROOM_POLL_INTERVAL: Duration = Duration::from_millis(50);
    const STALE_MS: u128 = 15_000;
    const MISSING_POLLS_BEFORE_LEFT: u32 = 10;

    let our_id_str = peer_id_to_string(our_id);
    let mut known_peers = BTreeSet::new();
    let mut last_seen_by_peer = BTreeMap::new();
    let mut missing_polls_by_peer = BTreeMap::new();
    let mut processed_signals = BTreeSet::new();

    // Initialize room and presence
    let _ = client.ensure_room_exists(&room_id).await;
    let _ = client
        .write_peer_presence(&room_id, &our_id_str, now_ms())
        .await;

    let mut presence_tick = futures_timer::Delay::new(PRESENCE_INTERVAL).fuse();
    let mut room_tick = futures_timer::Delay::new(ROOM_POLL_INTERVAL).fuse();

    loop {
        futures::select! {
            _ = presence_tick => {
                presence_tick = futures_timer::Delay::new(PRESENCE_INTERVAL).fuse();
                if let Err(e) = retry_operation(
                    || client.write_peer_presence(&room_id, &our_id_str, now_ms()),
                    3,
                    100
                ).await {
                    http_client::log_warn(&format!("Failed to update presence: {:?}", e));
                }
            }

            _ = room_tick => {
                room_tick = futures_timer::Delay::new(ROOM_POLL_INTERVAL).fuse();

                match client.read_room(&room_id).await {
                    Ok(Some(doc)) => {
                        let now = now_ms();
                        let fields_ref = doc.fields.as_ref();

                        // Process peers
                        let active = if let Some(peers_map) = fields_ref.and_then(|f| f.peers.as_ref()) {
                            peers_map.map_value.fields.iter()
                                .filter(|(id, _)| *id != &our_id_str)
                                .filter_map(|(peer_id_str, peer_data)| {
                                    let peer_id = peer_id_from_string(peer_id_str)?;
                                    let last_seen = extract_peer_last_seen(peer_data)?;
                                    if last_seen > 0 && now.saturating_sub(last_seen) <= STALE_MS {
                                        last_seen_by_peer.insert(peer_id, last_seen);
                                        missing_polls_by_peer.remove(&peer_id);
                                        Some(peer_id)
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        } else {
                            BTreeSet::new()
                        };

                        // Detect new peers
                        for &peer in &active {
                            if known_peers.insert(peer) && our_id > peer {
                                let _ = event_tx.unbounded_send(PeerEvent::NewPeer(peer));
                            }
                        }

                        // Detect left peers
                        let peers_to_remove = detect_left_peers(
                            &known_peers,
                            &active,
                            &last_seen_by_peer,
                            &mut missing_polls_by_peer,
                            now,
                            STALE_MS,
                            MISSING_POLLS_BEFORE_LEFT,
                        );

                        for peer in peers_to_remove {
                            known_peers.remove(&peer);
                            last_seen_by_peer.remove(&peer);
                            missing_polls_by_peer.remove(&peer);
                            let _ = event_tx.unbounded_send(PeerEvent::PeerLeft(peer));
                        }

                        // Process signals
                        if let Some(signals_map) = fields_ref.and_then(|f| f.signals.as_ref()) {
                            for (msg_id, signal_data) in &signals_map.map_value.fields {
                                if !processed_signals.contains(msg_id) {
                                    if let Some((sender, signal)) = parse_signal(signal_data, &our_id_str) {
                                        processed_signals.insert(msg_id.clone());
                                        #[cfg(not(target_arch = "wasm32"))]
                                        tracing::debug!("📨 Received signal from {}: {:?}", sender, signal);
                                        let _ = event_tx.unbounded_send(PeerEvent::Signal {
                                            sender,
                                            data: signal,
                                        });
                                    }
                                }
                            }
                        }
                    },
                    Ok(None) => {}, // Room doesn't exist yet
                    Err(e) => {
                        http_client::log_warn(&format!("Failed to read room (will retry): {:?}", e));
                    }
                }
            }

            req = req_rx.next() => {
                let Some(req) = req else {
                    http_client::log_error("Request channel closed unexpectedly");
                    break;
                };
                match req {
                    PeerRequest::KeepAlive => {},
                    PeerRequest::Signal { receiver, data } => {
                        let receiver_str = peer_id_to_string(receiver);
                        #[cfg(not(target_arch = "wasm32"))]
                        tracing::debug!("📤 Sending signal to {}: {:?}", receiver, data);

                        if let Err(e) = retry_operation(
                            || client.write_signal(&room_id, &receiver_str, &our_id_str, &data),
                            3,
                            100
                        ).await {
                            http_client::log_warn(&format!("Failed to send signal to {}: {:?}", receiver_str, e));
                        }
                    }
                }
            }
        }
    }
}
