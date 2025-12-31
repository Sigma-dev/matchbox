use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};

use futures::{
    FutureExt, StreamExt,
    channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded},
};
use matchbox_protocol::PeerId;
use matchbox_socket::{
    PeerEvent, PeerRequest, PeerSignal, SignalingError, Signaller, SignallerBuilder,
    async_trait::async_trait,
};
use serde::{Deserialize, Serialize};
use web_time::SystemTime;

const PROJECT_ID: &str = "p2p-relay";

#[derive(Clone, Debug)]
pub struct FirestoreConfig {
    pub room_id: String,
}

// ============================================================================
// Platform-specific HTTP implementations
// ============================================================================
#[cfg(target_arch = "wasm32")]
mod platform {
    use super::*;
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Headers, Request, RequestInit, RequestMode, Response};

    #[derive(Clone)]
    pub struct HttpClient;

    impl HttpClient {
        pub fn new() -> Self {
            Self
        }

        pub async fn fetch<B: Serialize>(
            &self,
            method: &str,
            url: &str,
            body: Option<&B>,
        ) -> Result<serde_json::Value, SignalingError> {
            let window = web_sys::window().ok_or_else(|| {
                SignalingError::UserImplementationError("no global window".to_string())
            })?;

            let init = RequestInit::new();
            init.set_method(method);
            init.set_mode(RequestMode::Cors);

            if let Some(b) = body {
                let headers = Headers::new().map_err(super::to_user_error)?;
                headers
                    .set("Content-Type", "application/json")
                    .map_err(super::to_user_error)?;
                init.set_headers(&headers);
                let body_str = serde_json::to_string(b).map_err(super::to_user_error)?;
                init.set_body(&JsValue::from_str(&body_str));
            }

            let request =
                Request::new_with_str_and_init(url, &init).map_err(super::to_user_error)?;
            let resp_value = JsFuture::from(window.fetch_with_request(&request))
                .await
                .map_err(super::to_user_error)?;
            let resp: Response = resp_value.dyn_into().map_err(super::to_user_error)?;

            if !resp.ok() {
                let status = resp.status();
                let status_text = resp.status_text();
                let error_text =
                    match JsFuture::from(resp.text().map_err(super::to_user_error)?).await {
                        Ok(js_text) => js_text
                            .as_string()
                            .unwrap_or_else(|| "no error details".to_string()),
                        Err(_) => "failed to read error body".to_string(),
                    };

                web_sys::console::error_1(
                    &format!("Firestore error {} {}: {}", status, status_text, error_text).into(),
                );

                return Err(SignalingError::UserImplementationError(format!(
                    "HTTP {} {}: {}",
                    status, status_text, error_text
                )));
            }

            let json = JsFuture::from(resp.json().map_err(super::to_user_error)?)
                .await
                .map_err(super::to_user_error)?;
            let value: serde_json::Value =
                serde_wasm_bindgen::from_value(json).map_err(super::to_user_error)?;
            Ok(value)
        }
    }

    pub fn log_error(msg: &str) {
        web_sys::console::error_1(&msg.into());
    }

    pub fn log_warn(msg: &str) {
        web_sys::console::warn_1(&msg.into());
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod platform {
    use super::*;
    use tracing::{error, warn};

    #[derive(Clone)]
    pub struct HttpClient {
        client: reqwest::Client,
    }

    impl HttpClient {
        pub fn new() -> Self {
            Self {
                client: reqwest::Client::new(),
            }
        }

        pub async fn fetch<B: Serialize>(
            &self,
            method: &str,
            url: &str,
            body: Option<&B>,
        ) -> Result<serde_json::Value, SignalingError> {
            let mut request = match method {
                "GET" => self.client.get(url),
                "POST" => self.client.post(url),
                "PATCH" => self.client.patch(url),
                "PUT" => self.client.put(url),
                "DELETE" => self.client.delete(url),
                _ => {
                    return Err(SignalingError::UserImplementationError(format!(
                        "Unsupported HTTP method: {}",
                        method
                    )));
                }
            };

            if let Some(b) = body {
                request = request.json(b);
            }

            let response = request.send().await.map_err(super::to_user_error)?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "no error details".to_string());

                error!("Firestore error {}: {}", status, error_text);

                return Err(SignalingError::UserImplementationError(format!(
                    "HTTP {}: {}",
                    status, error_text
                )));
            }

            let value = response.json().await.map_err(super::to_user_error)?;
            Ok(value)
        }
    }

    pub fn log_error(msg: &str) {
        error!("{}", msg);
    }

    pub fn log_warn(msg: &str) {
        warn!("{}", msg);
    }
}

// ============================================================================
// Shared Firestore Client (platform-agnostic business logic)
// ============================================================================
#[derive(Clone)]
struct FirestoreClient {
    http: platform::HttpClient,
}

impl fmt::Debug for FirestoreClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FirestoreClient")
            .field("project_id", &PROJECT_ID)
            .finish()
    }
}

impl FirestoreClient {
    fn new() -> Self {
        Self {
            http: platform::HttpClient::new(),
        }
    }

    fn room_doc_url(room_id: &str) -> String {
        format!(
            "https://firestore.googleapis.com/v1/projects/{}/databases/(default)/documents/rooms/{}",
            PROJECT_ID, room_id
        )
    }

    fn patch_url(room_id: &str, mask: &str) -> String {
        format!(
            "{}?updateMask.fieldPaths={}",
            Self::room_doc_url(room_id),
            mask
        )
    }

    async fn ensure_room_exists(&self, room_id: &str) -> Result<(), SignalingError> {
        let url = Self::room_doc_url(room_id);
        let body = serde_json::json!({ "fields": {} });
        self.http
            .fetch("PATCH", &url, Some(&body))
            .await
            .map(|_| ())
    }

    async fn write_peer_presence(
        &self,
        room_id: &str,
        peer_id: &str,
        last_seen_ms: u128,
    ) -> Result<(), SignalingError> {
        let url = Self::patch_url(room_id, "peers");
        let body = build_peer_presence_body(peer_id, last_seen_ms)?;
        self.http
            .fetch("PATCH", &url, Some(&body))
            .await
            .map(|_| ())
    }

    async fn write_signal(
        &self,
        room_id: &str,
        to_peer_id: &str,
        from_peer_id: &str,
        signal: &PeerSignal,
    ) -> Result<(), SignalingError> {
        let msg_id = format!("s{}", uuid::Uuid::new_v4().simple());
        let url = Self::patch_url(room_id, &format!("signals.{}", msg_id));
        let body = build_signal_body(&msg_id, to_peer_id, from_peer_id, signal)?;
        self.http
            .fetch("PATCH", &url, Some(&body))
            .await
            .map(|_| ())
    }

    async fn read_room(&self, room_id: &str) -> Result<Option<FirestoreRoomDoc>, SignalingError> {
        let url = Self::room_doc_url(room_id);
        match self.http.fetch::<()>("GET", &url, None).await {
            Ok(json) => {
                let doc: FirestoreRoomDoc = serde_json::from_value(json).map_err(to_user_error)?;
                Ok(Some(doc))
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                if error_msg.contains("404") || error_msg.contains("not found") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }
}

// ============================================================================
// Firestore data structures
// ============================================================================
#[derive(Debug, Serialize, Deserialize)]
struct FirestoreRoomDoc {
    #[serde(default)]
    fields: Option<FirestoreRoomFields>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct FirestoreRoomFields {
    #[serde(default)]
    peers: Option<FirestoreMapValue>,
    #[serde(default)]
    signals: Option<FirestoreMapValue>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FirestoreMapValue {
    #[serde(rename = "mapValue")]
    map_value: FirestoreMapFields,
}

#[derive(Debug, Serialize, Deserialize)]
struct FirestoreMapFields {
    fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct FirestoreMapValueEntry {
    #[serde(rename = "mapValue")]
    map_value: FirestoreMapFields,
}

#[derive(Debug, Serialize)]
struct FirestorePatchPeers {
    fields: FirestorePeersField,
}

#[derive(Debug, Serialize)]
struct FirestorePeersField {
    peers: FirestoreMapValue,
}

#[derive(Debug, Serialize)]
struct FirestorePatchSignals {
    fields: FirestoreSignalsField,
}

#[derive(Debug, Serialize)]
struct FirestoreSignalsField {
    signals: FirestoreMapValue,
}

// ============================================================================
// Helper functions
// ============================================================================
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn to_user_error<E: fmt::Debug>(e: E) -> SignalingError {
    SignalingError::UserImplementationError(format!("{e:#?}"))
}

fn peer_id_to_string(peer_id: PeerId) -> String {
    peer_id.0.to_string()
}

fn peer_id_from_string(s: &str) -> Option<PeerId> {
    uuid::Uuid::parse_str(s).ok().map(PeerId)
}

fn firestore_fields_string(value: &str) -> serde_json::Value {
    serde_json::json!({ "stringValue": value })
}

fn firestore_fields_int_ms(value: u128) -> serde_json::Value {
    serde_json::json!({ "integerValue": value.to_string() })
}

// Build Firestore request body for peer presence update
fn build_peer_presence_body(
    peer_id: &str,
    last_seen_ms: u128,
) -> Result<FirestorePatchPeers, SignalingError> {
    let mut peer_fields = BTreeMap::new();
    peer_fields.insert(
        "last_seen_ms".to_string(),
        firestore_fields_int_ms(last_seen_ms),
    );

    let peer_map_value_entry = FirestoreMapValueEntry {
        map_value: FirestoreMapFields {
            fields: peer_fields,
        },
    };
    let peer_value = serde_json::to_value(peer_map_value_entry).map_err(to_user_error)?;

    let mut peers_map = BTreeMap::new();
    peers_map.insert(peer_id.to_string(), peer_value);

    Ok(FirestorePatchPeers {
        fields: FirestorePeersField {
            peers: FirestoreMapValue {
                map_value: FirestoreMapFields { fields: peers_map },
            },
        },
    })
}

// Build Firestore request body for signal
fn build_signal_body(
    msg_id: &str,
    to_peer_id: &str,
    from_peer_id: &str,
    signal: &PeerSignal,
) -> Result<FirestorePatchSignals, SignalingError> {
    let mut signal_fields = BTreeMap::new();
    signal_fields.insert("to".to_string(), firestore_fields_string(to_peer_id));
    signal_fields.insert("from".to_string(), firestore_fields_string(from_peer_id));
    signal_fields.insert(
        "payload".to_string(),
        firestore_fields_string(&serde_json::to_string(signal).unwrap_or_default()),
    );
    signal_fields.insert("timestamp".to_string(), firestore_fields_int_ms(now_ms()));

    let signal_map_value_entry = FirestoreMapValueEntry {
        map_value: FirestoreMapFields {
            fields: signal_fields,
        },
    };
    let signal_value = serde_json::to_value(signal_map_value_entry).map_err(to_user_error)?;

    let mut signals_map = BTreeMap::new();
    signals_map.insert(msg_id.to_string(), signal_value);

    Ok(FirestorePatchSignals {
        fields: FirestoreSignalsField {
            signals: FirestoreMapValue {
                map_value: FirestoreMapFields {
                    fields: signals_map,
                },
            },
        },
    })
}

// Extract peer data from Firestore format
fn extract_peer_last_seen(peer_data: &serde_json::Value) -> Option<u128> {
    peer_data
        .get("mapValue")?
        .get("fields")?
        .get("last_seen_ms")?
        .get("integerValue")?
        .as_str()?
        .parse()
        .ok()
}

// Parse signal from Firestore format
fn parse_signal(signal_data: &serde_json::Value, our_id_str: &str) -> Option<(PeerId, PeerSignal)> {
    let signal_fields = signal_data.get("mapValue")?.get("fields")?;

    let to = signal_fields.get("to")?.get("stringValue")?.as_str()?;
    let from = signal_fields.get("from")?.get("stringValue")?.as_str()?;
    let payload = signal_fields.get("payload")?.get("stringValue")?.as_str()?;

    if to != our_id_str {
        return None;
    }

    let sender = peer_id_from_string(from)?;
    let signal: PeerSignal = serde_json::from_str(payload).ok()?;

    Some((sender, signal))
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
            .map_err(to_user_error)?;

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
        self.req_tx.unbounded_send(request).map_err(to_user_error)?;
        Ok(())
    }

    async fn next_message(&mut self) -> Result<PeerEvent, SignalingError> {
        self.event_rx
            .next()
            .await
            .ok_or(SignalingError::StreamExhausted)
    }
}

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
                    platform::log_warn(&format!("Failed to update presence: {:?}", e));
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
                        platform::log_warn(&format!("Failed to read room (will retry): {:?}", e));
                    }
                }
            }

            req = req_rx.next() => {
                let Some(req) = req else {
                    platform::log_error("Request channel closed unexpectedly");
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
                            platform::log_warn(&format!("Failed to send signal to {}: {:?}", receiver_str, e));
                        }
                    }
                }
            }
        }
    }
}
