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
// WASM32 Implementation (using web-sys fetch)
// ============================================================================
#[cfg(target_arch = "wasm32")]
mod platform {
    use super::*;
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Headers, Request, RequestInit, RequestMode, Response};

    #[derive(Clone)]
    pub struct FirestoreClient;

    impl fmt::Debug for FirestoreClient {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FirestoreClient")
                .field("project_id", &super::PROJECT_ID)
                .finish()
        }
    }

    impl FirestoreClient {
        pub fn new() -> Self {
            Self
        }

        fn room_doc_url(&self, room_id: &str) -> String {
            format!(
                "https://firestore.googleapis.com/v1/projects/{}/databases/(default)/documents/rooms/{}",
                super::PROJECT_ID,
                room_id
            )
        }

        fn patch_url(&self, room_id: &str, mask: &str) -> String {
            format!(
                "{}?updateMask.fieldPaths={}",
                self.room_doc_url(room_id),
                mask
            )
        }

        async fn http_fetch<B: Serialize>(
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

                // Try to get error details from response body
                let error_text =
                    match JsFuture::from(resp.text().map_err(super::to_user_error)?).await {
                        Ok(js_text) => js_text
                            .as_string()
                            .unwrap_or_else(|| "no error details".to_string()),
                        Err(_) => "failed to read error body".to_string(),
                    };

                // Log to console for debugging
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

        pub async fn ensure_room_exists(&self, room_id: &str) -> Result<(), SignalingError> {
            let url = self.room_doc_url(room_id);
            let body = serde_json::json!({
                "fields": {}
            });
            self.http_fetch("PATCH", &url, Some(&body))
                .await
                .map(|_| ())
        }

        pub async fn write_peer_presence(
            &self,
            room_id: &str,
            peer_id: &str,
            last_seen_ms: u128,
        ) -> Result<(), SignalingError> {
            let url = self.patch_url(room_id, "peers");
            let mut peer_fields = BTreeMap::new();
            peer_fields.insert(
                "last_seen_ms".to_string(),
                super::firestore_fields_int_ms(last_seen_ms),
            );
            let peer_map_value_entry = super::FirestoreMapValueEntry {
                map_value: super::FirestoreMapFields {
                    fields: peer_fields,
                },
            };
            let peer_value =
                serde_json::to_value(peer_map_value_entry).map_err(super::to_user_error)?;

            let mut peers_map = BTreeMap::new();
            peers_map.insert(peer_id.to_string(), peer_value);

            let body = super::FirestorePatchPeers {
                fields: super::FirestorePeersField {
                    peers: super::FirestoreMapValue {
                        map_value: super::FirestoreMapFields { fields: peers_map },
                    },
                },
            };
            self.http_fetch("PATCH", &url, Some(&body))
                .await
                .map(|_| ())
        }

        pub async fn write_signal(
            &self,
            room_id: &str,
            to_peer_id: &str,
            from_peer_id: &str,
            signal: &PeerSignal,
        ) -> Result<(), SignalingError> {
            // Use UUID without hyphens, prefixed with 's' to comply with Firestore property path rules
            // Property paths must start with [a-zA-Z_]
            let msg_id = format!("s{}", uuid::Uuid::new_v4().simple());
            // Use specific path to avoid overwriting other signals
            let url = self.patch_url(room_id, &format!("signals.{}", msg_id));
            let mut signal_fields = BTreeMap::new();
            signal_fields.insert("to".to_string(), super::firestore_fields_string(to_peer_id));
            signal_fields.insert(
                "from".to_string(),
                super::firestore_fields_string(from_peer_id),
            );
            signal_fields.insert(
                "payload".to_string(),
                super::firestore_fields_string(&serde_json::to_string(signal).unwrap_or_default()),
            );
            signal_fields.insert(
                "timestamp".to_string(),
                super::firestore_fields_int_ms(super::now_ms()),
            );
            let signal_map_value_entry = super::FirestoreMapValueEntry {
                map_value: super::FirestoreMapFields {
                    fields: signal_fields,
                },
            };
            let signal_value =
                serde_json::to_value(signal_map_value_entry).map_err(super::to_user_error)?;

            let mut signals_map = BTreeMap::new();
            signals_map.insert(msg_id, signal_value);

            let body = super::FirestorePatchSignals {
                fields: super::FirestoreSignalsField {
                    signals: super::FirestoreMapValue {
                        map_value: super::FirestoreMapFields {
                            fields: signals_map,
                        },
                    },
                },
            };
            self.http_fetch("PATCH", &url, Some(&body))
                .await
                .map(|_| ())
        }

        pub async fn read_room(
            &self,
            room_id: &str,
        ) -> Result<Option<super::FirestoreRoomDoc>, SignalingError> {
            let url = self.room_doc_url(room_id);
            match self.http_fetch::<()>("GET", &url, None).await {
                Ok(json) => {
                    let doc: super::FirestoreRoomDoc =
                        serde_json::from_value(json).map_err(super::to_user_error)?;
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

    pub fn log_error(msg: &str) {
        web_sys::console::error_1(&msg.into());
    }

    pub fn log_warn(msg: &str) {
        web_sys::console::warn_1(&msg.into());
    }
}

// ============================================================================
// Non-WASM32 Implementation (using reqwest)
// ============================================================================
#[cfg(not(target_arch = "wasm32"))]
mod platform {
    use super::*;
    use tracing::{error, warn};

    #[derive(Clone)]
    pub struct FirestoreClient {
        client: reqwest::Client,
    }

    impl fmt::Debug for FirestoreClient {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FirestoreClient")
                .field("project_id", &super::PROJECT_ID)
                .finish()
        }
    }

    impl FirestoreClient {
        pub fn new() -> Self {
            Self {
                client: reqwest::Client::new(),
            }
        }

        fn room_doc_url(&self, room_id: &str) -> String {
            format!(
                "https://firestore.googleapis.com/v1/projects/{}/databases/(default)/documents/rooms/{}",
                super::PROJECT_ID,
                room_id
            )
        }

        fn patch_url(&self, room_id: &str, mask: &str) -> String {
            format!(
                "{}?updateMask.fieldPaths={}",
                self.room_doc_url(room_id),
                mask
            )
        }

        async fn http_fetch<B: Serialize>(
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

        pub async fn ensure_room_exists(&self, room_id: &str) -> Result<(), SignalingError> {
            let url = self.room_doc_url(room_id);
            let body = serde_json::json!({
                "fields": {}
            });
            self.http_fetch("PATCH", &url, Some(&body))
                .await
                .map(|_| ())
        }

        pub async fn write_peer_presence(
            &self,
            room_id: &str,
            peer_id: &str,
            last_seen_ms: u128,
        ) -> Result<(), SignalingError> {
            let url = self.patch_url(room_id, "peers");
            let mut peer_fields = BTreeMap::new();
            peer_fields.insert(
                "last_seen_ms".to_string(),
                super::firestore_fields_int_ms(last_seen_ms),
            );
            let peer_map_value_entry = super::FirestoreMapValueEntry {
                map_value: super::FirestoreMapFields {
                    fields: peer_fields,
                },
            };
            let peer_value =
                serde_json::to_value(peer_map_value_entry).map_err(super::to_user_error)?;

            let mut peers_map = BTreeMap::new();
            peers_map.insert(peer_id.to_string(), peer_value);

            let body = super::FirestorePatchPeers {
                fields: super::FirestorePeersField {
                    peers: super::FirestoreMapValue {
                        map_value: super::FirestoreMapFields { fields: peers_map },
                    },
                },
            };
            self.http_fetch("PATCH", &url, Some(&body))
                .await
                .map(|_| ())
        }

        pub async fn write_signal(
            &self,
            room_id: &str,
            to_peer_id: &str,
            from_peer_id: &str,
            signal: &PeerSignal,
        ) -> Result<(), SignalingError> {
            // Use UUID without hyphens, prefixed with 's' to comply with Firestore property path rules
            // Property paths must start with [a-zA-Z_]
            let msg_id = format!("s{}", uuid::Uuid::new_v4().simple());
            // Use specific path to avoid overwriting other signals
            let url = self.patch_url(room_id, &format!("signals.{}", msg_id));
            let mut signal_fields = BTreeMap::new();
            signal_fields.insert("to".to_string(), super::firestore_fields_string(to_peer_id));
            signal_fields.insert(
                "from".to_string(),
                super::firestore_fields_string(from_peer_id),
            );
            signal_fields.insert(
                "payload".to_string(),
                super::firestore_fields_string(&serde_json::to_string(signal).unwrap_or_default()),
            );
            signal_fields.insert(
                "timestamp".to_string(),
                super::firestore_fields_int_ms(super::now_ms()),
            );
            let signal_map_value_entry = super::FirestoreMapValueEntry {
                map_value: super::FirestoreMapFields {
                    fields: signal_fields,
                },
            };
            let signal_value =
                serde_json::to_value(signal_map_value_entry).map_err(super::to_user_error)?;

            let mut signals_map = BTreeMap::new();
            signals_map.insert(msg_id, signal_value);

            let body = super::FirestorePatchSignals {
                fields: super::FirestoreSignalsField {
                    signals: super::FirestoreMapValue {
                        map_value: super::FirestoreMapFields {
                            fields: signals_map,
                        },
                    },
                },
            };
            self.http_fetch("PATCH", &url, Some(&body))
                .await
                .map(|_| ())
        }

        pub async fn read_room(
            &self,
            room_id: &str,
        ) -> Result<Option<super::FirestoreRoomDoc>, SignalingError> {
            let url = self.room_doc_url(room_id);
            match self.http_fetch::<()>("GET", &url, None).await {
                Ok(json) => {
                    let doc: super::FirestoreRoomDoc =
                        serde_json::from_value(json).map_err(super::to_user_error)?;
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

    pub fn log_error(msg: &str) {
        error!("{}", msg);
    }

    pub fn log_warn(msg: &str) {
        warn!("{}", msg);
    }
}

use platform::FirestoreClient;

// Firestore data structures matching the working implementation pattern
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

// Struct for a Firestore Value that contains a mapValue (for nested maps)
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
    let uuid = uuid::Uuid::parse_str(s).ok()?;
    Some(PeerId(uuid))
}

fn firestore_fields_string(value: &str) -> serde_json::Value {
    serde_json::json!({ "stringValue": value })
}

fn firestore_fields_int_ms(value: u128) -> serde_json::Value {
    serde_json::json!({ "integerValue": value.to_string() })
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

async fn run_firestore_loop(
    client: FirestoreClient,
    room_id: String,
    our_id: PeerId,
    event_tx: UnboundedSender<PeerEvent>,
    mut req_rx: UnboundedReceiver<PeerRequest>,
) {
    const PRESENCE_INTERVAL: Duration = Duration::from_secs(4);
    // Poll more frequently to reduce signal delivery latency
    // Critical for ICE candidate exchange during handshake
    const ROOM_POLL_INTERVAL: Duration = Duration::from_millis(50);
    const STALE_MS: u128 = 15_000;
    // Require peer to be missing for multiple consecutive polls before marking as left
    // This prevents false positives from temporary Firestore read issues
    // Increased to account for presence update interval (4s) and potential delays
    const MISSING_POLLS_BEFORE_LEFT: u32 = 10; // 5 seconds at 500ms polling

    let our_id_str = peer_id_to_string(our_id);
    let mut known_peers: BTreeSet<PeerId> = BTreeSet::new();
    let mut last_seen_by_peer: BTreeMap<PeerId, u128> = BTreeMap::new();
    let mut missing_polls_by_peer: BTreeMap<PeerId, u32> = BTreeMap::new();
    let mut processed_signals: BTreeSet<String> = BTreeSet::new();

    // Ensure room exists
    let _ = client.ensure_room_exists(&room_id).await;

    // Write initial presence
    let _ = client
        .write_peer_presence(&room_id, &our_id_str, now_ms())
        .await;

    let mut presence_tick = futures_timer::Delay::new(PRESENCE_INTERVAL).fuse();
    let mut room_tick = futures_timer::Delay::new(ROOM_POLL_INTERVAL).fuse();

    loop {
        futures::select! {
            _ = presence_tick => {
                presence_tick = futures_timer::Delay::new(PRESENCE_INTERVAL).fuse();
                // Retry presence updates - critical for staying connected
                let mut retries = 3;
                while retries > 0 {
                    match client.write_peer_presence(&room_id, &our_id_str, now_ms()).await {
                        Ok(_) => break,
                        Err(e) => {
                            retries -= 1;
                            if retries == 0 {
                                platform::log_warn(&format!("Failed to update presence after retries: {:?}", e));
                            } else {
                                // Brief delay before retry
                                futures_timer::Delay::new(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            }

            _ = room_tick => {
                room_tick = futures_timer::Delay::new(ROOM_POLL_INTERVAL).fuse();

                match client.read_room(&room_id).await {
                    Ok(Some(doc)) => {
                    let now = now_ms();

                    // Extract fields once to avoid borrow issues
                    let fields_ref = doc.fields.as_ref();

                    // Process peers
                    if let Some(peers_map) = fields_ref.and_then(|f| f.peers.as_ref()) {
                        let mut active: BTreeSet<PeerId> = BTreeSet::new();
                        for (peer_id_str, peer_data) in peers_map.map_value.fields.iter() {
                            if peer_id_str == &our_id_str {
                                continue;
                            }
                            let Some(peer_id) = peer_id_from_string(peer_id_str) else { continue; };

                            // Extract last_seen_ms from Firestore format
                            // peer_data is a mapValue, so we need to get the fields first
                            let last_seen_ms = peer_data
                                .get("mapValue")
                                .and_then(|mv| mv.get("fields"))
                                .and_then(|fields| fields.get("last_seen_ms"))
                                .and_then(|v| v.get("integerValue"))
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<u128>().ok());

                            // Only consider peer active if:
                            // 1. We successfully parsed last_seen_ms (not None/0)
                            // 2. The timestamp is recent enough
                            if let Some(last_seen) = last_seen_ms {
                                // Skip if last_seen is 0 (parsing failure fallback)
                                if last_seen > 0 && now.saturating_sub(last_seen) <= STALE_MS {
                                    active.insert(peer_id);
                                    last_seen_by_peer.insert(peer_id, last_seen);
                                    // Reset missing count when we see the peer
                                    missing_polls_by_peer.remove(&peer_id);
                                }
                            }
                        }

                        // Detect new peers
                        for peer in active.iter() {
                            if !known_peers.contains(peer) {
                                known_peers.insert(*peer);
                                // Offerer selection: only one side receives NewPeer
                                if our_id > *peer {
                                    let _ = event_tx.unbounded_send(PeerEvent::NewPeer(*peer));
                                }
                            }
                        }

                        // Detect left peers - use hysteresis to prevent false positives
                        // Only check for missing peers that we've seen at least once in active set
                        // This prevents false positives for newly discovered peers whose presence
                        // hasn't propagated to Firestore yet
                        let mut peers_to_remove = Vec::new();
                        for peer in known_peers.iter().copied() {
                            if !active.contains(&peer) {
                                // Only start tracking missing polls if we've seen this peer before
                                // (i.e., they have a last_seen entry, meaning they were once active)
                                if let Some(&last_seen_ms) = last_seen_by_peer.get(&peer) {
                                    // Don't count as missing if their last_seen timestamp is still recent
                                    // This accounts for temporary Firestore read issues or delayed presence updates
                                    if now.saturating_sub(last_seen_ms) > STALE_MS {
                                        // Their timestamp is stale, so they might actually be gone
                                        // Increment missing count
                                        let missing_count = missing_polls_by_peer.entry(peer).or_insert(0);
                                        *missing_count += 1;

                                        // Only mark as left after missing for multiple consecutive polls
                                        if *missing_count >= MISSING_POLLS_BEFORE_LEFT {
                                            peers_to_remove.push(peer);
                                        }
                                    } else {
                                        // Their timestamp is still fresh, so they're probably still there
                                        // Just a temporary read issue - reset missing count
                                        missing_polls_by_peer.remove(&peer);
                                    }
                                }
                                // If peer has never been seen in active set, don't count them as missing yet
                                // They might just be newly discovered and their presence hasn't propagated
                            } else {
                                // Peer is active - reset their missing count if it exists
                                missing_polls_by_peer.remove(&peer);
                            }
                        }
                        // Remove peers that have been missing for too long
                        for peer in peers_to_remove {
                            known_peers.remove(&peer);
                            last_seen_by_peer.remove(&peer);
                            missing_polls_by_peer.remove(&peer);
                            let _ = event_tx.unbounded_send(PeerEvent::PeerLeft(peer));
                        }
                    } else {
                        // No peers map in document - increment missing count for peers we've seen before
                        // Only count missing polls for peers that have been seen at least once
                        let mut peers_to_remove = Vec::new();
                        for peer in known_peers.iter().copied() {
                            // Only track missing polls if we've seen this peer before
                            if let Some(&last_seen_ms) = last_seen_by_peer.get(&peer) {
                                // Don't count as missing if their last_seen timestamp is still recent
                                if now.saturating_sub(last_seen_ms) > STALE_MS {
                                    // Their timestamp is stale, so they might actually be gone
                                    let missing_count = missing_polls_by_peer.entry(peer).or_insert(0);
                                    *missing_count += 1;

                                    if *missing_count >= MISSING_POLLS_BEFORE_LEFT {
                                        peers_to_remove.push(peer);
                                    }
                                } else {
                                    // Their timestamp is still fresh - reset missing count
                                    missing_polls_by_peer.remove(&peer);
                                }
                            }
                        }
                        // Remove peers that have been missing for too long
                        for peer in peers_to_remove {
                            known_peers.remove(&peer);
                            last_seen_by_peer.remove(&peer);
                            missing_polls_by_peer.remove(&peer);
                            let _ = event_tx.unbounded_send(PeerEvent::PeerLeft(peer));
                        }
                    }

                    // Process signals
                    if let Some(fields) = fields_ref {
                        if let Some(signals_map) = &fields.signals {
                        for (msg_id, signal_data) in signals_map.map_value.fields.iter() {
                            if processed_signals.contains(msg_id) {
                                continue;
                            }

                            // signal_data is a mapValue, so we need to get the fields first
                            let signal_fields = signal_data
                                .get("mapValue")
                                .and_then(|mv| mv.get("fields"));

                            let to = signal_fields
                                .and_then(|fields| fields.get("to"))
                                .and_then(|v| v.get("stringValue"))
                                .and_then(|v| v.as_str());
                            let from = signal_fields
                                .and_then(|fields| fields.get("from"))
                                .and_then(|v| v.get("stringValue"))
                                .and_then(|v| v.as_str());
                            let payload = signal_fields
                                .and_then(|fields| fields.get("payload"))
                                .and_then(|v| v.get("stringValue"))
                                .and_then(|v| v.as_str());

                            if let (Some(to_str), Some(from_str), Some(payload_str)) = (to, from, payload) {
                                if to_str == our_id_str {
                                    if let Some(sender) = peer_id_from_string(from_str) {
                                        if let Ok(signal) = serde_json::from_str::<PeerSignal>(payload_str) {
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
                        }
                        }
                    }
                    },
                    Ok(None) => {
                        // Room doesn't exist yet, that's ok
                    }
                    Err(e) => {
                        // Log error but continue - network issues shouldn't kill the signaller
                        platform::log_warn(&format!("Failed to read room (will retry): {:?}", e));
                    }
                }
            }

            req = req_rx.next() => {
                let Some(req) = req else {
                    // Channel closed - this shouldn't happen during normal operation
                    // but if it does, we should log and continue rather than breaking
                    platform::log_error("Request channel closed unexpectedly");
                    break;
                };
                match req {
                    PeerRequest::KeepAlive => {
                        // Presence is maintained separately
                    }
                    PeerRequest::Signal { receiver, data } => {
                        let receiver_str = peer_id_to_string(receiver);
                        #[cfg(not(target_arch = "wasm32"))]
                        tracing::debug!("📤 Sending signal to {}: {:?}", receiver, data);
                        // Retry signal writes - important for handshake
                        let mut retries = 3;
                        while retries > 0 {
                            match client.write_signal(&room_id, &receiver_str, &our_id_str, &data).await {
                                Ok(_) => break,
                                Err(e) => {
                                    retries -= 1;
                                    if retries == 0 {
                                        platform::log_warn(&format!("Failed to send signal to {} after retries: {:?}", receiver_str, e));
                                    } else {
                                        // Brief delay before retry
                                        futures_timer::Delay::new(Duration::from_millis(100)).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
