use std::{collections::BTreeMap, fmt};

use matchbox_protocol::PeerId;
use matchbox_socket::{PeerSignal, SignalingError};
use serde::{Deserialize, Serialize};
use web_time::SystemTime;

use crate::http_client::HttpClient;

const PROJECT_ID: &str = "p2p-relay";

// ============================================================================
// Shared Firestore Client (platform-agnostic business logic)
// ============================================================================

#[derive(Clone)]
pub struct FirestoreClient {
    http: HttpClient,
}

impl fmt::Debug for FirestoreClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FirestoreClient")
            .field("project_id", &PROJECT_ID)
            .finish()
    }
}

impl FirestoreClient {
    pub fn new() -> Self {
        Self {
            http: HttpClient::new(),
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

    pub async fn ensure_room_exists(&self, room_id: &str) -> Result<(), SignalingError> {
        let url = Self::room_doc_url(room_id);
        let body = serde_json::json!({ "fields": {} });
        self.http
            .fetch("PATCH", &url, Some(&body))
            .await
            .map(|_| ())
    }

    pub async fn write_peer_presence(
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

    pub async fn write_signal(
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

    pub async fn read_room(
        &self,
        room_id: &str,
    ) -> Result<Option<FirestoreRoomDoc>, SignalingError> {
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
pub struct FirestoreRoomDoc {
    #[serde(default)]
    pub fields: Option<FirestoreRoomFields>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FirestoreRoomFields {
    #[serde(default)]
    pub peers: Option<FirestoreMapValue>,
    #[serde(default)]
    pub signals: Option<FirestoreMapValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FirestoreMapValue {
    #[serde(rename = "mapValue")]
    pub map_value: FirestoreMapFields,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FirestoreMapFields {
    pub fields: BTreeMap<String, serde_json::Value>,
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

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn to_user_error<E: fmt::Debug>(e: E) -> SignalingError {
    SignalingError::UserImplementationError(format!("{e:#?}"))
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
pub fn extract_peer_last_seen(peer_data: &serde_json::Value) -> Option<u128> {
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
pub fn parse_signal(
    signal_data: &serde_json::Value,
    our_id_str: &str,
) -> Option<(PeerId, PeerSignal)> {
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

pub fn peer_id_to_string(peer_id: PeerId) -> String {
    peer_id.0.to_string()
}

pub fn peer_id_from_string(s: &str) -> Option<PeerId> {
    uuid::Uuid::parse_str(s).ok().map(PeerId)
}
