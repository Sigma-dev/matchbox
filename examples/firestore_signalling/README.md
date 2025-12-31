# Firestore signalling

This example shows how to use `matchbox_socket` **without running `matchbox_server`**, by plugging in a custom `Signaller` that uses a **public Firestore database** (Firestore REST API) to coordinate rooms and exchange WebRTC signals.

This example works on both **native (desktop/server)** and **WASM (browser)** targets.

## What you need

- A Firebase project named `p2p-relay` with Firestore enabled
- (Demo) Firestore rules that allow public read/write to the example's collections

## Run natively

### Host Mode (Create Lobby)

Run without parameters to create a new lobby with a 4-letter room code:

```sh
cargo run -p firestore_signalling
```

The host will display a 4-letter room code (e.g., `ABCD`). Share this code with clients.

### Client Mode (Join Lobby)

Run with the room code to join an existing lobby:

```sh
cargo run -p firestore_signalling -- ABCD
```

Replace `ABCD` with the actual room code from the host.

## Run on WASM

### Prerequisites

Install the `wasm32-unknown-unknown` target:

```sh
rustup target install wasm32-unknown-unknown
```

Install a lightweight web server:

```sh
cargo install wasm-server-runner
```

### Serve

```sh
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' cargo run -p firestore_signalling --target wasm32-unknown-unknown
```

On Windows PowerShell:

```powershell
$env:RUSTFLAGS='--cfg getrandom_backend="wasm_js"'
cargo run -p firestore_signalling --target wasm32-unknown-unknown
```

### Run

- Open `http://127.0.0.1:1334/`
- The page will auto-generate a room id in the URL hash (`#...`). Open a second tab with the same URL to connect.
- Open the browser console to see logs.

## Firestore data model

- Presence: `rooms/{room}/peers/{peerId}` with `last_seen_ms`
- Inbox: `rooms/{room}/inbox/{peerId}/messages/{msgId}` containing serialized `matchbox_protocol::PeerEvent<PeerSignal>` payloads

## Demo Firestore rules (unsafe, example-only)

Use **only** for a throwaway demo project:

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /rooms/{room}/{document=**} {
      allow read, write: if true;
    }
  }
}
```


