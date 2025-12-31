#![cfg(target_arch = "wasm32")]

use web_sys::Url;

pub fn get_query_param(key: &str) -> Option<String> {
    let window = web_sys::window()?;
    let href = window.location().href().ok()?;
    let url = Url::new(&href).ok()?;
    url.search_params().get(key)
}

pub fn get_or_set_room_hash() -> String {
    let window = web_sys::window().expect("no global `window` exists");
    let location = window.location();
    let hash = location.hash().ok().unwrap_or_default();
    let mut room = hash.trim_start_matches('#').to_string();
    if room.is_empty() {
        room = format!("room-{}", uuid::Uuid::new_v4());
        let _ = location.set_hash(&room);
    }
    room
}








