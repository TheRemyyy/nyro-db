use crate::database::NyroDB;
use crate::utils::logger::Logger;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::broadcast;
use warp::ws::{Message, WebSocket};

pub struct RealtimeServer;

impl RealtimeServer {
    pub async fn handle_client(ws: WebSocket, _db: Arc<NyroDB>, tx: broadcast::Sender<String>) {
        let (mut user_ws_tx, mut user_ws_rx) = ws.split();
        let mut rx = tx.subscribe();

        // Spawn a task to listen for broadcast messages and send to this client
        let send_task = tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                if user_ws_tx.send(Message::text(msg)).await.is_err() {
                    break;
                }
            }
        });

        // Listen for messages from client (e.g. subscribe to specific models)
        while let Some(result) = user_ws_rx.next().await {
            match result {
                Ok(_) => {
                    // We could handle subscription logic here
                }
                Err(e) => {
                    Logger::error(&format!("WebSocket error: {}", e));
                    break;
                }
            }
        }

        send_task.abort();
    }
}
