use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedSender},
};

const TCP_SERVER_ADDRESS: &str = "0.0.0.0:4000";
const DEFAULT_HTTP_PORT: u16 = 10000;

#[derive(Serialize, Deserialize, Clone, Debug)]
enum ClientMessage {
    Join { username: String },
    State {
        x: f32,
        y: f32,
        z: f32,
        rotation_y: f32,
        grounded: bool,
    },
    Chat { message: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum ServerMessage {
    Welcome { id: u64 },
    Snapshot { players: Vec<NetPlayer> },
    Chat { username: String, message: String },
    ServerInfo { message: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NetPlayer {
    id: u64,
    username: String,
    x: f32,
    y: f32,
    z: f32,
    rotation_y: f32,
    grounded: bool,
}

struct ClientState {
    player: NetPlayer,
    sender: UnboundedSender<ServerMessage>,
}

type Clients = Arc<Mutex<HashMap<u64, ClientState>>>;

#[derive(Clone)]
struct ServerState {
    clients: Clients,
    next_id: Arc<AtomicU64>,
}

impl ServerState {
    fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn allocate_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[tokio::main]
async fn main() {
    println!("=================================");
    println!("       VEYRON MULTIPLAYER");
    println!("=================================");

    let state = ServerState::new();

    // Keep the existing TCP endpoint for the current Windows client.
    // Render exposes the HTTP/WebSocket port publicly; the extra TCP port
    // is useful for local testing and does not need to be publicly exposed.
    let tcp_state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = run_tcp_server(tcp_state).await {
            eprintln!("TCP server stopped: {error}");
        }
    });

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_HTTP_PORT);
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("Could not bind WebSocket/HTTP server on {address}: {error}");
            return;
        }
    };

    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(health))
        .route("/ws", get(websocket_route))
        .with_state(state);

    println!("WebSocket server listening on {address}");
    println!("WebSocket route: /ws");
    println!("Health route: /healthz");
    if let Err(error) = axum::serve(listener, app).await {
        eprintln!("WebSocket/HTTP server stopped: {error}");
    }
}

async fn root() -> &'static str {
    "Veyron multiplayer server is online. Connect using the /ws WebSocket route."
}

async fn health() -> &'static str {
    "ok"
}

async fn websocket_route(
    ws: WebSocketUpgrade,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

async fn handle_websocket(socket: WebSocket, state: ServerState) {
    let (mut socket_sender, mut socket_receiver) = socket.split();
    let (server_sender, mut server_receiver) = mpsc::unbounded_channel::<ServerMessage>();

    let writer_task = tokio::spawn(async move {
        while let Some(message) = server_receiver.recv().await {
            match serde_json::to_string(&message) {
                Ok(json) => {
                    if socket_sender.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                Err(error) => eprintln!("Could not serialize WebSocket message: {error}"),
            }
        }
    });

    let id = state.allocate_id();
    let _ = server_sender.send(ServerMessage::Welcome { id });
    let _ = server_sender.send(ServerMessage::ServerInfo {
        message: "Connected to Veyron multiplayer over WebSocket!".to_string(),
    });

    let mut username = format!("Player{id}");
    let mut joined = false;

    while let Some(result) = socket_receiver.next().await {
        let message = match result {
            Ok(Message::Text(text)) => match serde_json::from_str::<ClientMessage>(text.as_str()) {
                Ok(message) => message,
                Err(error) => {
                    eprintln!("Invalid WebSocket packet: {error}");
                    continue;
                }
            },
            Ok(Message::Close(_)) | Err(_) => break,
            // Browser clients do not need to send binary data. Ping frames
            // are handled by the WebSocket stack; ignore other frame types.
            Ok(_) => continue,
        };

        process_client_message(
            &state,
            id,
            &server_sender,
            &mut username,
            &mut joined,
            message,
        );
    }

    remove_client(&state, id, &username, joined);
    writer_task.abort();
}

async fn run_tcp_server(state: ServerState) -> std::io::Result<()> {
    let listener = TcpListener::bind(TCP_SERVER_ADDRESS).await?;
    println!("TCP server listening on {TCP_SERVER_ADDRESS}");

    loop {
        let (stream, address) = listener.accept().await?;
        let connection_state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_tcp_client(stream, connection_state, address).await {
                eprintln!("TCP client {address} error: {error}");
            }
        });
    }
}

async fn handle_tcp_client(
    stream: TcpStream,
    state: ServerState,
    address: SocketAddr,
) -> std::io::Result<()> {
    let id = state.allocate_id();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let (server_sender, mut server_receiver) = mpsc::unbounded_channel::<ServerMessage>();

    let writer_task = tokio::spawn(async move {
        while let Some(message) = server_receiver.recv().await {
            let json = match serde_json::to_string(&message) {
                Ok(json) => json,
                Err(error) => {
                    eprintln!("Could not serialize TCP message: {error}");
                    continue;
                }
            };
            let packet = format!("{json}\n");
            if write_half.write_all(packet.as_bytes()).await.is_err() {
                break;
            }
            if write_half.flush().await.is_err() {
                break;
            }
        }
    });

    let _ = server_sender.send(ServerMessage::Welcome { id });
    let _ = server_sender.send(ServerMessage::ServerInfo {
        message: "Connected to Veyron multiplayer over TCP!".to_string(),
    });

    let mut username = format!("Player{id}");
    let mut joined = false;
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = match reader.read_line(&mut line).await {
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                eprintln!("TCP read error from {address}: {error}");
                break;
            }
        };
        if bytes_read == 0 {
            break;
        }

        let message: ClientMessage = match serde_json::from_str(line.trim()) {
            Ok(message) => message,
            Err(error) => {
                eprintln!("Invalid TCP packet from {address}: {error}");
                continue;
            }
        };

        process_client_message(
            &state,
            id,
            &server_sender,
            &mut username,
            &mut joined,
            message,
        );
    }

    remove_client(&state, id, &username, joined);
    writer_task.abort();
    Ok(())
}

fn process_client_message(
    state: &ServerState,
    id: u64,
    sender: &UnboundedSender<ServerMessage>,
    username: &mut String,
    joined: &mut bool,
    message: ClientMessage,
) {
    match message {
        ClientMessage::Join { username: requested_username } => {
            *username = sanitize_username(&requested_username);
            let player = NetPlayer {
                id,
                username: username.clone(),
                x: 0.0,
                y: 3.5,
                z: 0.0,
                rotation_y: 0.0,
                grounded: true,
            };

            if let Ok(mut clients) = state.clients.lock() {
                clients.insert(
                    id,
                    ClientState {
                        player,
                        sender: sender.clone(),
                    },
                );
            } else {
                eprintln!("Client map lock was poisoned during join");
                return;
            }

            let first_join = !*joined;
            *joined = true;
            if first_join {
                println!("[JOIN] {} connected", username);
                broadcast_info(&state.clients, &format!("{} joined the game!", username));
            }
            broadcast_snapshot(&state.clients);
        }
        ClientMessage::State { x, y, z, rotation_y, grounded } => {
            if !*joined {
                return;
            }

            if let Ok(mut clients) = state.clients.lock() {
                if let Some(client) = clients.get_mut(&id) {
                    client.player.x = clamp_position(x);
                    client.player.y = clamp_position(y);
                    client.player.z = clamp_position(z);
                    client.player.rotation_y = if rotation_y.is_finite() {
                        rotation_y.clamp(-1000.0, 1000.0)
                    } else {
                        0.0
                    };
                    client.player.grounded = grounded;
                }
            }
            broadcast_snapshot(&state.clients);
        }
        ClientMessage::Chat { message } => {
            if !*joined {
                return;
            }
            let clean_message = sanitize_message(&message);
            if clean_message.is_empty() {
                return;
            }
            println!("[CHAT] {}: {}", username, clean_message);
            broadcast(
                &state.clients,
                &ServerMessage::Chat {
                    username: username.clone(),
                    message: clean_message,
                },
            );
        }
    }
}

fn remove_client(state: &ServerState, id: u64, username: &str, joined: bool) {
    if let Ok(mut clients) = state.clients.lock() {
        clients.remove(&id);
    }

    if joined {
        println!("[LEAVE] {username} disconnected");
        broadcast_info(&state.clients, &format!("{username} left the game."));
        broadcast_snapshot(&state.clients);
    }
}

fn broadcast_snapshot(clients: &Clients) {
    let players = match clients.lock() {
        Ok(all_clients) => all_clients.values().map(|client| client.player.clone()).collect(),
        Err(_) => {
            eprintln!("Client map lock was poisoned during snapshot");
            return;
        }
    };
    broadcast(clients, &ServerMessage::Snapshot { players });
}

fn broadcast_info(clients: &Clients, message: &str) {
    broadcast(clients, &ServerMessage::ServerInfo { message: message.to_string() });
}

fn broadcast(clients: &Clients, message: &ServerMessage) {
    let senders = match clients.lock() {
        Ok(all_clients) => all_clients.values().map(|client| client.sender.clone()).collect::<Vec<_>>(),
        Err(_) => {
            eprintln!("Client map lock was poisoned during broadcast");
            return;
        }
    };

    for sender in senders {
        let _ = sender.send(message.clone());
    }
}

fn sanitize_username(username: &str) -> String {
    let cleaned: String = username
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_' || *character == '-')
        .take(20)
        .collect();
    if cleaned.is_empty() { "Player".to_string() } else { cleaned }
}

fn sanitize_message(message: &str) -> String {
    message.chars().filter(|character| !character.is_control()).take(200).collect()
}

fn clamp_position(value: f32) -> f32 {
    if value.is_finite() { value.clamp(-10000.0, 10000.0) } else { 0.0 }
}
