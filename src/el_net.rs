//! Execution-layer (EL) networking: a minimal point-to-point TCP transport.
//!
//! Unlike the consensus layer — which gossips over libp2p/QUIC (`network.rs`) —
//! real EL blob propagation is point-to-point over devp2p/TCP: a node *announces*
//! blob hashes to its peers, peers *request* what they need, and the holder *serves*
//! the response. This module models that flow over raw TCP with RLP-encoded frames
//! (`[u32 big-endian length | msg_id byte | rlp(body)]`), without RLPx encryption or
//! discovery (out of scope for a dummy-crypto simulator).
//!
//! A single background actor owns the listener and connection bookkeeping, but each
//! peer connection gets its **own** reader and writer task. The actor never blocks on
//! a socket: to send, it hands an encoded frame to the target peer's writer task over
//! an unbounded channel, so a slow/backpressured peer only backs up its own queue and
//! never stalls the actor's read path or other peers' writes (avoiding head-of-line
//! blocking that otherwise collapses throughput at scale). The state machine issues
//! [`ElCommand`]s and receives [`ElEvent`]s. All bandwidth accounting stays in the
//! (single-threaded) state machine — events carry encoded byte sizes — so determinism
//! under Shadow is preserved.

use crate::types::{BlobHashAnnounce, ElMessage};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Identifies a single EL peer connection (assigned in connection order).
pub type ElPeerId = usize;

// NOTE: frames larger than 64 KiB require a Shadow build with the
// `tcp_sendUserData` 65535-byte send cap removed (the patched shadow-arm fork /
// kamilsa/shadow-arm image). On unpatched Shadow, sendto() returns a partial
// write even with buffer space free, and edge-triggered epoll (tokio) then waits
// forever for an EPOLLOUT edge that never fires — deadlocking the connection.

/// Outbound instruction from the state machine to the EL network actor.
enum ElCommand {
    /// Send a message to every connected peer (used for blob-hash announcements).
    Broadcast(ElMessage),
    /// Send a message to one specific peer (requests and responses).
    Send { peer: ElPeerId, msg: ElMessage },
}

/// Inbound event from the EL network actor to the state machine.
pub enum ElEvent {
    /// A peer connection was established.
    PeerConnected(ElPeerId),
    /// A peer connection was closed.
    PeerDisconnected(ElPeerId),
    /// A decoded message arrived from `from`. `bytes` is the full on-wire frame
    /// length (length prefix + id byte + RLP body) for bandwidth accounting.
    Message {
        from: ElPeerId,
        msg: ElMessage,
        bytes: usize,
    },
}

/// Handle to the EL network actor held by the state machine.
pub struct ElHandle {
    cmd_tx: mpsc::UnboundedSender<ElCommand>,
    /// Receives inbound EL events; polled by the state machine's event loop.
    pub event_rx: mpsc::UnboundedReceiver<ElEvent>,
}

impl ElHandle {
    /// Announce blob hashes to all connected EL peers (builder, t=0).
    pub fn announce(&self, announce: BlobHashAnnounce) {
        let _ = self
            .cmd_tx
            .send(ElCommand::Broadcast(ElMessage::Announce(announce)));
    }

    /// Send a request or response to a single peer.
    pub fn send(&self, peer: ElPeerId, msg: ElMessage) {
        let _ = self.cmd_tx.send(ElCommand::Send { peer, msg });
    }
}

/// Internal message from a per-connection reader task to the main actor.
enum ReaderMsg {
    Frame { peer: ElPeerId, payload: Vec<u8> },
    Closed { peer: ElPeerId },
}

/// Spawn the EL network actor and return a handle to it.
///
/// Binds a TCP listener on `listen_port` and dials each address in `peers`
/// (retrying until reachable, since peers may start in any order).
pub fn spawn_el_network(listen_port: u16, peers: Vec<SocketAddr>) -> ElHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    tokio::spawn(run_actor(listen_port, peers, cmd_rx, event_tx));

    ElHandle { cmd_tx, event_rx }
}

/// Main actor loop: owns the listener, all peer write halves, and connection ids.
async fn run_actor(
    listen_port: u16,
    peers: Vec<SocketAddr>,
    mut cmd_rx: mpsc::UnboundedReceiver<ElCommand>,
    event_tx: mpsc::UnboundedSender<ElEvent>,
) {
    let listener = match TcpListener::bind(("0.0.0.0", listen_port)).await {
        Ok(l) => l,
        Err(e) => {
            warn!(listen_port, error = %e, "EL: failed to bind TCP listener");
            return;
        }
    };
    info!(listen_port, "EL: listening on TCP");

    // Channel carrying outbound (dialed) streams back into the actor.
    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<TcpStream>();
    for addr in peers {
        let conn_tx = conn_tx.clone();
        tokio::spawn(async move {
            loop {
                match TcpStream::connect(addr).await {
                    Ok(stream) => {
                        info!(%addr, "EL: connected to peer");
                        let _ = conn_tx.send(stream);
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        });
    }

    // Channel carrying inbound frames / closes from per-connection reader/writer tasks.
    let (reader_tx, mut reader_rx) = mpsc::unbounded_channel::<ReaderMsg>();

    // Each peer maps to the sender half of its writer task's queue. Sending is
    // non-blocking: the frame is queued and the writer task drains it independently,
    // so one slow peer can never block the actor or other peers.
    let mut writers: HashMap<ElPeerId, mpsc::UnboundedSender<Arc<Vec<u8>>>> = HashMap::new();
    let mut next_id: ElPeerId = 0;
    let mut frames_delivered: u64 = 0; // DIAG: messages handed to the state machine

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                // Handle dropped (state machine finished) → shut the actor down.
                None => break,
                Some(ElCommand::Broadcast(msg)) => {
                    // Encode once and hand a shared copy to every peer's writer task.
                    let payload = Arc::new(msg.encode());
                    for tx in writers.values() {
                        let _ = tx.send(payload.clone());
                    }
                }
                Some(ElCommand::Send { peer, msg }) => {
                    if let Some(tx) = writers.get(&peer) {
                        let _ = tx.send(Arc::new(msg.encode()));
                    } else {
                        warn!(peer, "EL: send to unknown peer");
                    }
                }
            },

            accept = listener.accept() => {
                if let Ok((stream, addr)) = accept {
                    debug!(%addr, "EL: accepted inbound connection");
                    register_connection(stream, &mut next_id, &mut writers, &reader_tx, &event_tx);
                }
            }

            conn = conn_rx.recv() => {
                if let Some(stream) = conn {
                    register_connection(stream, &mut next_id, &mut writers, &reader_tx, &event_tx);
                }
            }

            rmsg = reader_rx.recv() => match rmsg {
                Some(ReaderMsg::Frame { peer, payload }) => {
                    // On-wire size includes the 4-byte length prefix.
                    let bytes = payload.len() + 4;
                    match ElMessage::decode(&payload) {
                        Ok(msg) => {
                            frames_delivered += 1;
                            let _ = event_tx.send(ElEvent::Message { from: peer, msg, bytes });
                        }
                        Err(e) => warn!(peer, error = ?e, "EL: failed to decode frame"),
                    }
                }
                Some(ReaderMsg::Closed { peer }) => {
                    // Either the reader or the writer task may report a close; only
                    // emit the disconnect once (when the writer is still present).
                    if writers.remove(&peer).is_some() {
                        let _ = event_tx.send(ElEvent::PeerDisconnected(peer));
                    }
                }
                None => {}
            },
        }
    }

    info!(frames_delivered, "EL: actor shutting down");
}

/// Split a stream, spawn its reader + writer tasks, and announce the peer. The
/// returned writer-queue sender is stored so the actor can enqueue frames without
/// ever awaiting a socket write itself.
fn register_connection(
    stream: TcpStream,
    next_id: &mut ElPeerId,
    writers: &mut HashMap<ElPeerId, mpsc::UnboundedSender<Arc<Vec<u8>>>>,
    reader_tx: &mpsc::UnboundedSender<ReaderMsg>,
    event_tx: &mpsc::UnboundedSender<ElEvent>,
) {
    let _ = stream.set_nodelay(true);
    let id = *next_id;
    *next_id += 1;

    let (read_half, write_half) = stream.into_split();
    let (write_tx, write_rx) = mpsc::unbounded_channel::<Arc<Vec<u8>>>();
    writers.insert(id, write_tx);
    spawn_writer(id, write_half, write_rx, reader_tx.clone());
    spawn_reader(id, read_half, reader_tx.clone());
    let _ = event_tx.send(ElEvent::PeerConnected(id));
}

/// Per-connection writer: drains this peer's frame queue and writes each frame to the
/// socket. Runs independently of the actor, so a backpressured socket only stalls this
/// one peer. On write failure it reports the peer closed and exits.
fn spawn_writer(
    peer: ElPeerId,
    mut write_half: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Arc<Vec<u8>>>,
    closed_tx: mpsc::UnboundedSender<ReaderMsg>,
) {
    tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if let Err(e) = write_frame(&mut write_half, &payload).await {
                debug!(peer, error = %e, "EL: write failed");
                let _ = closed_tx.send(ReaderMsg::Closed { peer });
                break;
            }
        }
    });
}

/// Per-connection reader: decode frames off the wire and forward them to the actor.
fn spawn_reader(
    peer: ElPeerId,
    mut read_half: OwnedReadHalf,
    tx: mpsc::UnboundedSender<ReaderMsg>,
) {
    tokio::spawn(async move {
        loop {
            match read_frame(&mut read_half).await {
                Ok(payload) => {
                    if tx.send(ReaderMsg::Frame { peer, payload }).is_err() {
                        break; // actor gone
                    }
                }
                Err(_) => {
                    let _ = tx.send(ReaderMsg::Closed { peer });
                    break;
                }
            }
        }
    });
}

/// Write a single length-prefixed frame.
async fn write_frame(w: &mut OwnedWriteHalf, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(payload).await?;
    w.flush().await
}

/// Read a single length-prefixed frame.
async fn read_frame(r: &mut OwnedReadHalf) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok(payload)
}
