use anyhow::{anyhow, bail, Context, Result};
use futures::{future, stream::FuturesUnordered, Future, StreamExt};
use log::{debug, error, info};
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Entry, HashMap},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs,
    net::{TcpListener, TcpStream},
    sync::broadcast,
    sync::mpsc,
    task::JoinHandle,
    time::{self, timeout},
};

use crate::network::peer::JsonPeer;

use super::{
    peer::{Peer, PeerState, StateChange},
    Config,
};

#[derive(Debug)]
pub enum NetworkControllerEvent {
    CandidateConnection {
        ip: SocketAddr,
        socket: TcpStream,
        is_outgoing: bool,
    },
}

pub struct NetworkController {
    stop_signal: broadcast::Sender<()>,
    join_handles: Vec<JoinHandle<()>>,
    event_receiver: mpsc::Receiver<NetworkControllerEvent>,
    state: Arc<ControllerState>,
}

impl NetworkController {
    pub async fn new(peers_file: PathBuf, listen_port: u16, config: Config) -> Result<Self> {
        let peers_file_contents = fs::read(&peers_file).await?;
        let peers: Vec<JsonPeer> = serde_json::from_slice(&peers_file_contents)?;
        let peers = HashMap::from_iter(peers.into_iter().map(|p| (p.address.ip(), p.into())));
        let (stop_signal, stop) = broadcast::channel(1);
        let (event_sender, event_receiver) = mpsc::channel(100);
        let state = Arc::new(ControllerState::new(config.clone(), peers));
        let acceptor_handle = tokio::spawn(monitor(
            "acceptor",
            stop_signal.subscribe(),
            acceptor(
                listen_port,
                config.clone(),
                Arc::clone(&state),
                event_sender.clone(),
            ),
        ));
        let connector_handle = tokio::spawn(monitor(
            "connector",
            stop_signal.subscribe(),
            connector(config.clone(), Arc::clone(&state), event_sender),
        ));
        let dumper_handle = tokio::spawn(monitor(
            "dumper",
            stop,
            dumper(
                peers_file,
                config.peer_file_dump_interval.try_into()?,
                Arc::clone(&state),
            ),
        ));
        let join_handles = vec![acceptor_handle, connector_handle, dumper_handle];
        Ok(Self {
            stop_signal,
            join_handles,
            event_receiver,
            state,
        })
    }

    pub async fn wait_event(&mut self) -> Result<NetworkControllerEvent> {
        let event = self
            .event_receiver
            .recv()
            .await
            .context("Event channel closed")?;
        Ok(event)
    }

    pub fn feedback_peer_alive(&self, address: SocketAddr, is_outgoing: bool) {
        self.state.set_peer_alive(address, is_outgoing);
    }

    pub async fn shut_down(self) -> Result<()> {
        self.stop_signal
            .send(())
            .map_err(|_| anyhow!("Stop signar receiver closed. Already shutdown?"))?;
        future::try_join_all(self.join_handles).await?;
        Ok(())
    }
}

async fn acceptor(
    listen_port: u16,
    config: Config,
    state: Arc<ControllerState>,
    event_sender: mpsc::Sender<NetworkControllerEvent>,
) -> Result<()> {
    let listener = TcpListener::bind(SocketAddr::new("::".parse().unwrap(), listen_port)).await?;
    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                if let Err(e) = state.accept_incomming(peer_addr) {
                    info!(
                        "acceptor: Rejecting incomming connection from {} due to: {}",
                        peer_addr, e
                    );
                };
                event_sender
                    .send(NetworkControllerEvent::CandidateConnection {
                        ip: peer_addr,
                        socket,
                        is_outgoing: false,
                    })
                    .await
                    .context("Event channel closed")?;
            }
            Err(e) => {
                error!("Error accepting connection: {:?}. Retrying", e)
            }
        }
    }
}

async fn connector(
    config: Config,
    state: Arc<ControllerState>,
    event_sender: mpsc::Sender<NetworkControllerEvent>,
) -> Result<()> {
    loop {
        let candidates = state.prepare_outgoing_candidates();
        let mut futures: FuturesUnordered<_> = candidates
            .iter()
            .map(|addr| try_connect(config.connection_timeout, *addr))
            .collect();
        while let Some(connection_result) = futures.next().await {
            match connection_result {
                Ok((address, stream)) => {
                    if let Err(e) = state.accept_outgoing(address) {
                        info!("connector: Error accepting outgoing to {}: {}", address, e)
                    } else {
                        event_sender
                            .send(NetworkControllerEvent::CandidateConnection {
                                ip: address,
                                socket: stream,
                                is_outgoing: true,
                            })
                            .await
                            .context("Event channel closed")?;
                    }
                }
                Err(e) => state
                    .failed_outgoing(e.0)
                    .map_err(|e| error!("Setting failed_outging {:?}", e))
                    .unwrap_or(()),
            };
        }
        if candidates.is_empty() {
            debug!(
                "connector: Nothing to do. Sleeping for {:?}",
                config.connector_wait
            );
            time::sleep(config.connector_wait).await;
        }
    }
}

struct ConnectionError(pub SocketAddr);

async fn try_connect(
    max_wait: Duration,
    address: SocketAddr,
) -> std::result::Result<(SocketAddr, TcpStream), ConnectionError> {
    if let Ok(connection_result) = timeout(max_wait, TcpStream::connect(address)).await {
        match connection_result {
            Ok(stream) => return Ok((address, stream)),
            Err(e) => error!("connection: Error while connecting to {}: {:?}", address, e),
        }
    } else {
        error!("connector: Timeout connecting to {} ", address);
    };
    Err(ConnectionError(address))
}

async fn dumper(
    peers_file: PathBuf,
    dump_interval: Duration,
    state: Arc<ControllerState>,
) -> Result<()> {
    loop {
        time::sleep(dump_interval).await;
        let values = state.json_peers();
        let json_peers = serde_json::to_vec(&values)?;
        if let Err(e) = fs::write(&peers_file, json_peers).await {
            error!("dumper: Error writing peers file {:?}: {:?}", peers_file, e)
        }
    }
}

async fn monitor(
    task_name: impl AsRef<str>,
    mut stop_signal: broadcast::Receiver<()>,
    fut: impl Future<Output = Result<()>>,
) {
    tokio::select! {
        result = fut => match result {
            Ok(_) => info!("Task {} completed succesfully", task_name.as_ref()),
            Err(e) => error!("Task {} completed with error: {:#?}", task_name.as_ref(), e),
        },
        _ = stop_signal.recv() => {
            info!("{}: Stop signal received. Shutting down.", task_name.as_ref());
        }
    }
}

struct ControllerState {
    config: Config,
    inner: Mutex<InnerState>,
}

impl ControllerState {
    pub fn new(config: Config, peers: HashMap<IpAddr, Peer>) -> Self {
        Self {
            config,
            inner: Mutex::new(InnerState::new(peers)),
        }
    }

    pub fn in_alive_count(&self) -> u32 {
        self.inner.lock().in_alive_count
    }

    pub fn in_handshacking_count(&self) -> u32 {
        self.inner.lock().in_handshaking_count
    }

    pub fn json_peers(&self) -> Vec<JsonPeer> {
        let peers = &self.inner.lock().peers;
        peers.values().map(Into::into).collect()
    }

    pub fn accept_incomming(&self, address: SocketAddr) -> Result<()> {
        let mut locked_inner = self.inner.lock();
        let can_accept_incomming = locked_inner.in_alive_count
            < self.config.max_incoming_connections
            && locked_inner.in_handshaking_count
                < self.config.max_simultaneous_incoming_connection_attempts;
        if !can_accept_incomming {
            bail!("Incomming cap reached");
        };
        let state_change = match locked_inner.peers.entry(address.ip()) {
            Entry::Occupied(mut oe) => oe.get_mut().set_in_handshaking(),
            Entry::Vacant(ve) => {
                let (peer, state_change) = Peer::new_in_handshaking(address);
                ve.insert(peer);
                Ok(state_change)
            }
        }
        .context(format!("accept_incomming: Error accepting {}", address))?;
        locked_inner.update_counts(&state_change);
        Ok(())
    }

    pub fn accept_outgoing(&self, address: SocketAddr) -> Result<()> {
        let mut locked_inner = self.inner.lock();
        if locked_inner.out_connecting_count + locked_inner.out_handshaking_count
            >= self.config.max_simultaneous_outgoing_connection_attempts
        {
            bail!("Outgoing cap reached")
        }
        let state_change = locked_inner
            .peers
            .get_mut(&address.ip())
            .ok_or_else(|| anyhow!("accept_outgoing: Peer not found"))?
            .set_out_handshaking()
            .context(format!("accept_outgoing: Error accepting {}", address))?;
        locked_inner.update_counts(&state_change);
        Ok(())
    }

    pub fn failed_outgoing(&self, address: SocketAddr) -> Result<()> {
        let mut locked_inner = self.inner.lock();
        let peer = locked_inner
            .peers
            .get_mut(&address.ip())
            .ok_or_else(|| anyhow!("failed_outgoing: Peer not found"))?;
        let state_change = peer.fail_to_idle();
        locked_inner.update_counts(&state_change);
        Ok(())
    }

    pub fn set_peer_alive(&self, address: SocketAddr, is_outgoing: bool) -> Result<()> {
        let mut locked_inner = self.inner.lock();
        let peer = locked_inner
            .peers
            .get_mut(&address.ip())
            .ok_or_else(|| anyhow!("set_peer_alive: Peer not found"))?;
        let state_change = peer.set_alive(is_outgoing);
        locked_inner.update_counts(&state_change);
        Ok(())
    }

    pub fn prepare_outgoing_candidates(&self) -> Vec<SocketAddr> {
        vec![]
    }
}

struct InnerState {
    peers: HashMap<IpAddr, Peer>,
    in_alive_count: u32,
    in_handshaking_count: u32,
    out_connecting_count: u32,
    out_handshaking_count: u32,
    out_alive_count: u32,
    idle_count: u32,
    banned_count: u32,
}

impl InnerState {
    pub fn new(peers: HashMap<IpAddr, Peer>) -> Self {
        let mut idle_count = 0;
        let mut banned_count = 0;
        for peer in peers.values() {
            match peer.state() {
                PeerState::Idle => idle_count += 1,
                PeerState::Banned => banned_count += 1,
                _ => (),
            }
        }
        Self {
            peers,
            in_alive_count: 0,
            in_handshaking_count: 0,
            out_connecting_count: 0,
            out_handshaking_count: 0,
            out_alive_count: 0,
            idle_count,
            banned_count,
        }
    }

    fn update_counts(&mut self, transition: &StateChange) {
        if let Some(old_state) = transition.old {
            match old_state {
                PeerState::Idle => self.idle_count -= 1,
                PeerState::OutConnecting => self.out_connecting_count -= 1,
                PeerState::OutHandshaking => self.out_handshaking_count -= 1,
                PeerState::OutAlive => self.out_alive_count -= 1,
                PeerState::InHandshaking => self.in_handshaking_count -= 1,
                PeerState::InAlive => self.in_alive_count -= 1,
                PeerState::Banned => self.banned_count -= 1,
            }
        }
        match transition.new {
            PeerState::Idle => self.idle_count += 1,
            PeerState::OutConnecting => self.out_connecting_count += 1,
            PeerState::OutHandshaking => self.out_handshaking_count += 1,
            PeerState::OutAlive => self.out_alive_count += 1,
            PeerState::InHandshaking => self.in_handshaking_count += 1,
            PeerState::InAlive => self.in_alive_count += 1,
            PeerState::Banned => self.banned_count += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    #[test]
    fn bind_ip() {
        let addr = SocketAddr::new("::".parse().unwrap(), 1234);
        assert!(addr.ip().is_unspecified());
        assert_eq!(1234, addr.port());
    }
}
