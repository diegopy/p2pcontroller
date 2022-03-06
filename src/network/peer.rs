use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use strum::Display;
use time::OffsetDateTime;

#[derive(Copy, Clone, Display)]
pub enum PeerState {
    Idle,           //we know about the peer but we aren't currently doing anything with it
    OutConnecting,  //we are currently trying to establish an outgoing TCP connection to the peer
    OutHandshaking, //we are currently handshaking with a peeer after having established and outgoing TCP connection to it
    OutAlive, //we have an outgoing TCP connection to the peer and the handshake is done, the peer is functional
    InHandshaking, //we are currently handshaking with a peeer after this peer established a TCP connection towards our node
    InAlive, //the peer has established a TCP connection towards us and the handshake is done, the peer is functional
    Banned, //we have banned this peer: we won't connect to it and will reject connection attempts from it
}

pub struct Peer {
    address: SocketAddr,
    state: PeerState,
    last_alive: Option<OffsetDateTime>,
    last_failure: Option<OffsetDateTime>,
}

impl Peer {
    pub fn new_in_handshaking(address: SocketAddr) -> (Self, StateChange) {
        (
            Peer {
                address,
                state: PeerState::InHandshaking,
                last_alive: None,
                last_failure: None,
            },
            StateChange {
                old: None,
                new: PeerState::InHandshaking,
            },
        )
    }

    pub fn set_out_handshaking(&mut self) -> Result<StateChange> {
        if matches!(
            self.state,
            PeerState::Banned | PeerState::InAlive | PeerState::InHandshaking
        ) {
            bail!("Can't set out_handshaking. Current state is {}", self.state)
        }
        Ok(self.transition_to(PeerState::OutHandshaking))
    }

    pub fn set_in_handshaking(&mut self) -> Result<StateChange> {
        if matches!(
            self.state,
            PeerState::Banned
                | PeerState::OutConnecting
                | PeerState::OutHandshaking
                | PeerState::OutAlive
        ) {
            bail!("Can't set in_handshaking. Current state is {}", self.state)
        }
        Ok(self.transition_to(PeerState::InHandshaking))
    }

    pub fn fail_to_idle(&mut self) -> StateChange {
        self.last_failure = Some(OffsetDateTime::now_utc());
        self.transition_to(PeerState::Idle)
    }

    pub fn set_alive(&mut self, is_outgoing: bool) -> StateChange {
        self.last_alive = Some(OffsetDateTime::now_utc());
        let new_state = if is_outgoing {
            PeerState::OutAlive
        } else {
            PeerState::InAlive
        };
        self.transition_to(new_state)
    }

    pub fn state(&self) -> PeerState {
        self.state
    }

    fn transition_to(&mut self, new_state: PeerState) -> StateChange {
        let state_change = StateChange {
            old: Some(self.state),
            new: new_state,
        };
        self.state = new_state;
        state_change
    }
}

pub struct StateChange {
    pub old: Option<PeerState>,
    pub new: PeerState,
}

/// Json representation of a peer
/// It doesn't make sense to map the full state, only banned or not.
/// The other states depend on the runtime state of the controller.
#[derive(Serialize, Deserialize)]
pub struct JsonPeer {
    pub address: SocketAddr,
    pub is_banned: bool,
    pub last_alive: Option<OffsetDateTime>,
    pub last_failure: Option<OffsetDateTime>,
}

impl From<JsonPeer> for Peer {
    fn from(json_peer: JsonPeer) -> Self {
        Self {
            address: json_peer.address,
            state: if json_peer.is_banned {
                PeerState::Banned
            } else {
                PeerState::Idle
            },
            last_alive: json_peer.last_alive,
            last_failure: json_peer.last_failure,
        }
    }
}

impl From<&Peer> for JsonPeer {
    fn from(peer: &Peer) -> Self {
        Self {
            address: peer.address,
            is_banned: matches!(peer.state, PeerState::Banned),
            last_alive: peer.last_alive,
            last_failure: peer.last_failure,
        }
    }
}
