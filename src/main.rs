// main.rs

use std::{fs, path::PathBuf};

use anyhow::Error;
use clap::Parser;
use log::error;
use network::Config;
use toml::de;
mod network;

fn config_from_file(file_name: &str) -> Result<Config, Error> {
    let file_contents = fs::read(file_name)?;
    Ok(de::from_slice(&file_contents)?)
}

#[derive(Parser)]
struct Opt {
    #[clap(parse(try_from_str = config_from_file))]
    pub config: Config,
    pub peers_file: PathBuf,
    pub listen_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // launch network controller
    let opt = Opt::parse();
    let config = opt.config;

    let mut net =
        network::controller::NetworkController::new(opt.peers_file, opt.listen_port, config)
            .await?;

    /*

        NetworkController internally maintains a list of known peers and connections with them.

            It does not read/write on sockets, but only listens/connects



        NetworkController::new should create a NetworkController object and spawn an async loop that:

            - maintains a list of known peers identified by their IP addresses

            - each peer in the list has the following properties:

                - status: enum:

                    Idle : we know about the peer but we aren't currently doing anything with it

                    OutConnecting : we are currently trying to establish an outgoing TCP connection to the peer

                    OutHandshaking : we are currently handshaking with a peeer after having established and outgoing TCP connection to it

                    OutAlive : we have an outgoing TCP connection to the peer and the handshake is done, the peer is functional

                    InHandshaking : we are currently handshaking with a peeer after this peer established a TCP connection towards our node

                    InAlive : the peer has established a TCP connection towards us and the handshake is done, the peer is functional

                    Banned : we have banned this peer: we won't connect to it and will reject connection attempts from it

                - last_alive: Option<DateTime<Utc>> : date at which we have seen the peer in an InAlive our OutAlive state for the last time

                    if None, it means thet we never had a successful TCP connection + handshake with the peer

                - last_failure: Option<DateTime<Utc>> : date at which a connection or handshake failed for the last time with that peer

                    if status == "Banned", it is set to the time of ban, and updated at any subsequent incoming connection attempt from that peer

                    if None, it means that we never failed to connect or handshake, and that we never banned that peer

            - on startup, the peer list is loaded from the JSON file peers_file (this file should be preloaded with a list of bootstrap peers at first launch)

            - every peer_file_dump_interval_seconds seconds, the peer list is dumped to the peers_file JSON file if there have been any changes

            - always tries to keep target_outgoing_connections peers in a OutAlive status by launching outgoing TCP connections towards the most promising peers when necessary

                - when starting a connection attempt, set the peer status to OutConnecting

                - when a TCP connection is established, set the peer status to OutHandshaking and emit a network::controller::NetworkControllerEvent::CandidateConnection event

                - up to max_simultaneous_outgoing_connection_attempts peers can be in an OutConnecting or OutHandshaking status

            - listens on port listen_port, accepts incoming TCP connections

                - when a connection is accepted, set the peer status to InHandshaking and emit a network::controller::NetworkControllerEvent::CandidateConnection event

                    if the peer is absent from the peer list, add it to the peer list

                - no more than max_incoming_connections peers can have InAlive status, extra connection attemps must be rejected

                - no more than max_simultaneous_incoming_connection_attempts peers can have InHandshaking status, extra connection attemps must be rejected

            - no more than max_banned_peers can have Banned status. If necessary, some smartly chosen Banned peers may be dropped to respect this condition.

            - no more than max_idle_peers can have Idle status. If necessary, some smartly chosen Idle peers may be dropped to respect this condition.

            - only up to a single TCP connection per peer is allowed (whatever the direction)

    */

    // loop over messages coming from the network controller

    loop {
        tokio::select! {
            evt = net.wait_event() => match evt {
                Ok(msg) => match msg {
                    network::controller::NetworkControllerEvent::CandidateConnection {ip, socket, is_outgoing} => {
                        let _ = (ip, socket, is_outgoing);
                        // ip is the peer ip, and socket is a tokio TCPStream

                        // triggered when a new TCP connection with a peer is established

                        // is_outgoing is true if our node has connected to the peer node

                        // is_outgoing is false if the peer node has connected to our node



                        // here, a handshake must be performed by reading/writing data to socket

                        //  if the handshake succesds, call net.feedback_peer_alive(ip).await; to signal NetworkController to set the peer in InAlive or OutAlive state (this should update last_alive)

                        //  if handshake fails or the connection closes unexpectedly at any time, call net.feedback_peer_failed(ip).await; to signal NetworkController to set the peer status to Idle  (this should update last_failure)



                        // once the handshake is done, we can use this peer socket in main.rs
                        //return Ok(());

                    }
                },
                Err(e) => { error!("Error in loop: {:?}", e); break},
            }
        }
    }

    /*

        call net.feedback_peer_alive(ip).await whenever the peer gives a sign of life (this should update last_alive)



        if the peer misbehaves at any time, call net.feedback_peer_banned(ip).await; to signal NetworkController to set the peer status to Banned (this should update last_failure)



        if we have closed the peer connection cleanly, call net.feedback_peer_closed(ip).await; to signal NetworkController to set the peer status to Idle



        after handshake, and then again periodically, main.rs should ask alive peers for the list of peer IPs they know about, and feed them to the network controller: net.feedback_peer_list(list_of_ips).await;

            net.feedback_peer_list should merge the new peers to the existing peer list in a smart way

        similarly, peers can ask us for the list of peer IPs we know about, and we can retrieve it with net.get_good_peer_ips()

            Note that net.get_good_peer_ips() excludes banned peers and sorts the peers from "best" to "worst"

    */

    net.shut_down().await
}
