
use clap::Parser;
use simple_dns::{
    CLASS, Packet, PacketFlag, QTYPE, Question, ResourceRecord, TYPE, rdata::RData,
};
use tokio::{signal, task::JoinSet};
use tokio::net::UdpSocket;
use tracing::{debug, error, info};
use socket2::{Domain, Protocol, Socket, Type as SockType};
use std::{net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4}, process::ExitCode, str::FromStr, sync::Arc, time::Duration};
use mdns_webhook_provider::model::{config::MDNSConfig, filestore::FileStore, records::RecordType};

const MDNS_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
const MDNS_PORT: u16 = 5353;
const TTL: u32 = 120; // TTL usuel pour les enregistrements d'hôte en mDNS

#[tokio::main]
async fn main() -> ExitCode {
    let Ok(app_config) = MDNSConfig::try_parse() else {
        return ExitCode::FAILURE;
    };

    tracing_subscriber::fmt()
        .with_max_level(if app_config.debug {tracing::Level::DEBUG} else { tracing::Level::INFO} )
        .init();

    info!("Config: filestore_path={}", &app_config.filestore_path);
    info!("Config: debug={}", &app_config.debug);

    match run(app_config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(app_config: MDNSConfig) -> Result<(), String> {
    let mut file_store = FileStore::new(
        &app_config.filestore_path,
        ).await;
    
    file_store.spawn_watcher();
    let file_store = Arc::new(file_store);

    let socket= create_socket()
        .map_err(|e| format!("Listen on '{MDNS_ADDR}:{MDNS_PORT}' error : {e}"))?;
    let socket = Arc::new(socket);
    info!(
        "Serveur mDNS (requêtes de type host uniquement) à l'écoute sur {MDNS_ADDR}:{MDNS_PORT}"
    );

    // Garde la trace de toutes les tâches lancées, pour pouvoir les
    // attendre (ou les annuler) proprement lors du shutdown.
    let mut join_set: JoinSet<()> = JoinSet::new();
 
    // Future de shutdown "épinglée" une seule fois pour être réutilisée
    // à chaque itération du select! sans se reconstruire.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let mut buf = vec![0u8; 4096];

    loop {
        tokio::select! {
            // biased! => on vérifie le shutdown en priorité, pour ne pas
            // accepter une nouvelle requête juste après avoir reçu le signal.
            biased;
 
            _ = &mut shutdown => {
                info!("\nArrêt demandé : on cesse d'accepter de nouvelles requêtes.");
                break;
            }

            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((len, src)) => {
                        let socket = socket.clone();
                        let file_store = file_store.clone();
                        let data = buf[..len].to_vec();
                        join_set.spawn(async move {
                            let packet = match Packet::parse(&data) {
                                Ok(p) => p,
                                Err(e) => {
                                    debug!("Ignore malformed mdns query : {e}");
                                    return;
                                },
                            };
                            if packet.has_flags(PacketFlag::RESPONSE) {
                                return;
                            } 
                            // Lancement d'une tâche asynchrone légère (pas un thread OS).
                            process_mdns_request(file_store, src, packet, socket).await;
                        });
                    }
                    Err(e) => {
                        error!("Erreur de réception: {}", e);
                    }
                }
            }

            // Réclame (drain) les tâches déjà terminées au fil de l'eau.
            Some(res) = join_set.join_next(), if !join_set.is_empty() => {
                if let Err(e) = res {
                    error!("Une tâche a paniqué ou a été annulée: {}", e);
                }
            }
        }
    }
    gracefull_shutdown(join_set, Duration::from_secs(app_config.gracefull_shutdown_timeout)).await;
    close_socket(socket);
    info!("Shutdown completed.");
    Ok(())
}

async fn process_mdns_request(file_store: Arc<FileStore>, src: std::net::SocketAddr, packet: Packet<'_>, socket: Arc<UdpSocket>) {
    debug!("process mdns query id: {} with {} question(s)", packet.id(), packet.questions.capacity());
    let unicast_questions = packet.questions.iter().filter(|q| q.unicast_response).collect();
    if let Some(response) = build_response(packet.id(), unicast_questions, &file_store).await { 
        send_mdns_response(src, &socket, response).await;
    };
    let multicast_questions = packet.questions.iter().filter(|q| !q.unicast_response).collect();
    if let Some(response) = build_response(packet.id(), multicast_questions, &file_store).await { 
        send_mdns_response(SocketAddr::new(std::net::IpAddr::V4(MDNS_ADDR), MDNS_PORT), &socket, response).await;
    };
}

async fn send_mdns_response(dst: std::net::SocketAddr, socket: &Arc<UdpSocket>, response: Packet<'_>) {
    match response.build_bytes_vec_compressed() {
        Ok(bytes) => match socket.send_to(&bytes, dst).await {
            Ok(_) => {
                for q in &response.questions {
                    debug!("Réponse envoyée pour « {} » à {dst}", q.qname);
                }
            }
            Err(e) => error!("Erreur d'envoi de la réponse : {e}"),
        },
        Err(e) => error!("Erreur de construction de la réponse : {e}"),
    }
}

/// Construit, si possible, le paquet de réponse mDNS pour un ensemble de questions données.
/// Retourne `None` si aucune question ne correspond à une entrée connue.
async fn build_response<'a>(
    query_id: u16,
    questions: Vec<&Question<'a>>,
    cm_store: &Arc<FileStore>
) -> Option<Packet<'a>> {
    debug!("build response for: {} with {} question(s)",query_id, questions.capacity());

    // return eraly to avoid locking cache
    if questions.is_empty() {
        return None
    }

    let mut reply = Packet::new_reply(query_id);
    reply.set_flags(PacketFlag::AUTHORITATIVE_ANSWER);
    
    for question in questions {
        // On ne traite QUE les questions de type "host" (A / AAAA) : les
        // requêtes de découverte de service (PTR, SRV, TXT, ANY, ...) sont
        // silencieusement ignorées.
        let wanted_type = match question.qtype {
            QTYPE::TYPE(TYPE::A) => RecordType::A,
            QTYPE::TYPE(TYPE::AAAA) => RecordType::AAAA,
            _ => continue,
        };
        debug!("received query {} {}", match wanted_type { RecordType::A=> "A", RecordType::AAAA=>"AAAA",_=> "??" }, question.qname.to_string().to_lowercase());

        let ips = cm_store.query(question).await;
        for ip in ips {
            let rdata = match wanted_type {
                RecordType::A => {
                    let Ok(ip) = Ipv4Addr::from_str(&ip) else { continue };
                    RData::A(ip.into())
                },
                RecordType::AAAA => {
                    let Ok(ip) = Ipv6Addr::from_str(&ip) else { continue };
                    RData::AAAA(ip.into())
                },
                _ => continue,
            };

            // On réutilise le nom de la question (même durée de vie que le
            // paquet reçu), pas besoin de le ré-encoder à la main.
            let record = ResourceRecord::new(question.qname.clone(), CLASS::IN, TTL, rdata)
                .with_cache_flush(true);
            reply.answers.push(record);
        }

        // On renvoie la question dans la réponse : ça n'est pas obligatoire
        // en mDNS multicast, mais ça aide les résolveurs classiques (dig, etc.)
        reply.questions.push(question.clone());
    }

    if reply.answers.is_empty() {
        None
    } else {
        Some(reply)
    }
}

/// Crée et configure le socket UDP multicast IPv4 sur le port mDNS standard.
fn create_socket() -> std::io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, SockType::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MDNS_PORT);
    socket.bind(&bind_addr.into())?;
    socket.join_multicast_v4(&MDNS_ADDR, &Ipv4Addr::UNSPECIFIED)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_nonblocking(true)?;

    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

fn close_socket(socket: Arc<UdpSocket>) {
    if let Err(e) = socket.leave_multicast_v4( MDNS_ADDR, Ipv4Addr::UNSPECIFIED) {
        error!("Error leaving multicast : {e}");
    }
    drop(socket);
}


// /// Crée et configure le socket UDP multicast IPv4 sur le port mDNS standard.
// fn create_socket() -> std::io::Result<UdpSocket> {
//     let socket = Socket::new(Domain::IPV4, SockType::DGRAM, Some(Protocol::UDP))?;
//     socket.set_reuse_address(true)?;
//     #[cfg(unix)]
//     socket.set_reuse_port(true)?;

//     let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MDNS_PORT);
//     socket.bind(&bind_addr.into())?;
//     socket.join_multicast_v4(&MDNS_ADDR, &Ipv4Addr::UNSPECIFIED)?;
//     socket.set_multicast_loop_v4(true)?;

//     Ok(socket.into())
// }

/// Attend soit Ctrl+C, soit un SIGTERM (utile quand le process est arrêté
/// par systemd, Docker, Kubernetes, etc.). Se résout dès que l'un des deux
/// survient.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("échec de l'installation du handler Ctrl+C");
    };
 
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("échec de l'installation du handler SIGTERM")
            .recv()
            .await;
    };
 
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
 
    tokio::select! {
        _ = ctrl_c => println!("Signal Ctrl+C reçu"),
        _ = terminate => println!("Signal SIGTERM reçu"),
    }
}

/// Attend la fin des tâches déjà lancées, avec un délai maximum. Si le délai
/// est dépassé, les tâches restantes sont annulées de force.
async fn gracefull_shutdown(mut join_set: JoinSet<()>, timeout: Duration) {
    let in_flight = join_set.len();
    if in_flight == 0 {
        println!("Aucune tâche en cours.");
        return;
    }
 
    println!(
        "Attente de {} tâche(s) en cours (max {}s)...",
        in_flight,
        timeout.as_secs()
    );
 
    let drain = async {
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                eprintln!("Une tâche a paniqué ou a été annulée: {}", e);
            }
        }
    };
 
    match tokio::time::timeout(timeout, drain).await {
        Ok(()) => println!("Toutes les tâches en cours se sont terminées normalement."),
        Err(_) => {
            let remaining = join_set.len();
            eprintln!(
                "Timeout atteint : {} tâche(s) encore active(s), arrêt forcé.",
                remaining
            );
            // Annule (abort) toutes les tâches restantes du JoinSet.
            join_set.shutdown().await;
        }
    }
}
 
