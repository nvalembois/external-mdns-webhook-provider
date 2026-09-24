
use clap::Parser;
use simple_dns::{
    rdata::RData, Packet, PacketFlag, QTYPE, ResourceRecord, CLASS, TYPE,
};
use tracing::{debug, error, info};
use socket2::{Domain, Protocol, Socket, Type as SockType};
use std::{net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, UdpSocket}, process::ExitCode, str::FromStr};
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
    info!("Config: health_listen_addr={}", &app_config.health_listen_addr);
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

    let socket: UdpSocket = create_socket()
        .map_err(|e| format!("Listen on '{MDNS_ADDR}:{MDNS_PORT}' error : {e}"))?;
    info!(
        "Serveur mDNS (requêtes de type host uniquement) à l'écoute sur {MDNS_ADDR}:{MDNS_PORT}"
    );

    let mut buf = [0u8; 4096];

    loop {
        let (len, src) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                error!("Erreur de réception : {e}");
                continue;
            }
        };

        let packet = match Packet::parse(&buf[..len]) {
            Ok(p) => p,
            Err(_) => continue, // paquet non-DNS ou malformé : on ignore
        };

        // On ignore tout ce qui n'est pas une requête (les réponses des
        // autres participants mDNS, notamment).
        if packet.has_flags(PacketFlag::RESPONSE) {
            continue;
        }

        let Some(response) = build_response(&packet, &file_store).await else { 
            continue;
        };

        match response.build_bytes_vec_compressed() {
            Ok(bytes) => match socket.send_to(&bytes, src) {
                Ok(_) => {
                    for q in &response.questions {
                        debug!("Réponse envoyée pour « {} » à {src}", q.qname);
                    }
                }
                Err(e) => error!("Erreur d'envoi de la réponse : {e}"),
            },
            Err(e) => error!("Erreur de construction de la réponse : {e}"),
        }
    }
}

/// Construit, si possible, le paquet de réponse mDNS pour une requête donnée.
/// Retourne `None` si aucune question ne correspond à une entrée connue.
async fn build_response<'a>(query: &Packet<'a>, cm_store: &FileStore) -> Option<Packet<'a>> {
    let mut reply = Packet::new_reply(query.id());
    reply.set_flags(PacketFlag::AUTHORITATIVE_ANSWER);

    // return eraly to avoid locking cache
    if query.questions.is_empty() {
        return None
    }
    
    for question in &query.questions {
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

    Ok(socket.into())
}
