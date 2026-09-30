
use std::{path::Path, thread::sleep};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;
use notify::{Error, Event, RecursiveMode, Watcher, recommended_watcher};
use tracing::{debug, error};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub enum FileEvent {
    Create,
    Modify,
    Remove,
}

pub struct FileWatcher {
    // path: String,
    receiver: tokio_mpsc::Receiver<Result<Event, Error>>,
    shutdown: CancellationToken,
}

impl FileWatcher {

    pub async fn new(path: &str, ) -> (Self, JoinHandle<()>) {
        let (tx, rx) = tokio_mpsc::channel::<notify::Result<Event>>(100);
        let shutdown = CancellationToken::new();

        let shutdown_clone = shutdown.clone();
        let watch_path = String::from(path);
        let handle = tokio::task::spawn_blocking(move || {
            while !shutdown_clone.is_cancelled() {
                let (std_tx, std_rx) = std_mpsc::channel();
                let event_handler = move |res| { 
                    if let Err(e) = std_tx.send(res) {
                        error!("Failed to send file event: {e}");
                    };
                };
    
                let mut watcher = match recommended_watcher(event_handler) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!("Impossible de créer le watcher: {e}");
                        sleep(Duration::from_secs(1));
                        continue;
                    }
                };
    
                if let Err(e) = watcher.watch(Path::new(Path::new(&watch_path)), RecursiveMode::NonRecursive) {
                    tracing::error!("Impossible de surveiller {watch_path}: {e}");
                        sleep(Duration::from_secs(1));
                        continue;
                }
                debug!("file watcher started"); 

                // Poll avec timeout court pour pouvoir vérifier l'annulation
                loop {
                    if shutdown_clone.is_cancelled() {
                        break;
                    }
                    match std_rx.recv_timeout(Duration::from_millis(300)) {
                        Ok(event) => {
                            if tx.blocking_send(event).is_err() {
                                break; // récepteur async fermé -> on s'arrête
                            }
                        }
                        Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                debug!("file watcher stopped"); 
            }
        });

        (FileWatcher{
            // path: String::from(path),
            receiver: rx,
            shutdown: shutdown,
        }, handle)
    }
    
    pub fn shutdown(&mut self) {
        self.shutdown.cancel();
    }

    pub fn running(&self) -> bool {
        !self.shutdown.is_cancelled()
    }

    pub async fn recv(&mut self) -> Option<FileEvent> {
        match self.receiver.recv().await {
            Some(Ok(e)) if e.kind.is_create() => Some(FileEvent::Create),
            Some(Ok(e)) if e.kind.is_modify() => Some(FileEvent::Modify),
            Some(Ok(e)) if e.kind.is_remove() => Some(FileEvent::Remove),
            Some(Err(e)) => {
                error!("File watch receive error event : {e}");
                None
            },
            Some(_) => None,
            None => None,
        }
    }

}

// Démarre une tâche de fond qui scrute la ConfigMap et met à jour le cache
// dès qu'un changement de la clé `records` est détecté côté cluster.
// fn spawn_watcher(path: String, cache: &mut Arc<RwLock<Vec<Endpoint>>>) {
//     let mut cache= cache.clone();
//     debug!("Starting file watcher...");
//     tokio::spawn(async move {
//         let mut watcher: notify::INotifyWatcher;
//         let mut rx: Receiver<Result<Event, notify::Error>>;
        
//         loop {
//             let tx: Sender<Result<Event, notify::Error>>;
//             (tx, rx) = mpsc::channel::<notify::Result<Event>>();
//             watcher = match notify::recommended_watcher(tx) {
//                 Ok(i) => i,
//                 Err(e) => {
//                     error!("Failed to start watcher {e}");
//                     continue
//                 },
//             };
//             match watcher.watch(Path::new(&path), RecursiveMode::Recursive) {
//                 Ok(_) => break,
//                 Err(e) => error!("Failed to start watching {path} : {e}"),
//             }
//             sleep(Duration::from_secs(1)).await;
//         }
//         debug!("File watcher started");
//         loop {
//             let event = match rx.recv() {
//                 Ok(Ok(e)) => e,
//                 Ok(Err(e)) => {
//                     error!("receive notify event error : {e}");
//                     continue;
//                 },
//                 Err(e) => {
//                     error!("error receiving notify event : {e}");
//                     continue;
//                 },
//             };
//             match event.kind {
//                 // notify::EventKind::Create(_) => read(&path, &mut cache).await,
//                 notify::EventKind::Modify(_) => read(&path, &mut cache).await,
//                 notify::EventKind::Remove(_) => *cache.write().await =vec![],
//                 _ => {},
//             };
//         }
//     });
// }    
