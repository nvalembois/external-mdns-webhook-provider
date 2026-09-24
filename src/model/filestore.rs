use std::{path::{Path, PathBuf}, sync::{Arc, mpsc::{Receiver, Sender}}, time::Duration};

use simple_dns::Question;
use tokio::{sync::RwLock, time::sleep};
use std::fs::File;
use std::sync::mpsc;
use notify::{Event, RecursiveMode, Watcher};
use tracing::{error, info};

use crate::model::records::Endpoint;

#[derive(Clone)]
pub struct FileStore {
    path: String,
    cache: Arc<RwLock<Vec<Endpoint>>>,
}

impl FileStore {

    pub async fn new(path: &str) -> Self {
        let mut cache = Arc::new(RwLock::new(vec![]));
        read(path, &mut cache).await;
        Self {
            path: String::from(path),
            cache: cache,
        }
    }

    /// Démarre une tâche de fond qui scrute la ConfigMap et met à jour le cache
    /// dès qu'un changement de la clé `records` est détecté côté cluster.
    pub fn spawn_watcher(&mut self) {
        let path = self.path.clone();
        let mut cache = self.cache.clone();
        
        tokio::spawn(async move {
            let mut watcher: notify::INotifyWatcher;
            let mut rx: Receiver<Result<Event, notify::Error>>;
            loop {
                let tx: Sender<Result<Event, notify::Error>>;
                (tx, rx) = mpsc::channel::<notify::Result<Event>>();
                watcher = match notify::recommended_watcher(tx) {
                    Ok(i) => i,
                    Err(e) => {
                        error!("Failed to start watcher {e}");
                        continue
                    },
                };
                match watcher.watch(Path::new(&path), RecursiveMode::NonRecursive) {
                    Ok(_) => break,
                    Err(e) => error!("Failed to start watching {path} : {e}"),
                }
                sleep(Duration::from_secs(1)).await;
            }

            loop {
                let event = match rx.recv() {
                    Ok(Ok(e)) => e,
                    Ok(Err(e)) => {
                        error!("receive notify event error : {e}");
                        continue;
                    },
                    Err(e) => {
                        error!("error receiving notify event : {e}");
                        continue;
                    },
                };
                match event.kind {
                    // notify::EventKind::Create(_) => read(&path, &mut cache).await,
                    notify::EventKind::Modify(_) => read(&path, &mut cache).await,
                    notify::EventKind::Remove(_) => *cache.write().await =vec![],
                    _ => {},
                };
            }
        });
    }    
    
    pub async fn query<'a>(&self, question: &Question<'a>) -> Vec<String> {
        let records = self.cache.read().await;
        records.iter()
            .filter(|r| r.dns_name.to_lowercase() == question.qname.to_string().to_lowercase() && r.record_type == question.qtype)
            .map(|r| r.targets.clone())
            .flatten()
            .collect()
    }

}

async fn read(path: &str, cache: &mut Arc<RwLock<Vec<Endpoint>>>) {
    let file = match File::open(PathBuf::from(path)) {
        Ok(f) => f,
        Err(e) => {
            error!("error opening file '{0}' : {e}", path);
            return ;
        },
    };
    let records:Vec<Endpoint> = match serde_json::from_reader(file) {
        Ok(v) => v,
        Err(e) => {
            error!("error decoding file '{0}' : {e}", path);
            return ;
        },
    };
    info!("loaded data from {path} and found {} records", records.capacity());
    *cache.write().await = records;
}
