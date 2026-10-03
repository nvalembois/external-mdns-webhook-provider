
use simple_dns::Question;
use std::{fs, path::PathBuf};
use std::sync::Arc;
use std::fs::File;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::model::records::Endpoint;

#[derive(Clone)]
pub struct FileStore {
    path: String,
    cache: Arc<RwLock<Vec<Endpoint>>>,
}

impl FileStore {

    pub async fn new(path: &str) -> Self {
        let store = Self {
            path: String::from(path),
            cache: Arc::new(RwLock::new(vec![])),
        };
        store.refresh().await;
        store
    }

   
    pub async fn query<'a>(&self, question: &Question<'a>) -> Vec<String> {
        let records = self.cache.read().await;
        records.iter()
            .filter(|r| r.dns_name.to_lowercase() == question.qname.to_string().to_lowercase() && r.record_type == question.qtype)
            .map(|r| r.targets.clone())
            .flatten()
            .collect()
    }

    pub async fn refresh(&self) {
        match fs::metadata(self.path.as_str()) {
            Ok(m) if 0 == m.len() => {
                info!("ignore reading empty file");
                return ;
            },
            Err(e) => {
                error!("error getting file stat for'{0}' : {e}", self.path);
                return ;
            },
            Ok(_) => {},
        };
        let file = match File::open(PathBuf::from(self.path.as_str())) {
            Ok(f) => f,
            Err(e) => {
                error!("error opening file '{0}' : {e}", self.path);
                return ;
            },
        };
        let records:Vec<Endpoint> = match serde_json::from_reader(file) {
            Ok(v) => v,
            Err(e) => {
                error!("error decoding file '{0}' : {e}", self.path);
                return ;
            },
        };
        info!("loaded data from {} and found {} records", self.path, records.capacity());
        *self.cache.write().await = records;
    }
    
    pub async fn clear(&self) {
        *self.cache.write().await = vec![];
    }
    
}
