use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    api::{Api, ObjectMeta, Patch, PatchParams, PostParams},
    runtime::{watcher, WatchStreamExt},
    Client, Error as KubeError,
};
use serde::{Deserialize, Serialize};
use simple_dns::Question;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::model::records::{Changes, Endpoint};

/// Store synchronisé pour la ConfigMap contenant la clé `records` :
/// - crée la CM si absente, avec `records: "[]"`
/// - watch en continu et rafraîchit le cache local (HashMap host -> ip)
/// - expose lecture/écriture thread-safe
#[derive(Clone)]
pub struct ConfigMapStore {
    api: Api<ConfigMap>,
    name: String,
    key: String,
    cache: Arc<RwLock<Vec<Endpoint>>>,
}
#[derive(Serialize, Deserialize,Debug)]
struct ConfigMapPatch {
    data: std::collections::BTreeMap<String, String>,
}

impl ConfigMapStore {
    /// Récupère la ConfigMap si elle existe, sinon la crée avec `records: ''`.
    /// Parse la clé `records` pour initialiser le cache local.
    pub async fn new(namespace: &str, name: &str, key: &str) -> Result<Self, KubeError> {
        let client = Client::try_default().await?;
        let api_namespace = match namespace.is_empty() {
            true => &client.default_namespace().to_string(),
            false => namespace,
        };
        let api: Api<ConfigMap> = Api::namespaced(client, api_namespace);

        let raw_data = match api.get(name).await {
            Ok(cm) => {
                info!(%name, "ConfigMap trouvée, chargement du cache initial");
                cm.data.unwrap_or_default()
            }
            Err(KubeError::Api(ae)) if ae.code == 404 => {
                info!(%name, "ConfigMap absente, création avec records vide");
                let mut data = std::collections::BTreeMap::new();
                data.insert(key.to_string(), "".to_string());

                let cm = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(name.to_string()),
                        namespace: Some(namespace.to_string()),
                        ..Default::default()
                    },
                    data: Some(data),
                    ..Default::default()
                };
                let created = api.create(&PostParams::default(), &cm).await?;
                created.data.unwrap_or_default()
            }
            Err(e) => return Err(e),
        };

        Ok(Self {
            api,
            name: name.to_string(),
            key: key.to_string(),
            cache: Arc::new(RwLock::new(decode_cm_data(key, raw_data))),
        })
    }

    async fn update_cm(&self) -> Result<(), KubeError> {
        let records = &(*self.cache.read().await);
        let records = match serde_json::to_string(records) {
            Ok(s) => s,
            Err(e) => {
                error!("error encoding cm data : {e}");
                String::new()
            }
        };
        let data = BTreeMap::from([(self.key.clone(), records)]);
        match self.api.get_metadata_opt(&self.name).await? {
            Some(_) => {
                let patch = Patch::Merge(ConfigMapPatch{data: data});
                let params = PatchParams::default();
                self.api.patch(&self.name, &params, &patch).await.map(|_| ())
            },
            None => {
                info!("ConfigMap absente, création");
                let cm = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(self.name.clone()),
                        namespace: self.api.namespace().map(|s| s.to_string()),
                        ..Default::default()
                    },
                    data: Some(data),
                    ..Default::default()
                };
                self.api.create(&PostParams::default(), &cm).await.map(|_| ())
            }
        }
    }

    /// Démarre une tâche de fond qui scrute la ConfigMap et met à jour le cache
    /// dès qu'un changement de la clé `records` est détecté côté cluster.
    pub fn spawn_watcher(&self) {
        let api = self.api.clone();
        let cache = self.cache.clone();
        let name = self.name.clone();
        let key = self.key.clone();

        tokio::spawn(async move {
            let wc = watcher::Config::default().fields(&format!("metadata.name={name}"));
            let mut stream = watcher(api, wc).default_backoff().boxed();

            loop {
                match stream.next().await {
                    Some(Ok(watcher::Event::Apply(cm) | watcher::Event::InitApply(cm))) => {
                        let raw_data = cm.data.unwrap_or_default();
                        *cache.write().await = decode_cm_data(&key, raw_data);
                        info!(%name, "cache records mis à jour (watch)");
                    }
                    Some(Ok(watcher::Event::Delete(_))) => {
                        warn!(%name, "ConfigMap supprimée, cache records vidé");
                        *cache.write().await = vec![];
                    }
                    Some(Ok(event)) => debug!("ignore cm event {:?}", event),
                    Some(Err(e)) => error!(%name, error = %e, "erreur watch"),
                    None => {
                        warn!(%name, "flux de watch terminé");
                        break;
                    }
                }
            }
        });
    }    
    
    /// Copie complète du cache local courant.
    pub async fn into_endpoints(&self) -> Vec<Endpoint> {
        self.cache.read().await.clone().into()
    }

    pub async fn appy_changes(&self, changes: &mut Changes) -> Result<(), String> {

        if self.apply_changes_internal(changes).await {
            return self.update_cm().await.map_err(|e| e.to_string())
        }

        Ok(())
    }

    async fn apply_changes_internal(&self, changes: &mut Changes) -> bool {
        // get write exclusive access to cache
        let mut records = self.cache.write().await;
        let mut changed = false;
        
        // Add create items to records
        if let Some(ref mut creates) = changes.create {
            records.append(creates);
            changed = true;
        }
        
        // TODO:: updates_new/updates_old
        // for record in changes.update_new.unwrap_or(vec![]) {
        //     if !VALID_RECORT_TYPES.contains(&record.record_type) { continue }
        //     for target in record.targets {
        //         let ip: IpAddr = match target.parse() {
        //             Ok(v) => v,
        //             Err(_) => {
        //                 info!("Skip line with unparseable ip : {line}");
        //                 continue
        //             }
        //         };
        //         changed |= records.insert(&record.dns_name, ip);
        //     }
        // }
        // // Replace from update_old by update_new
        // if let Some(new_records) = &changes.update_new {
        //     if let Some(old_records) = &changes.update_old {
        //         let mut old_record_iter = old_records.iter();
        //         for new_record in new_records {
        //             if let Some(old_record) = old_record_iter.next() {
        //                 if old_record.dns_name != new_record.dns_name {
        //                     warn!("skip replace for records {:?} -> {:?}", old_record, new_record);
        //                     continue;
        //                 }
        //                 match host_records.get_mut(&old_record.dns_name) {
        //                     Some(ips) => {
        //                         ips.retain(|ip| !old_record.targets.contains(&ip));
        //                         for ip in &new_record.targets {
        //                             if !ips.insert(ip.clone()) {
        //                                 warn!("records {} all_ready contains {ip}", new_record.dns_name);
        //                             }
        //                         }
        //                     }
        //                     None => { warn!("replace {} isn't in records", &old_record.dns_name);
        //                     if let Some(e) = host_records.insert(
        //                         new_record.dns_name.clone(),
        //                         new_record.targets.clone().into_iter().collect()) {
        //                             warn!("cannot add {new_record:?} : {e:?}");
        //                         }
        //                     }
        //                 }
        //             } else {
        //                 warn!("Cannot iterate on old_records");
        //             }
        //         }
        //     } else {
        //         warn!("No changes.OldRecords and Some(changes.NewRecords)");
        //     }
        // } else if let Some(_) = &changes.update_old {
        //     warn!("No changes.NewRecords and Some(changes.OldRecords)");
        // }
        
        // Remove delete items
        if let Some(ref mut deletes) = changes.delete {
            for record in deletes {
                let size = records.capacity();
                records.retain(|r| r != record);
                if size != records.capacity() {
                    changed = true;
                }
            }
        }

        changed
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

fn decode_cm_data(key: &str, raw_data: BTreeMap<String, String>) -> Vec<Endpoint> {
    let records = match raw_data.get(key) {
        Some(s) => s.as_str(),
        None => "",
    };
        
    match serde_json::from_str(records) {
        Ok(r) => r,
        Err(e) => {
            error!("error decoding cm data : {e}");
            vec![]
        }
    }
}
