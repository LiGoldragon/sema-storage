use kameo::{
    Actor,
    actor::{ActorRef, Spawn},
    message::{Context, Message},
};
use rkyv::{Archive, Deserialize, Serialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference, VersionedStoreName,
    VersioningPolicy,
};
use signal_sema_storage::{
    ChangeEvent, DocumentKey, DocumentKind, FixtureScope, IdentifierBlock, Rejection, Reply,
    Request, SlotSummary, Snapshot, StoredDocument, SubscriptionIdentifier, Version,
};
use std::{convert::Infallible, path::Path};
use tokio::sync::broadcast;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(String),
    #[error("actor: {0}")]
    Actor(String),
}
type Result<T> = std::result::Result<T, Error>;

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
enum StorageRecord {
    Document(StoredDocument),
    Allocator { scope: FixtureScope, next: u32 },
}
impl EngineRecord for StorageRecord {
    fn record_key(&self) -> RecordKey {
        match self {
            Self::Document(d) => RecordKey::new(document_record_key(&d.key, d.version)),
            Self::Allocator { scope, .. } => RecordKey::new(format!("allocator:{}", scope.0)),
        }
    }
}
fn kind_tag(kind: DocumentKind) -> u8 {
    match kind {
        DocumentKind::TypeSchema => 0,
        DocumentKind::SignalContract => 1,
        DocumentKind::NexusRuntime => 2,
        DocumentKind::SemaStorage => 3,
        DocumentKind::Nomos => 4,
        DocumentKind::Logos => 5,
    }
}
fn document_record_key(key: &DocumentKey, version: Version) -> String {
    format!(
        "doc:{}:{}:{}:{}",
        key.scope.0,
        kind_tag(key.kind),
        key.slot.0,
        version.0
    )
}

pub struct SemaPlane {
    engine: Engine,
    records: TableReference<StorageRecord>,
}
impl SemaPlane {
    pub fn open(path: &Path) -> Result<Self> {
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(1)).with_versioning(VersioningPolicy::new(
                VersionedStoreName::new("language-engine-prototype"),
            )),
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
        let records = engine
            .register_table(TableDescriptor::new(
                TableName::new("language_engine_records"),
                FamilyName::new("language-engine-record"),
                SchemaHash::for_label("language-engine-record-v1"),
            ))
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { engine, records })
    }
    fn all(&self) -> Result<Vec<StorageRecord>> {
        self.engine
            .match_records(QueryPlan::all(self.records))
            .map(|s| s.records().to_vec())
            .map_err(|e| Error::Storage(e.to_string()))
    }
    fn documents(&self) -> Result<Vec<StoredDocument>> {
        Ok(self
            .all()?
            .into_iter()
            .filter_map(|r| match r {
                StorageRecord::Document(d) => Some(d),
                _ => None,
            })
            .collect())
    }
    fn latest_for(&self, key: &DocumentKey) -> Result<Option<StoredDocument>> {
        Ok(self
            .documents()?
            .into_iter()
            .filter(|d| d.key == *key)
            .max_by_key(|d| d.version))
    }
    fn dispatch(&mut self, request: Request) -> Result<(Reply, Option<ChangeEvent>)> {
        match request {
            Request::Store { key, payload } => {
                if key.kind != payload.kind() {
                    return Ok((Reply::Rejected(Rejection::InvalidKind), None));
                }
                if let Err(violation) = payload.validate() {
                    return Ok((Reply::Rejected(Rejection::InvalidDocument(violation)), None));
                }
                let version = Version(self.latest_for(&key)?.map_or(1, |d| d.version.0 + 1));
                let hash = payload
                    .content_hash()
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let stored = StoredDocument {
                    key: key.clone(),
                    version,
                    hash,
                    payload,
                };
                let receipt = self
                    .engine
                    .assert(Assertion::new(
                        self.records,
                        StorageRecord::Document(stored.clone()),
                    ))
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let summary = SlotSummary { key, version, hash };
                let event = ChangeEvent {
                    subscription: SubscriptionIdentifier(0),
                    snapshot: Snapshot(receipt.snapshot().value()),
                    document: summary.clone(),
                };
                Ok((Reply::Stored(summary), Some(event)))
            }
            Request::Fetch { key, version } => {
                let value = match version {
                    Some(v) => self
                        .documents()?
                        .into_iter()
                        .find(|d| d.key == key && d.version == v),
                    None => self.latest_for(&key)?,
                };
                Ok((Reply::Document(value), None))
            }
            Request::List { scope, kind } => {
                let mut docs = self.documents()?;
                docs.sort_by_key(|d| (kind_tag(d.key.kind), d.key.slot, d.version));
                let values = docs
                    .into_iter()
                    .filter(|d| d.key.scope == scope && kind.is_none_or(|k| k == d.key.kind))
                    .map(|d| SlotSummary {
                        key: d.key,
                        version: d.version,
                        hash: d.hash,
                    })
                    .collect();
                Ok((Reply::Listed(values), None))
            }
            Request::HashFetch { hash } => Ok((
                Reply::Document(self.documents()?.into_iter().find(|d| d.hash == hash)),
                None,
            )),
            Request::Snapshot { .. } => self
                .engine
                .latest_snapshot()
                .map(|s| (Reply::Snapshotted(Snapshot(s.value())), None))
                .map_err(|e| Error::Storage(e.to_string())),
            Request::Subscribe { scope, kind } => {
                let (initial, _) = self.dispatch(Request::List { scope, kind })?;
                let Reply::Listed(initial) = initial else {
                    unreachable!()
                };
                Ok((
                    Reply::Subscribed {
                        identifier: SubscriptionIdentifier(0),
                        initial,
                    },
                    None,
                ))
            }
            Request::AllocateIdentifiers { scope, count } => {
                if count == 0 {
                    return Ok((Reply::Rejected(Rejection::CountZero), None));
                }
                let records = self.all()?;
                let current = records
                    .iter()
                    .find_map(|r| match r {
                        StorageRecord::Allocator { scope: s, next } if *s == scope => Some(*next),
                        _ => None,
                    })
                    .unwrap_or(0);
                let updated = StorageRecord::Allocator {
                    scope,
                    next: current
                        .checked_add(count)
                        .ok_or_else(|| Error::Storage("identifier overflow".into()))?,
                };
                if current == 0 {
                    self.engine
                        .assert(Assertion::new(self.records, updated))
                        .map_err(|e| Error::Storage(e.to_string()))?;
                } else {
                    self.engine
                        .mutate(Mutation::new(self.records, updated))
                        .map_err(|e| Error::Storage(e.to_string()))?;
                }
                Ok((
                    Reply::IdentifiersAllocated(IdentifierBlock {
                        first: current,
                        length: count,
                    }),
                    None,
                ))
            }
        }
    }
}
impl Actor for SemaPlane {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(
        actor: Self::Args,
        _: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}
pub struct Execute(pub Request);
impl Message<Execute> for SemaPlane {
    type Reply = Result<(Reply, Option<ChangeEvent>)>;
    async fn handle(&mut self, msg: Execute, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.dispatch(msg.0)
    }
}

pub struct NexusPlane {
    sema: ActorRef<SemaPlane>,
    events: broadcast::Sender<ChangeEvent>,
    dispatched: u64,
}
impl NexusPlane {
    fn new(sema: ActorRef<SemaPlane>, events: broadcast::Sender<ChangeEvent>) -> Self {
        Self {
            sema,
            events,
            dispatched: 0,
        }
    }
}
impl Actor for NexusPlane {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(
        actor: Self::Args,
        _: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}
impl Message<Execute> for NexusPlane {
    type Reply = Result<Reply>;
    async fn handle(&mut self, msg: Execute, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.dispatched += 1;
        let (reply, event) = self
            .sema
            .ask(msg)
            .send()
            .await
            .map_err(|e| Error::Actor(e.to_string()))?;
        if let Some(event) = event {
            let _ = self.events.send(event);
        }
        Ok(reply)
    }
}

pub struct SignalPlane {
    nexus: ActorRef<NexusPlane>,
    admitted: u64,
}
impl SignalPlane {
    fn new(nexus: ActorRef<NexusPlane>) -> Self {
        Self { nexus, admitted: 0 }
    }
}
impl Actor for SignalPlane {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(
        actor: Self::Args,
        _: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}
impl Message<Execute> for SignalPlane {
    type Reply = Result<Reply>;
    async fn handle(&mut self, msg: Execute, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.admitted += 1;
        self.nexus
            .ask(msg)
            .send()
            .await
            .map_err(|e| Error::Actor(e.to_string()))
    }
}

#[derive(Clone)]
pub struct Runtime {
    signal: ActorRef<SignalPlane>,
    nexus: ActorRef<NexusPlane>,
    sema: ActorRef<SemaPlane>,
    events: broadcast::Sender<ChangeEvent>,
}
impl Runtime {
    pub async fn open(path: &Path) -> Result<Self> {
        let sema = SemaPlane::spawn(SemaPlane::open(path)?);
        let (events, _) = broadcast::channel(64);
        let nexus = NexusPlane::spawn(NexusPlane::new(sema.clone(), events.clone()));
        let signal = SignalPlane::spawn(SignalPlane::new(nexus.clone()));
        Ok(Self {
            signal,
            nexus,
            sema,
            events,
        })
    }
    pub async fn request(&self, request: Request) -> Result<Reply> {
        self.signal
            .ask(Execute(request))
            .send()
            .await
            .map_err(|e| Error::Actor(e.to_string()))
    }
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.events.subscribe()
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.signal
            .stop_gracefully()
            .await
            .map_err(|e| Error::Actor(e.to_string()))?;
        self.signal.wait_for_shutdown().await;
        self.nexus
            .stop_gracefully()
            .await
            .map_err(|e| Error::Actor(e.to_string()))?;
        self.nexus.wait_for_shutdown().await;
        self.sema
            .stop_gracefully()
            .await
            .map_err(|e| Error::Actor(e.to_string()))?;
        self.sema.wait_for_shutdown().await;
        Ok(())
    }
}
