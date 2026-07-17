use kameo::{
    Actor,
    actor::{ActorRef, Spawn},
    message::{Context, Message},
};
use rkyv::{Archive, Deserialize, Serialize};
use sema_engine::{
    Assertion, CommitRequest, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan,
    RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_frame::{ProtocolVersion, SIGNAL_FRAME_PROTOCOL_VERSION};
use signal_sema_storage::{
    BindOutcome, BoundIdentities, ChangeEvent, DeclaredIdentity, DeclaredKey, DeclaredShape,
    DocumentKey, DocumentKind, FixtureScope, IdentifierBlock, IdentityAssignment, IdentityIntent,
    MintedUniverse, Rejection, Reply, Request, SchemaWholeHandle, SlotSummary, Snapshot,
    StoredDocument, SubscriptionIdentifier, TypeIdentity, Version,
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

/// The shared-frame protocol version a connecting peer declared at handshake,
/// paired with the daemon's authority to serve requests under it.
///
/// The daemon negotiates the `signal-frame` protocol version once per connection.
/// Every later request on that connection is answerable only while the peer's
/// version stays compatible with the daemon's own; an incompatible peer is
/// answered with the typed [`Rejection::IncompatibleWireVersion`] rather than
/// dispatched. This keeps the daemon's version surface enforced instead of merely
/// declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegotiatedWire {
    peer: ProtocolVersion,
}
impl NegotiatedWire {
    pub fn new(peer: ProtocolVersion) -> Self {
        Self { peer }
    }
    /// Whether the daemon's own wire version can serve this peer.
    pub fn is_compatible(self) -> bool {
        SIGNAL_FRAME_PROTOCOL_VERSION.accepts(self.peer)
    }
    /// The typed rejection to answer an incoming request with when the peer's wire
    /// version is incompatible, or `None` when the daemon can serve it.
    pub fn request_rejection(self) -> Option<Rejection> {
        (!self.is_compatible()).then_some(Rejection::IncompatibleWireVersion)
    }
}

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

/// One durable record of the central identity authority (design v2, primary-56d1.11).
/// It lives in its own table so the document/allocator schema is untouched, and gets
/// the same engine-backed durability and recovery guarantee as documents: a restart
/// replays it, so identities are never lost or re-issued.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
enum AuthorityRecord {
    /// The single schema-whole→universe registry and the never-reused universe cursor.
    UniverseRegistry(UniverseRegistry),
    /// One universe's type-identity bindings and its never-reused local cursor.
    UniverseState(UniverseState),
}
impl EngineRecord for AuthorityRecord {
    fn record_key(&self) -> RecordKey {
        match self {
            Self::UniverseRegistry(_) => RecordKey::new("universe-registry".to_string()),
            Self::UniverseState(state) => {
                RecordKey::new(format!("universe-state:{}", state.universe))
            }
        }
    }
}

/// The schema-whole→universe registry: every whole handle the authority has minted a
/// universe for, and the monotonic cursor. A universe id is never reused (law 1 at the
/// whole level: the same whole keeps its one universe forever).
#[derive(Archive, Serialize, Deserialize, Clone, Debug, Default)]
struct UniverseRegistry {
    next_universe: u32,
    wholes: Vec<WholeBinding>,
}
impl UniverseRegistry {
    /// The universe for `whole`, minting a fresh never-reused one when unseen. Returns
    /// the universe and whether a mint occurred (so the caller persists the change only
    /// when the registry actually moved).
    fn resolve_or_mint(&mut self, whole: &SchemaWholeHandle) -> Result<(u32, bool)> {
        if let Some(binding) = self.wholes.iter().find(|binding| &binding.handle == whole) {
            return Ok((binding.universe, false));
        }
        let universe = self.next_universe;
        self.next_universe = self
            .next_universe
            .checked_add(1)
            .ok_or_else(|| Error::Storage("universe identifier space exhausted".into()))?;
        self.wholes.push(WholeBinding {
            handle: whole.clone(),
            universe,
        });
        Ok((universe, true))
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
struct WholeBinding {
    handle: SchemaWholeHandle,
    universe: u32,
}

/// One universe's identity bindings: each declared thing's key, the local identity it
/// holds, and its recorded structural shape, plus the monotonic local cursor. A local
/// id is never reused within the universe.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
struct UniverseState {
    universe: u32,
    next_local: u32,
    bindings: Vec<TypeBinding>,
}
impl UniverseState {
    fn empty(universe: u32) -> Self {
        Self {
            universe,
            next_local: 0,
            bindings: Vec::new(),
        }
    }

    /// Apply one declaration's bind-or-mint against this universe, enforcing the two
    /// laws. Law 1 — a `MintOrBind` never re-mints for a key it already bound; the same
    /// declared thing keeps its one identity, and a shape change under it is a §3
    /// version-advance that keeps the id. Law 2 — a `Continue` may only rebind an
    /// identity that already names a thing of the same shape; claiming an identity that
    /// names a structurally different thing is a rebind and is rejected.
    fn bind_or_mint(
        &mut self,
        declaration: &DeclaredIdentity,
    ) -> std::result::Result<IdentityAssignment, Rejection> {
        match declaration.intent {
            IdentityIntent::MintOrBind => {
                if let Some(binding) = self
                    .bindings
                    .iter_mut()
                    .find(|binding| binding.key == declaration.key)
                {
                    binding.shape = declaration.shape;
                    return Ok(IdentityAssignment {
                        key: declaration.key.clone(),
                        identity: TypeIdentity(binding.identity),
                        outcome: BindOutcome::Bound,
                    });
                }
                let identity = self.mint_local()?;
                self.bindings.push(TypeBinding {
                    key: declaration.key.clone(),
                    identity,
                    shape: declaration.shape,
                });
                Ok(IdentityAssignment {
                    key: declaration.key.clone(),
                    identity: TypeIdentity(identity),
                    outcome: BindOutcome::Minted,
                })
            }
            IdentityIntent::Continue(claimed) => {
                let binding = self
                    .bindings
                    .iter_mut()
                    .find(|binding| binding.identity == claimed.0)
                    .ok_or(Rejection::IdentityNeverMinted(claimed))?;
                if binding.shape != declaration.shape {
                    return Err(Rejection::IdentityRebindRejected {
                        identity: claimed,
                        bound_shape: binding.shape,
                        attempted_shape: declaration.shape,
                    });
                }
                binding.key = declaration.key.clone();
                Ok(IdentityAssignment {
                    key: declaration.key.clone(),
                    identity: claimed,
                    outcome: BindOutcome::Bound,
                })
            }
        }
    }

    fn mint_local(&mut self) -> std::result::Result<u32, Rejection> {
        let identity = self.next_local;
        self.next_local = self.next_local.checked_add(1).ok_or(Rejection::Internal)?;
        Ok(identity)
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
struct TypeBinding {
    key: DeclaredKey,
    identity: u32,
    shape: DeclaredShape,
}

pub struct SemaPlane {
    engine: Engine,
    records: TableReference<StorageRecord>,
    authority: TableReference<AuthorityRecord>,
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
        let authority = engine
            .register_table(TableDescriptor::new(
                TableName::new("identity_authority_records"),
                FamilyName::new("identity-authority-record"),
                SchemaHash::for_label("identity-authority-record-v1"),
            ))
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self {
            engine,
            records,
            authority,
        })
    }

    fn authority_records(&self) -> Result<Vec<AuthorityRecord>> {
        self.engine
            .match_records(QueryPlan::all(self.authority))
            .map(|matched| matched.records().to_vec())
            .map_err(|e| Error::Storage(e.to_string()))
    }

    fn universe_registry(&self) -> Result<Option<UniverseRegistry>> {
        Ok(self
            .authority_records()?
            .into_iter()
            .find_map(|record| match record {
                AuthorityRecord::UniverseRegistry(registry) => Some(registry),
                AuthorityRecord::UniverseState(_) => None,
            }))
    }

    fn universe_state(&self, universe: u32) -> Result<Option<UniverseState>> {
        Ok(self
            .authority_records()?
            .into_iter()
            .find_map(|record| match record {
                AuthorityRecord::UniverseState(state) if state.universe == universe => Some(state),
                _ => None,
            }))
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
            Request::BindIdentities {
                whole,
                declarations,
            } => {
                if declarations.is_empty() {
                    return Ok((Reply::Rejected(Rejection::EmptyDeclarationSet), None));
                }
                let existing_registry = self.universe_registry()?;
                let registry_existed = existing_registry.is_some();
                let mut registry = existing_registry.unwrap_or_default();
                let (universe, minted_universe) = registry.resolve_or_mint(&whole)?;

                let existing_state = self.universe_state(universe)?;
                let state_existed = existing_state.is_some();
                let mut state = existing_state.unwrap_or_else(|| UniverseState::empty(universe));

                let mut assignments = Vec::with_capacity(declarations.len());
                for declaration in &declarations {
                    match state.bind_or_mint(declaration) {
                        Ok(assignment) => assignments.push(assignment),
                        // Reject without persisting: the two laws hold by writing nothing
                        // on the error path, so on-disk identities are never disturbed.
                        Err(rejection) => return Ok((Reply::Rejected(rejection), None)),
                    }
                }

                // Persist registry (only when it moved) and state as one atomic commit —
                // the same single-commit durability documents get, so a restart never
                // loses or re-issues an identity.
                let mut commit = CommitRequest::new(self.authority);
                if minted_universe {
                    let registry = AuthorityRecord::UniverseRegistry(registry);
                    commit = if registry_existed {
                        commit.mutate(registry)
                    } else {
                        commit.assert(registry)
                    };
                }
                let state = AuthorityRecord::UniverseState(state);
                commit = if state_existed {
                    commit.mutate(state)
                } else {
                    commit.assert(state)
                };
                self.engine
                    .commit(commit)
                    .map_err(|e| Error::Storage(e.to_string()))?;

                Ok((
                    Reply::IdentitiesBound(BoundIdentities {
                        universe: MintedUniverse(universe),
                        assignments,
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
