//! The identity keystone at the storage layer (design v2, primary-56d1.11). The
//! central authority's two laws are proven as enforced, durable semantics:
//!
//! - Law 1 (never re-mint the same thing): the same declared schema-whole, bound twice
//!   with a fresh process and only the on-disk state in between, receives identical
//!   identities — regardless of declaration order.
//! - A differing declaration set under one handle keeps stable identities for carried
//!   declarations and mints fresh ones only for new declarations.
//! - Law 2 (never rebind an identity to a different thing): a `Continue` claiming an
//!   existing identity for a structurally different shape is rejected with a typed error.
//! - Durability: minted universes are never reused, and a restart re-issues nothing.

use sema_storage::Runtime;
use signal_sema_storage::{
    BindOutcome, DeclaredIdentity, DeclaredKey, DeclaredShape, IdentityIntent, MintedUniverse,
    Rejection, Reply, Request, SchemaWholeHandle, TypeIdentity,
};

fn mint(name: &str, shape: u8) -> DeclaredIdentity {
    DeclaredIdentity {
        key: DeclaredKey(name.as_bytes().to_vec()),
        shape: DeclaredShape([shape; 32]),
        intent: IdentityIntent::MintOrBind,
    }
}

fn whole(handle: &str) -> SchemaWholeHandle {
    SchemaWholeHandle(handle.as_bytes().to_vec())
}

async fn bind(runtime: &Runtime, handle: &str, declarations: Vec<DeclaredIdentity>) -> Reply {
    runtime
        .request(Request::BindIdentities {
            whole: whole(handle),
            declarations,
        })
        .await
        .expect("bind request")
}

/// The identity of a declaration named `name` in `reply`, and whether it was minted or
/// bound.
fn assignment(reply: &Reply, name: &str) -> (TypeIdentity, BindOutcome) {
    let Reply::IdentitiesBound(bound) = reply else {
        panic!("expected IdentitiesBound, got {reply:?}");
    };
    let key = DeclaredKey(name.as_bytes().to_vec());
    let found = bound
        .assignments
        .iter()
        .find(|assignment| assignment.key == key)
        .unwrap_or_else(|| panic!("no assignment for {name} in {bound:?}"));
    (found.identity, found.outcome)
}

fn universe(reply: &Reply) -> MintedUniverse {
    let Reply::IdentitiesBound(bound) = reply else {
        panic!("expected IdentitiesBound, got {reply:?}");
    };
    bound.universe
}

/// Law 1, the keystone: the same declared schema-whole bound twice across a fresh
/// process — only the on-disk state carried over, declarations reversed the second time
/// — receives identical universe and identities. The same thing is never re-ID'ed.
#[tokio::test]
async fn same_declared_schema_binds_identical_identities_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sema");

    let first = {
        let runtime = Runtime::open(&path).await.unwrap();
        let reply = bind(
            &runtime,
            "payments/v1",
            vec![mint("Alpha", 1), mint("Beta", 2)],
        )
        .await;
        runtime.shutdown().await.unwrap();
        reply
    };
    assert_eq!(assignment(&first, "Alpha").1, BindOutcome::Minted);
    assert_eq!(assignment(&first, "Beta").1, BindOutcome::Minted);

    // A fresh process — a new Runtime over the same on-disk state, reversed order.
    let second = {
        let runtime = Runtime::open(&path).await.unwrap();
        let reply = bind(
            &runtime,
            "payments/v1",
            vec![mint("Beta", 2), mint("Alpha", 1)],
        )
        .await;
        runtime.shutdown().await.unwrap();
        reply
    };

    assert_eq!(universe(&first), universe(&second), "same universe rebound");
    assert_eq!(
        assignment(&first, "Alpha").0,
        assignment(&second, "Alpha").0,
        "Alpha keeps its one identity",
    );
    assert_eq!(
        assignment(&first, "Beta").0,
        assignment(&second, "Beta").0,
        "Beta keeps its one identity",
    );
    // The second binding re-bound, it did not re-mint.
    assert_eq!(assignment(&second, "Alpha").1, BindOutcome::Bound);
    assert_eq!(assignment(&second, "Beta").1, BindOutcome::Bound);
}

/// A differing declaration set under one handle: carried declarations keep their
/// identities; only genuinely new declarations mint fresh, never-reused ones.
#[tokio::test]
async fn carried_declarations_are_stable_and_only_new_ones_mint() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(&dir.path().join("authority.sema"))
        .await
        .unwrap();

    let base = bind(&runtime, "catalog", vec![mint("Alpha", 1), mint("Beta", 2)]).await;
    let alpha = assignment(&base, "Alpha").0;

    // Drop Beta, keep Alpha, add Gamma.
    let evolved = bind(
        &runtime,
        "catalog",
        vec![mint("Alpha", 1), mint("Gamma", 3)],
    )
    .await;

    assert_eq!(
        assignment(&evolved, "Alpha"),
        (alpha, BindOutcome::Bound),
        "Alpha keeps its identity",
    );
    let (gamma, gamma_outcome) = assignment(&evolved, "Gamma");
    assert_eq!(gamma_outcome, BindOutcome::Minted, "Gamma is a new thing");
    assert_eq!(
        gamma,
        TypeIdentity(2),
        "Gamma mints the next never-reused local after Alpha(0), Beta(1)",
    );
}

/// Law 2: a `Continue` that claims an existing identity for a structurally different
/// shape is rejected with a typed error. An identity names exactly one thing.
#[tokio::test]
async fn rebinding_an_identity_to_a_different_shape_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(&dir.path().join("authority.sema"))
        .await
        .unwrap();

    let minted = bind(&runtime, "ledger", vec![mint("Alpha", 1)]).await;
    let alpha = assignment(&minted, "Alpha").0;

    // Claim Alpha's identity for a thing of a different shape (2 != 1).
    let attempt = bind(
        &runtime,
        "ledger",
        vec![DeclaredIdentity {
            key: DeclaredKey(b"Impostor".to_vec()),
            shape: DeclaredShape([2; 32]),
            intent: IdentityIntent::Continue(alpha),
        }],
    )
    .await;

    match attempt {
        Reply::Rejected(Rejection::IdentityRebindRejected {
            identity,
            bound_shape,
            attempted_shape,
        }) => {
            assert_eq!(identity, alpha);
            assert_eq!(bound_shape, DeclaredShape([1; 32]));
            assert_eq!(attempted_shape, DeclaredShape([2; 32]));
        }
        other => panic!("expected a law-2 IdentityRebindRejected, got {other:?}"),
    }
}

/// A distinct schema-whole after a restart mints a distinct, never-reused universe —
/// the universe cursor is durable, not reset on reopen.
#[tokio::test]
async fn distinct_wholes_mint_distinct_universes_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sema");

    let first = {
        let runtime = Runtime::open(&path).await.unwrap();
        let reply = bind(&runtime, "alpha-whole", vec![mint("One", 1)]).await;
        runtime.shutdown().await.unwrap();
        reply
    };
    let second = {
        let runtime = Runtime::open(&path).await.unwrap();
        let reply = bind(&runtime, "beta-whole", vec![mint("One", 1)]).await;
        runtime.shutdown().await.unwrap();
        reply
    };

    assert_ne!(
        universe(&first),
        universe(&second),
        "a distinct whole never reuses a minted universe id",
    );
}

/// An empty declaration set is rejected — the whole-level analogue of a zero count.
#[tokio::test]
async fn empty_declaration_set_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(&dir.path().join("authority.sema"))
        .await
        .unwrap();
    let reply = bind(&runtime, "empty", Vec::new()).await;
    assert!(matches!(
        reply,
        Reply::Rejected(Rejection::EmptyDeclarationSet)
    ));
}
