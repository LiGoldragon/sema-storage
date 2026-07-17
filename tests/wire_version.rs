use sema_storage::NegotiatedWire;
use signal_frame::{ProtocolVersion, SIGNAL_FRAME_PROTOCOL_VERSION};
use signal_sema_storage::Rejection;

#[test]
fn current_wire_version_is_served_and_incompatible_is_rejected() {
    let current = NegotiatedWire::new(SIGNAL_FRAME_PROTOCOL_VERSION);
    assert!(current.is_compatible());
    assert_eq!(current.request_rejection(), None);

    let incompatible = NegotiatedWire::new(ProtocolVersion::new(
        SIGNAL_FRAME_PROTOCOL_VERSION.major() + 1,
        0,
        0,
    ));
    assert!(!incompatible.is_compatible());
    assert_eq!(
        incompatible.request_rejection(),
        Some(Rejection::IncompatibleWireVersion),
    );
}
