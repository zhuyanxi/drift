mod backend;
mod croc;
mod native;
mod wire;

pub use backend::{
    BackendAvailability, BackendCancellation, BackendCapabilities, BackendCapability,
    BackendControlResult, BackendError, BackendEvent, BackendInfo, BackendOperationError,
    BackendProtocolError, BackendRequestError, BackendUnavailableReason, ReceiveRequest,
    SendRequest, TransferBackend, TransferHandle,
};
pub use croc::{
    parse_croc_line, parse_croc_version, CrocBackend, CrocParseError, CrocVersion,
    SUPPORTED_CROC_VERSION_RANGE,
};
pub use native::{NativeBackend, NATIVE_PROTOCOL_VERSION};
pub use wire::{
    AuthMessageKind, CapabilityOffer, FrameDecoder, FrameHeader, FramePayload, KnownCapability,
    MessageClass, MessageType, ProtocolPhase, WireCodec, WireError, WireFrame, WireId, WireLimits,
    WireMessage, WireRole, WireState, WireVersion, OPTIONAL_MESSAGE_BIT, WIRE_HEADER_LEN,
    WIRE_MAGIC, WIRE_VERSION,
};
