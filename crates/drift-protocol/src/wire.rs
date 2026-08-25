use std::{fmt, io};

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const WIRE_MAGIC: [u8; 4] = *b"DRFT";
pub const WIRE_HEADER_LEN: usize = 28;
pub const OPTIONAL_MESSAGE_BIT: u8 = 0x80;
pub const WIRE_VERSION: WireVersion = WireVersion { major: 1, minor: 0 };

const DEFAULT_MAX_FRAME_PAYLOAD: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_OPAQUE_PAYLOAD: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_CAPABILITIES: usize = 64;
const DEFAULT_MAX_BUFFERED_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WireVersion {
    pub major: u8,
    pub minor: u8,
}

impl WireVersion {
    pub const fn new(major: u8, minor: u8) -> Self {
        Self { major, minor }
    }

    pub const fn same_major(self, other: Self) -> bool {
        self.major == other.major
    }

    pub fn negotiate(self, peer: Self) -> Result<Self, WireError> {
        if !self.same_major(peer) {
            return Err(WireError::UnsupportedVersion {
                found: peer,
                supported_major: self.major,
            });
        }
        Ok(Self::new(self.major, self.minor.min(peer.minor)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireLimits {
    pub max_frame_payload: usize,
    pub max_opaque_payload: usize,
    pub max_capabilities: usize,
    pub max_buffered_bytes: usize,
}

impl Default for WireLimits {
    fn default() -> Self {
        Self {
            max_frame_payload: DEFAULT_MAX_FRAME_PAYLOAD,
            max_opaque_payload: DEFAULT_MAX_OPAQUE_PAYLOAD,
            max_capabilities: DEFAULT_MAX_CAPABILITIES,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
        }
    }
}

impl WireLimits {
    pub const fn new(
        max_frame_payload: usize,
        max_opaque_payload: usize,
        max_capabilities: usize,
        max_buffered_bytes: usize,
    ) -> Self {
        Self {
            max_frame_payload,
            max_opaque_payload,
            max_capabilities,
            max_buffered_bytes,
        }
    }

    fn validate(self) -> Result<(), WireError> {
        let minimum_buffer = WIRE_HEADER_LEN.saturating_add(self.max_frame_payload);
        if self.max_frame_payload > u32::MAX as usize
            || self.max_opaque_payload > self.max_frame_payload
            || self.max_buffered_bytes < minimum_buffer
        {
            return Err(WireError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WireError {
    #[error("wire limits are invalid")]
    InvalidLimits,
    #[error("wire frame is incomplete; need {needed} more bytes")]
    Incomplete { needed: usize },
    #[error("wire stream ended before frame completed")]
    Truncated,
    #[error("wire frame magic is invalid")]
    InvalidMagic,
    #[error("wire version {found:?} is unsupported; major version {supported_major} is required")]
    UnsupportedVersion {
        found: WireVersion,
        supported_major: u8,
    },
    #[error("wire frame has reserved flags {flags:#04x}")]
    ReservedFlags { flags: u8 },
    #[error("wire frame payload length {length} exceeds limit {max}")]
    FrameTooLarge { length: usize, max: usize },
    #[error("wire decoder buffer exceeds limit {max}")]
    BufferTooLarge { max: usize },
    #[error("unknown mandatory wire message type {message_type:#04x}")]
    UnknownMandatoryMessage { message_type: u8 },
    #[error("known wire message type {message_type:?} may not be marked optional")]
    KnownMessageMarkedOptional { message_type: MessageType },
    #[error("wire message {message_type:?} is malformed: {reason}")]
    MalformedMessage {
        message_type: MessageType,
        reason: &'static str,
    },
    #[error("wire message field {field} length {length} exceeds limit {max}")]
    FieldTooLarge {
        field: &'static str,
        length: usize,
        max: usize,
    },
    #[error("wire capability {id} is required but unsupported")]
    UnsupportedCapability { id: u16 },
    #[error("wire capability {id} was required but not acknowledged")]
    MissingRequiredCapability { id: u16 },
    #[error("wire capability {id} was offered more than once")]
    DuplicateCapability { id: u16 },
    #[error("wire message contains trailing bytes")]
    TrailingBytes,
    #[error("wire message {message_type:?} is invalid in protocol phase {phase:?}")]
    InvalidState {
        phase: ProtocolPhase,
        message_type: MessageType,
    },
    #[error("wire negotiation message was duplicated")]
    DuplicateNegotiation,
    #[error("wire frame sequence {sequence} is not after previous sequence {previous}")]
    InvalidSequence { sequence: u64, previous: u64 },
    #[error("wire session identifier does not match negotiated session")]
    InvalidSession,
    #[error("wire I/O failed with {kind:?}")]
    Io { kind: io::ErrorKind },
}

impl From<io::Error> for WireError {
    fn from(error: io::Error) -> Self {
        Self::Io { kind: error.kind() }
    }
}

impl From<WireError> for crate::backend::BackendError {
    fn from(error: WireError) -> Self {
        let reason = match error {
            WireError::UnsupportedVersion { .. } => {
                crate::backend::BackendProtocolError::UnsupportedVersion
            }
            WireError::FrameTooLarge { .. }
            | WireError::FieldTooLarge { .. }
            | WireError::BufferTooLarge { .. } => {
                crate::backend::BackendProtocolError::ResourceLimit
            }
            WireError::InvalidState { .. }
            | WireError::InvalidSequence { .. }
            | WireError::InvalidSession
            | WireError::DuplicateNegotiation => crate::backend::BackendProtocolError::InvalidState,
            WireError::Io { kind } => {
                return crate::backend::BackendError::Io(io::Error::new(kind, "wire I/O failed"));
            }
            WireError::InvalidLimits
            | WireError::Incomplete { .. }
            | WireError::Truncated
            | WireError::InvalidMagic
            | WireError::ReservedFlags { .. }
            | WireError::UnknownMandatoryMessage { .. }
            | WireError::KnownMessageMarkedOptional { .. }
            | WireError::MalformedMessage { .. }
            | WireError::UnsupportedCapability { .. }
            | WireError::MissingRequiredCapability { .. }
            | WireError::DuplicateCapability { .. }
            | WireError::TrailingBytes => crate::backend::BackendProtocolError::MalformedMessage,
        };
        crate::backend::BackendError::Protocol { reason }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 1,
    HelloAck = 2,
    Auth = 3,
    SessionStart = 4,
    SessionAccept = 5,
    ManifestOffer = 6,
    ManifestAccept = 7,
    ChunkRequest = 8,
    ChunkData = 9,
    ChunkAck = 10,
    Pause = 11,
    Resume = 12,
    Cancel = 13,
    Close = 14,
    Error = 15,
}

impl MessageType {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Hello),
            2 => Some(Self::HelloAck),
            3 => Some(Self::Auth),
            4 => Some(Self::SessionStart),
            5 => Some(Self::SessionAccept),
            6 => Some(Self::ManifestOffer),
            7 => Some(Self::ManifestAccept),
            8 => Some(Self::ChunkRequest),
            9 => Some(Self::ChunkData),
            10 => Some(Self::ChunkAck),
            11 => Some(Self::Pause),
            12 => Some(Self::Resume),
            13 => Some(Self::Cancel),
            14 => Some(Self::Close),
            15 => Some(Self::Error),
            _ => None,
        }
    }

    pub const fn class(self) -> MessageClass {
        match self {
            Self::Hello | Self::HelloAck => MessageClass::Negotiation,
            Self::Auth => MessageClass::Authentication,
            Self::SessionStart | Self::SessionAccept => MessageClass::Session,
            Self::ManifestOffer | Self::ManifestAccept => MessageClass::Negotiation,
            Self::ChunkRequest | Self::ChunkData | Self::ChunkAck => MessageClass::Data,
            Self::Pause | Self::Resume => MessageClass::Control,
            Self::Cancel | Self::Close | Self::Error => MessageClass::Terminal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageClass {
    Negotiation,
    Authentication,
    Session,
    Data,
    Control,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthMessageKind {
    Init = 1,
    Response = 2,
    Confirm = 3,
}

impl AuthMessageKind {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Init),
            2 => Some(Self::Response),
            3 => Some(Self::Confirm),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireRole {
    Sender = 1,
    Receiver = 2,
}

impl WireRole {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Sender),
            2 => Some(Self::Receiver),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct WireId([u8; 16]);

impl WireId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for WireId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WireId(..)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownCapability {
    EncryptedRecords,
    Pause,
    Resume,
    Relay,
}

impl KnownCapability {
    pub const fn id(self) -> u16 {
        match self {
            Self::EncryptedRecords => 1,
            Self::Pause => 2,
            Self::Resume => 3,
            Self::Relay => 4,
        }
    }

    const fn from_id(id: u16) -> Option<Self> {
        match id {
            1 => Some(Self::EncryptedRecords),
            2 => Some(Self::Pause),
            3 => Some(Self::Resume),
            4 => Some(Self::Relay),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityOffer {
    pub id: u16,
    pub required: bool,
}

impl CapabilityOffer {
    pub const fn optional(id: u16) -> Self {
        Self {
            id,
            required: false,
        }
    }

    pub const fn required(id: u16) -> Self {
        Self { id, required: true }
    }

    pub const fn known(self) -> Option<KnownCapability> {
        KnownCapability::from_id(self.id)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum WireMessage {
    Hello {
        capabilities: Vec<CapabilityOffer>,
    },
    HelloAck {
        version: WireVersion,
        capabilities: Vec<CapabilityOffer>,
    },
    Auth {
        kind: AuthMessageKind,
        body: Vec<u8>,
    },
    SessionStart {
        session_id: WireId,
        transfer_id: WireId,
        role: WireRole,
    },
    SessionAccept {
        session_id: WireId,
    },
    ManifestOffer {
        session_id: WireId,
        file_count: u64,
        total_size: u64,
        body: Vec<u8>,
    },
    ManifestAccept {
        session_id: WireId,
    },
    ChunkRequest {
        session_id: WireId,
        file_id: WireId,
        index: u64,
        offset: u64,
        length: u64,
    },
    ChunkData {
        session_id: WireId,
        file_id: WireId,
        index: u64,
        offset: u64,
        length: u64,
        body: Vec<u8>,
    },
    ChunkAck {
        session_id: WireId,
        file_id: WireId,
        index: u64,
        offset: u64,
        length: u64,
    },
    Pause {
        session_id: WireId,
    },
    Resume {
        session_id: WireId,
    },
    Cancel {
        session_id: WireId,
        reason: u16,
    },
    Close {
        session_id: WireId,
        reason: u16,
    },
    Error {
        code: u16,
    },
}

impl fmt::Debug for WireMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct(match self {
            Self::Hello { .. } => "WireMessage::Hello",
            Self::HelloAck { .. } => "WireMessage::HelloAck",
            Self::Auth { .. } => "WireMessage::Auth",
            Self::SessionStart { .. } => "WireMessage::SessionStart",
            Self::SessionAccept { .. } => "WireMessage::SessionAccept",
            Self::ManifestOffer { .. } => "WireMessage::ManifestOffer",
            Self::ManifestAccept { .. } => "WireMessage::ManifestAccept",
            Self::ChunkRequest { .. } => "WireMessage::ChunkRequest",
            Self::ChunkData { .. } => "WireMessage::ChunkData",
            Self::ChunkAck { .. } => "WireMessage::ChunkAck",
            Self::Pause { .. } => "WireMessage::Pause",
            Self::Resume { .. } => "WireMessage::Resume",
            Self::Cancel { .. } => "WireMessage::Cancel",
            Self::Close { .. } => "WireMessage::Close",
            Self::Error { .. } => "WireMessage::Error",
        });
        match self {
            Self::Hello { capabilities } => {
                debug.field("capability_count", &capabilities.len());
            }
            Self::HelloAck {
                version,
                capabilities,
            } => {
                debug
                    .field("version", version)
                    .field("capability_count", &capabilities.len());
            }
            Self::Auth { kind, body } => {
                debug.field("kind", kind).field("body_len", &body.len());
            }
            Self::SessionStart {
                session_id,
                transfer_id,
                role,
            } => {
                debug
                    .field("session_id", session_id)
                    .field("transfer_id", transfer_id)
                    .field("role", role);
            }
            Self::SessionAccept { session_id }
            | Self::ManifestAccept { session_id }
            | Self::Pause { session_id }
            | Self::Resume { session_id } => {
                debug.field("session_id", session_id);
            }
            Self::ManifestOffer {
                session_id,
                file_count,
                total_size,
                body,
            } => {
                debug
                    .field("session_id", session_id)
                    .field("file_count", file_count)
                    .field("total_size", total_size)
                    .field("body_len", &body.len());
            }
            Self::ChunkRequest {
                session_id,
                file_id,
                index,
                offset,
                length,
            }
            | Self::ChunkAck {
                session_id,
                file_id,
                index,
                offset,
                length,
            } => {
                debug
                    .field("session_id", session_id)
                    .field("file_id", file_id)
                    .field("index", index)
                    .field("offset", offset)
                    .field("length", length);
            }
            Self::ChunkData {
                session_id,
                file_id,
                index,
                offset,
                length,
                body,
            } => {
                debug
                    .field("session_id", session_id)
                    .field("file_id", file_id)
                    .field("index", index)
                    .field("offset", offset)
                    .field("length", length)
                    .field("body_len", &body.len());
            }
            Self::Cancel { session_id, reason } | Self::Close { session_id, reason } => {
                debug
                    .field("session_id", session_id)
                    .field("reason", reason);
            }
            Self::Error { code } => {
                debug.field("code", code);
            }
        }
        debug.finish()
    }
}

impl WireMessage {
    pub const fn message_type(&self) -> MessageType {
        match self {
            Self::Hello { .. } => MessageType::Hello,
            Self::HelloAck { .. } => MessageType::HelloAck,
            Self::Auth { .. } => MessageType::Auth,
            Self::SessionStart { .. } => MessageType::SessionStart,
            Self::SessionAccept { .. } => MessageType::SessionAccept,
            Self::ManifestOffer { .. } => MessageType::ManifestOffer,
            Self::ManifestAccept { .. } => MessageType::ManifestAccept,
            Self::ChunkRequest { .. } => MessageType::ChunkRequest,
            Self::ChunkData { .. } => MessageType::ChunkData,
            Self::ChunkAck { .. } => MessageType::ChunkAck,
            Self::Pause { .. } => MessageType::Pause,
            Self::Resume { .. } => MessageType::Resume,
            Self::Cancel { .. } => MessageType::Cancel,
            Self::Close { .. } => MessageType::Close,
            Self::Error { .. } => MessageType::Error,
        }
    }

    pub const fn class(&self) -> MessageClass {
        self.message_type().class()
    }

    pub fn encode_payload(&self, limits: WireLimits) -> Result<Vec<u8>, WireError> {
        limits.validate()?;
        let mut writer = PayloadWriter::default();
        match self {
            Self::Hello { capabilities } => {
                validate_capabilities(capabilities, limits.max_capabilities)?;
                writer.put_u16(capabilities.len() as u16);
                for capability in capabilities {
                    writer.put_u16(capability.id);
                    writer.put_u8(u8::from(capability.required));
                }
            }
            Self::HelloAck {
                version,
                capabilities,
            } => {
                validate_capabilities(capabilities, limits.max_capabilities)?;
                writer.put_u8(version.major);
                writer.put_u8(version.minor);
                writer.put_u16(capabilities.len() as u16);
                for capability in capabilities {
                    writer.put_u16(capability.id);
                    writer.put_u8(u8::from(capability.required));
                }
            }
            Self::Auth { kind, body } => {
                ensure_opaque_limit("authentication", body.len(), limits.max_opaque_payload)?;
                writer.put_u8(*kind as u8);
                writer.put_bytes(body)?;
            }
            Self::SessionStart {
                session_id,
                transfer_id,
                role,
            } => {
                writer.put_id(*session_id);
                writer.put_id(*transfer_id);
                writer.put_u8(*role as u8);
            }
            Self::SessionAccept { session_id }
            | Self::ManifestAccept { session_id }
            | Self::Pause { session_id }
            | Self::Resume { session_id } => writer.put_id(*session_id),
            Self::ManifestOffer {
                session_id,
                file_count,
                total_size,
                body,
            } => {
                ensure_opaque_limit("manifest", body.len(), limits.max_opaque_payload)?;
                writer.put_id(*session_id);
                writer.put_u64(*file_count);
                writer.put_u64(*total_size);
                writer.put_bytes(body)?;
            }
            Self::ChunkRequest {
                session_id,
                file_id,
                index,
                offset,
                length,
            }
            | Self::ChunkAck {
                session_id,
                file_id,
                index,
                offset,
                length,
            } => {
                writer.put_id(*session_id);
                writer.put_id(*file_id);
                writer.put_u64(*index);
                writer.put_u64(*offset);
                writer.put_u64(*length);
            }
            Self::ChunkData {
                session_id,
                file_id,
                index,
                offset,
                length,
                body,
            } => {
                let declared_length =
                    usize::try_from(*length).map_err(|_| WireError::MalformedMessage {
                        message_type: MessageType::ChunkData,
                        reason: "chunk length does not fit platform usize",
                    })?;
                if declared_length != body.len() {
                    return Err(WireError::MalformedMessage {
                        message_type: MessageType::ChunkData,
                        reason: "chunk length does not match body length",
                    });
                }
                ensure_opaque_limit("chunk", body.len(), limits.max_opaque_payload)?;
                writer.put_id(*session_id);
                writer.put_id(*file_id);
                writer.put_u64(*index);
                writer.put_u64(*offset);
                writer.put_u64(*length);
                writer.put_bytes(body)?;
            }
            Self::Cancel { session_id, reason } | Self::Close { session_id, reason } => {
                writer.put_id(*session_id);
                writer.put_u16(*reason);
            }
            Self::Error { code } => writer.put_u16(*code),
        }
        if writer.bytes.len() > limits.max_frame_payload {
            return Err(WireError::FrameTooLarge {
                length: writer.bytes.len(),
                max: limits.max_frame_payload,
            });
        }
        Ok(writer.bytes)
    }

    fn decode_payload(
        message_type: MessageType,
        payload: &[u8],
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        let mut reader = PayloadReader::new(payload);
        let message = match message_type {
            MessageType::Hello => Self::Hello {
                capabilities: reader.capabilities(limits.max_capabilities, message_type)?,
            },
            MessageType::HelloAck => {
                let version = reader.version(message_type)?;
                Self::HelloAck {
                    version,
                    capabilities: reader.capabilities(limits.max_capabilities, message_type)?,
                }
            }
            MessageType::Auth => {
                let kind = reader.auth_kind(message_type)?;
                let body =
                    reader.opaque("authentication", limits.max_opaque_payload, message_type)?;
                Self::Auth { kind, body }
            }
            MessageType::SessionStart => Self::SessionStart {
                session_id: reader.id(message_type)?,
                transfer_id: reader.id(message_type)?,
                role: reader.role(message_type)?,
            },
            MessageType::SessionAccept => Self::SessionAccept {
                session_id: reader.id(message_type)?,
            },
            MessageType::ManifestOffer => Self::ManifestOffer {
                session_id: reader.id(message_type)?,
                file_count: reader.u64(message_type)?,
                total_size: reader.u64(message_type)?,
                body: reader.opaque("manifest", limits.max_opaque_payload, message_type)?,
            },
            MessageType::ManifestAccept => Self::ManifestAccept {
                session_id: reader.id(message_type)?,
            },
            MessageType::ChunkRequest => Self::ChunkRequest {
                session_id: reader.id(message_type)?,
                file_id: reader.id(message_type)?,
                index: reader.u64(message_type)?,
                offset: reader.u64(message_type)?,
                length: reader.u64(message_type)?,
            },
            MessageType::ChunkData => {
                let session_id = reader.id(message_type)?;
                let file_id = reader.id(message_type)?;
                let index = reader.u64(message_type)?;
                let offset = reader.u64(message_type)?;
                let length = reader.u64(message_type)?;
                let body = reader.opaque("chunk", limits.max_opaque_payload, message_type)?;
                let declared_length =
                    usize::try_from(length).map_err(|_| WireError::MalformedMessage {
                        message_type,
                        reason: "chunk length does not fit platform usize",
                    })?;
                if declared_length != body.len() {
                    return Err(WireError::MalformedMessage {
                        message_type,
                        reason: "chunk length does not match body length",
                    });
                }
                Self::ChunkData {
                    session_id,
                    file_id,
                    index,
                    offset,
                    length,
                    body,
                }
            }
            MessageType::ChunkAck => Self::ChunkAck {
                session_id: reader.id(message_type)?,
                file_id: reader.id(message_type)?,
                index: reader.u64(message_type)?,
                offset: reader.u64(message_type)?,
                length: reader.u64(message_type)?,
            },
            MessageType::Pause => Self::Pause {
                session_id: reader.id(message_type)?,
            },
            MessageType::Resume => Self::Resume {
                session_id: reader.id(message_type)?,
            },
            MessageType::Cancel => Self::Cancel {
                session_id: reader.id(message_type)?,
                reason: reader.u16(message_type)?,
            },
            MessageType::Close => Self::Close {
                session_id: reader.id(message_type)?,
                reason: reader.u16(message_type)?,
            },
            MessageType::Error => Self::Error {
                code: reader.u16(message_type)?,
            },
        };
        reader.finish()?;
        Ok(message)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: WireVersion,
    pub flags: u8,
    pub message_type: u8,
    pub payload_len: u32,
    pub sequence: u64,
    pub correlation_id: u64,
}

impl fmt::Debug for FrameHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrameHeader")
            .field("version", &self.version)
            .field("flags", &self.flags)
            .field("message_type", &self.message_type)
            .field("payload_len", &self.payload_len)
            .field("sequence", &self.sequence)
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

impl FrameHeader {
    pub const fn optional(self) -> bool {
        self.message_type & OPTIONAL_MESSAGE_BIT != 0
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum FramePayload {
    Message(WireMessage),
    UnknownOptional { message_type: u8, body: Vec<u8> },
}

impl fmt::Debug for FramePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => message.fmt(formatter),
            Self::UnknownOptional { message_type, body } => formatter
                .debug_struct("FramePayload::UnknownOptional")
                .field("message_type", message_type)
                .field("body_len", &body.len())
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct WireFrame {
    pub header: FrameHeader,
    pub payload: FramePayload,
}

impl fmt::Debug for WireFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WireFrame")
            .field("header", &self.header)
            .field("payload", &self.payload)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WireCodec {
    limits: WireLimits,
    local_version: WireVersion,
}

impl Default for WireCodec {
    fn default() -> Self {
        Self::new(WIRE_VERSION, WireLimits::default())
    }
}

impl WireCodec {
    pub const fn new(local_version: WireVersion, limits: WireLimits) -> Self {
        Self {
            limits,
            local_version,
        }
    }

    pub const fn limits(self) -> WireLimits {
        self.limits
    }

    pub const fn local_version(self) -> WireVersion {
        self.local_version
    }

    pub fn encode(
        &self,
        version: WireVersion,
        sequence: u64,
        message: &WireMessage,
    ) -> Result<Vec<u8>, WireError> {
        self.encode_with_correlation(version, sequence, sequence, message)
    }

    pub fn encode_with_correlation(
        &self,
        version: WireVersion,
        sequence: u64,
        correlation_id: u64,
        message: &WireMessage,
    ) -> Result<Vec<u8>, WireError> {
        self.limits.validate()?;
        self.validate_version(version)?;
        self.validate_message_version(message)?;
        let payload = message.encode_payload(self.limits)?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| WireError::FrameTooLarge {
            length: payload.len(),
            max: self.limits.max_frame_payload,
        })?;
        let header = FrameHeader {
            version,
            flags: 0,
            message_type: message.message_type() as u8,
            payload_len,
            sequence,
            correlation_id,
        };
        let mut bytes = Vec::with_capacity(WIRE_HEADER_LEN + payload.len());
        encode_header(&mut bytes, header);
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<(WireFrame, usize), WireError> {
        self.limits.validate()?;
        let header = decode_header(bytes)?;
        self.validate_header(header)?;
        let payload_len = header.payload_len as usize;
        let frame_len =
            WIRE_HEADER_LEN
                .checked_add(payload_len)
                .ok_or(WireError::FrameTooLarge {
                    length: payload_len,
                    max: self.limits.max_frame_payload,
                })?;
        if bytes.len() < frame_len {
            return Err(WireError::Incomplete {
                needed: frame_len - bytes.len(),
            });
        }
        let body = &bytes[WIRE_HEADER_LEN..frame_len];
        let payload = decode_payload(header.message_type, body, self.limits)?;
        if let FramePayload::Message(message) = &payload {
            self.validate_message_version(message)?;
        }
        Ok((WireFrame { header, payload }, frame_len))
    }

    pub fn decode_exact(&self, bytes: &[u8]) -> Result<WireFrame, WireError> {
        let (frame, consumed) = self.decode(bytes)?;
        if consumed != bytes.len() {
            return Err(WireError::TrailingBytes);
        }
        Ok(frame)
    }

    pub async fn read_frame<R>(&self, reader: &mut R) -> Result<WireFrame, WireError>
    where
        R: AsyncRead + Unpin,
    {
        self.limits.validate()?;
        let mut header_bytes = [0_u8; WIRE_HEADER_LEN];
        read_exact_or_truncated(reader, &mut header_bytes).await?;
        let header = decode_header(&header_bytes)?;
        self.validate_header(header)?;
        let frame_len = WIRE_HEADER_LEN
            .checked_add(header.payload_len as usize)
            .ok_or(WireError::FrameTooLarge {
                length: header.payload_len as usize,
                max: self.limits.max_frame_payload,
            })?;
        let mut bytes = header_bytes.to_vec();
        bytes.resize(frame_len, 0);
        read_exact_or_truncated(reader, &mut bytes[WIRE_HEADER_LEN..]).await?;
        self.decode_exact(&bytes)
    }

    pub async fn write_frame<W>(
        &self,
        writer: &mut W,
        version: WireVersion,
        sequence: u64,
        message: &WireMessage,
    ) -> Result<(), WireError>
    where
        W: AsyncWrite + Unpin,
    {
        self.write_frame_with_correlation(writer, version, sequence, sequence, message)
            .await
    }

    pub async fn write_frame_with_correlation<W>(
        &self,
        writer: &mut W,
        version: WireVersion,
        sequence: u64,
        correlation_id: u64,
        message: &WireMessage,
    ) -> Result<(), WireError>
    where
        W: AsyncWrite + Unpin,
    {
        let bytes = self.encode_with_correlation(version, sequence, correlation_id, message)?;
        writer.write_all(&bytes).await.map_err(WireError::from)
    }

    fn validate_version(&self, version: WireVersion) -> Result<(), WireError> {
        if !self.local_version.same_major(version) {
            return Err(WireError::UnsupportedVersion {
                found: version,
                supported_major: self.local_version.major,
            });
        }
        Ok(())
    }

    fn validate_header(&self, header: FrameHeader) -> Result<(), WireError> {
        self.validate_version(header.version)?;
        if header.flags != 0 {
            return Err(WireError::ReservedFlags {
                flags: header.flags,
            });
        }
        let payload_len = header.payload_len as usize;
        if payload_len > self.limits.max_frame_payload {
            return Err(WireError::FrameTooLarge {
                length: payload_len,
                max: self.limits.max_frame_payload,
            });
        }
        Ok(())
    }

    fn validate_message_version(&self, message: &WireMessage) -> Result<(), WireError> {
        if let WireMessage::HelloAck { version, .. } = message {
            self.validate_version(*version)?;
        }
        Ok(())
    }
}

pub struct FrameDecoder {
    codec: WireCodec,
    buffer: Vec<u8>,
}

impl FrameDecoder {
    pub fn new(codec: WireCodec) -> Self {
        Self {
            codec,
            buffer: Vec::new(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<WireFrame>, WireError> {
        self.codec.limits.validate()?;
        let new_len =
            self.buffer
                .len()
                .checked_add(bytes.len())
                .ok_or(WireError::BufferTooLarge {
                    max: self.codec.limits.max_buffered_bytes,
                })?;
        if new_len > self.codec.limits.max_buffered_bytes {
            return Err(WireError::BufferTooLarge {
                max: self.codec.limits.max_buffered_bytes,
            });
        }
        self.buffer.extend_from_slice(bytes);
        let mut frames = Vec::new();
        let mut consumed = 0;
        let result = loop {
            match self.codec.decode(&self.buffer[consumed..]) {
                Ok((frame, frame_consumed)) => {
                    consumed =
                        consumed
                            .checked_add(frame_consumed)
                            .ok_or(WireError::BufferTooLarge {
                                max: self.codec.limits.max_buffered_bytes,
                            })?;
                    frames.push(frame);
                }
                Err(WireError::Incomplete { .. }) => break Ok(()),
                Err(error) => break Err(error),
            }
        };
        if consumed != 0 {
            self.buffer.drain(..consumed);
        }
        result?;
        Ok(frames)
    }

    pub fn finish(self) -> Result<(), WireError> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(WireError::Truncated)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolPhase {
    PreAuthentication,
    Negotiating,
    Authenticating,
    Authenticated,
    Transferring,
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireState {
    phase: ProtocolPhase,
    last_sequence: Option<u64>,
    hello_seen: bool,
    hello_ack_seen: bool,
    required_capabilities: Vec<u16>,
    auth_started: bool,
    auth_responded: bool,
    auth_confirmed: bool,
    session_id: Option<WireId>,
    session_started: bool,
    session_accepted: bool,
    manifest_offered: bool,
    manifest_accepted: bool,
    paused: bool,
}

impl Default for WireState {
    fn default() -> Self {
        Self {
            phase: ProtocolPhase::PreAuthentication,
            last_sequence: None,
            hello_seen: false,
            hello_ack_seen: false,
            required_capabilities: Vec::new(),
            auth_started: false,
            auth_responded: false,
            auth_confirmed: false,
            session_id: None,
            session_started: false,
            session_accepted: false,
            manifest_offered: false,
            manifest_accepted: false,
            paused: false,
        }
    }
}

impl WireState {
    pub const fn phase(&self) -> ProtocolPhase {
        self.phase
    }

    pub const fn paused(&self) -> bool {
        self.paused
    }

    pub fn accept_frame(&mut self, frame: &WireFrame) -> Result<(), WireError> {
        if let Some(previous) = self.last_sequence {
            if frame.header.sequence <= previous {
                return Err(WireError::InvalidSequence {
                    sequence: frame.header.sequence,
                    previous,
                });
            }
        }
        let result = match &frame.payload {
            FramePayload::Message(message) => self.accept(message),
            FramePayload::UnknownOptional { .. } => Ok(()),
        };
        if result.is_ok() {
            self.last_sequence = Some(frame.header.sequence);
        }
        result
    }

    pub fn accept(&mut self, message: &WireMessage) -> Result<(), WireError> {
        if self.phase == ProtocolPhase::Closed {
            return Err(self.invalid(message));
        }

        match message {
            WireMessage::Hello { capabilities } => {
                if self.phase != ProtocolPhase::PreAuthentication || self.hello_seen {
                    return Err(WireError::DuplicateNegotiation);
                }
                self.required_capabilities = capabilities
                    .iter()
                    .filter_map(|capability| capability.required.then_some(capability.id))
                    .collect();
                self.hello_seen = true;
                self.phase = ProtocolPhase::Negotiating;
            }
            WireMessage::HelloAck { capabilities, .. } => {
                if self.phase != ProtocolPhase::Negotiating
                    || !self.hello_seen
                    || self.hello_ack_seen
                {
                    return Err(self.invalid(message));
                }
                self.require_capabilities(capabilities)?;
                self.hello_ack_seen = true;
                self.phase = ProtocolPhase::Authenticating;
            }
            WireMessage::Auth { kind, .. } => {
                if self.phase != ProtocolPhase::Authenticating {
                    return Err(self.invalid(message));
                }
                match kind {
                    AuthMessageKind::Init if self.auth_started => {
                        return Err(self.invalid(message));
                    }
                    AuthMessageKind::Init => {
                        self.auth_started = true;
                    }
                    AuthMessageKind::Response if !self.auth_started || self.auth_responded => {
                        return Err(self.invalid(message));
                    }
                    AuthMessageKind::Response => {
                        self.auth_responded = true;
                    }
                    AuthMessageKind::Confirm if !self.auth_responded => {
                        return Err(self.invalid(message));
                    }
                    AuthMessageKind::Confirm => {
                        self.auth_confirmed = true;
                        self.phase = ProtocolPhase::Authenticated;
                    }
                }
            }
            WireMessage::SessionStart { session_id, .. } => {
                if self.phase != ProtocolPhase::Authenticated || !self.auth_confirmed {
                    return Err(self.invalid(message));
                }
                if self.session_started {
                    return Err(self.invalid(message));
                }
                self.session_started = true;
                self.session_id = Some(*session_id);
            }
            WireMessage::SessionAccept { session_id } => {
                if self.phase != ProtocolPhase::Authenticated || !self.session_started {
                    return Err(self.invalid(message));
                }
                self.require_session(*session_id)?;
                if self.session_accepted {
                    return Err(self.invalid(message));
                }
                self.session_accepted = true;
            }
            WireMessage::ManifestOffer { session_id, .. } => {
                if self.phase != ProtocolPhase::Authenticated || !self.session_accepted {
                    return Err(self.invalid(message));
                }
                self.require_session(*session_id)?;
                if self.manifest_offered {
                    return Err(self.invalid(message));
                }
                self.manifest_offered = true;
            }
            WireMessage::ManifestAccept { session_id } => {
                if self.phase != ProtocolPhase::Authenticated || !self.manifest_offered {
                    return Err(self.invalid(message));
                }
                self.require_session(*session_id)?;
                if self.manifest_accepted {
                    return Err(self.invalid(message));
                }
                self.manifest_accepted = true;
                self.phase = ProtocolPhase::Transferring;
            }
            WireMessage::ChunkRequest { session_id, .. }
            | WireMessage::ChunkData { session_id, .. }
            | WireMessage::ChunkAck { session_id, .. }
            | WireMessage::Pause { session_id }
            | WireMessage::Resume { session_id }
            | WireMessage::Cancel { session_id, .. }
            | WireMessage::Close { session_id, .. } => {
                self.require_session(*session_id)?;
                self.accept_session_message(message)?;
            }
            WireMessage::Error { .. } => {}
        }
        Ok(())
    }

    fn accept_session_message(&mut self, message: &WireMessage) -> Result<(), WireError> {
        match message {
            WireMessage::ChunkRequest { .. }
            | WireMessage::ChunkData { .. }
            | WireMessage::ChunkAck { .. } => {
                if self.phase != ProtocolPhase::Transferring || self.paused {
                    return Err(self.invalid(message));
                }
            }
            WireMessage::Pause { .. } => {
                if self.phase != ProtocolPhase::Transferring || self.paused {
                    return Err(self.invalid(message));
                }
                self.paused = true;
            }
            WireMessage::Resume { .. } => {
                if self.phase != ProtocolPhase::Transferring || !self.paused {
                    return Err(self.invalid(message));
                }
                self.paused = false;
            }
            WireMessage::Cancel { .. } => {
                if self.phase == ProtocolPhase::Closed {
                    return Err(self.invalid(message));
                }
                self.phase = ProtocolPhase::Closing;
            }
            WireMessage::Close { .. } => {
                self.phase = ProtocolPhase::Closed;
            }
            _ => {}
        }
        Ok(())
    }

    fn require_session(&self, session_id: WireId) -> Result<(), WireError> {
        if self.session_id == Some(session_id) {
            Ok(())
        } else {
            Err(WireError::InvalidSession)
        }
    }

    fn require_capabilities(&self, acknowledged: &[CapabilityOffer]) -> Result<(), WireError> {
        for required_id in &self.required_capabilities {
            if !acknowledged
                .iter()
                .any(|capability| capability.id == *required_id)
            {
                return Err(WireError::MissingRequiredCapability { id: *required_id });
            }
        }
        Ok(())
    }

    fn invalid(&self, message: &WireMessage) -> WireError {
        WireError::InvalidState {
            phase: self.phase,
            message_type: message.message_type(),
        }
    }
}

fn validate_capabilities(
    capabilities: &[CapabilityOffer],
    max_capabilities: usize,
) -> Result<(), WireError> {
    if capabilities.len() > max_capabilities || capabilities.len() > u16::MAX as usize {
        return Err(WireError::FieldTooLarge {
            field: "capabilities",
            length: capabilities.len(),
            max: max_capabilities,
        });
    }
    let mut seen = Vec::with_capacity(capabilities.len());
    for capability in capabilities {
        if seen.contains(&capability.id) {
            return Err(WireError::DuplicateCapability { id: capability.id });
        }
        if capability.required && KnownCapability::from_id(capability.id).is_none() {
            return Err(WireError::UnsupportedCapability { id: capability.id });
        }
        seen.push(capability.id);
    }
    Ok(())
}

fn ensure_opaque_limit(field: &'static str, length: usize, max: usize) -> Result<(), WireError> {
    if length > max || length > u32::MAX as usize {
        return Err(WireError::FieldTooLarge { field, length, max });
    }
    Ok(())
}

fn decode_payload(
    raw_message_type: u8,
    payload: &[u8],
    limits: WireLimits,
) -> Result<FramePayload, WireError> {
    let optional = raw_message_type & OPTIONAL_MESSAGE_BIT != 0;
    let message_type = raw_message_type & !OPTIONAL_MESSAGE_BIT;
    let Some(message_type) = MessageType::from_wire(message_type) else {
        if optional {
            return Ok(FramePayload::UnknownOptional {
                message_type: raw_message_type,
                body: payload.to_owned(),
            });
        }
        return Err(WireError::UnknownMandatoryMessage {
            message_type: raw_message_type,
        });
    };
    if optional {
        return Err(WireError::KnownMessageMarkedOptional { message_type });
    }
    match WireMessage::decode_payload(message_type, payload, limits) {
        Ok(message) => Ok(FramePayload::Message(message)),
        Err(WireError::Incomplete { .. }) => Err(WireError::MalformedMessage {
            message_type,
            reason: "message body is truncated",
        }),
        Err(error) => Err(error),
    }
}

fn encode_header(bytes: &mut Vec<u8>, header: FrameHeader) {
    bytes.extend_from_slice(&WIRE_MAGIC);
    bytes.push(header.version.major);
    bytes.push(header.version.minor);
    bytes.push(header.flags);
    bytes.push(header.message_type);
    bytes.extend_from_slice(&header.payload_len.to_be_bytes());
    bytes.extend_from_slice(&header.sequence.to_be_bytes());
    bytes.extend_from_slice(&header.correlation_id.to_be_bytes());
}

fn decode_header(bytes: &[u8]) -> Result<FrameHeader, WireError> {
    if bytes.len() < WIRE_HEADER_LEN {
        return Err(WireError::Incomplete {
            needed: WIRE_HEADER_LEN - bytes.len(),
        });
    }
    if bytes[..WIRE_MAGIC.len()] != WIRE_MAGIC {
        return Err(WireError::InvalidMagic);
    }
    let version = WireVersion::new(bytes[4], bytes[5]);
    let flags = bytes[6];
    let message_type = bytes[7];
    let payload_len = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let sequence = u64::from_be_bytes([
        bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
    ]);
    let correlation_id = u64::from_be_bytes([
        bytes[20], bytes[21], bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27],
    ]);
    Ok(FrameHeader {
        version,
        flags,
        message_type,
        payload_len,
        sequence,
        correlation_id,
    })
}

async fn read_exact_or_truncated<R>(reader: &mut R, buffer: &mut [u8]) -> Result<(), WireError>
where
    R: AsyncRead + Unpin,
{
    match reader.read_exact(buffer).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Err(WireError::Truncated),
        Err(error) => Err(WireError::from(error)),
    }
}

#[derive(Default)]
struct PayloadWriter {
    bytes: Vec<u8>,
}

impl PayloadWriter {
    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_id(&mut self, value: WireId) {
        self.bytes.extend_from_slice(value.as_bytes());
    }

    fn put_bytes(&mut self, value: &[u8]) -> Result<(), WireError> {
        let length = u32::try_from(value.len()).map_err(|_| WireError::FieldTooLarge {
            field: "payload",
            length: value.len(),
            max: u32::MAX as usize,
        })?;
        self.bytes.extend_from_slice(&length.to_be_bytes());
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

struct PayloadReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize, message_type: MessageType) -> Result<&'a [u8], WireError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(WireError::MalformedMessage {
                message_type,
                reason: "field length overflow",
            })?;
        if end > self.bytes.len() {
            return Err(WireError::Incomplete {
                needed: end - self.bytes.len(),
            });
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self, message_type: MessageType) -> Result<u8, WireError> {
        Ok(self.take(1, message_type)?[0])
    }

    fn u16(&mut self, message_type: MessageType) -> Result<u16, WireError> {
        let bytes = self.take(2, message_type)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self, message_type: MessageType) -> Result<u32, WireError> {
        let bytes = self.take(4, message_type)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self, message_type: MessageType) -> Result<u64, WireError> {
        let bytes = self.take(8, message_type)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn id(&mut self, message_type: MessageType) -> Result<WireId, WireError> {
        let bytes = self.take(16, message_type)?;
        let mut id = [0_u8; 16];
        id.copy_from_slice(bytes);
        Ok(WireId::from_bytes(id))
    }

    fn version(&mut self, message_type: MessageType) -> Result<WireVersion, WireError> {
        Ok(WireVersion::new(
            self.u8(message_type)?,
            self.u8(message_type)?,
        ))
    }

    fn role(&mut self, message_type: MessageType) -> Result<WireRole, WireError> {
        let value = self.u8(message_type)?;
        WireRole::from_wire(value).ok_or(WireError::MalformedMessage {
            message_type,
            reason: "unknown role",
        })
    }

    fn auth_kind(&mut self, message_type: MessageType) -> Result<AuthMessageKind, WireError> {
        let value = self.u8(message_type)?;
        AuthMessageKind::from_wire(value).ok_or(WireError::MalformedMessage {
            message_type,
            reason: "unknown authentication message kind",
        })
    }

    fn capabilities(
        &mut self,
        max_capabilities: usize,
        message_type: MessageType,
    ) -> Result<Vec<CapabilityOffer>, WireError> {
        let count = self.u16(message_type)? as usize;
        if count > max_capabilities {
            return Err(WireError::FieldTooLarge {
                field: "capabilities",
                length: count,
                max: max_capabilities,
            });
        }
        let mut capabilities = Vec::with_capacity(count);
        for _ in 0..count {
            let id = self.u16(message_type)?;
            let flags = self.u8(message_type)?;
            if flags & !1 != 0 {
                return Err(WireError::MalformedMessage {
                    message_type,
                    reason: "unknown capability flags",
                });
            }
            capabilities.push(CapabilityOffer {
                id,
                required: flags & 1 != 0,
            });
        }
        validate_capabilities(&capabilities, max_capabilities)?;
        Ok(capabilities)
    }

    fn opaque(
        &mut self,
        field: &'static str,
        max: usize,
        message_type: MessageType,
    ) -> Result<Vec<u8>, WireError> {
        let length = self.u32(message_type)? as usize;
        if length > max {
            return Err(WireError::FieldTooLarge { field, length, max });
        }
        Ok(self.take(length, message_type)?.to_owned())
    }

    fn finish(&self) -> Result<(), WireError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(WireError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BackendError;
    use tokio::io::duplex;

    fn id(value: u8) -> WireId {
        WireId::from_bytes([value; 16])
    }

    fn hello() -> WireMessage {
        WireMessage::Hello {
            capabilities: vec![
                CapabilityOffer::required(KnownCapability::EncryptedRecords.id()),
                CapabilityOffer::optional(0x9000),
            ],
        }
    }

    fn session_start() -> WireMessage {
        WireMessage::SessionStart {
            session_id: id(1),
            transfer_id: id(2),
            role: WireRole::Sender,
        }
    }

    fn authenticated_prefix() -> Vec<WireMessage> {
        vec![
            hello(),
            WireMessage::HelloAck {
                version: WIRE_VERSION,
                capabilities: vec![CapabilityOffer::required(
                    KnownCapability::EncryptedRecords.id(),
                )],
            },
            WireMessage::Auth {
                kind: AuthMessageKind::Init,
                body: vec![1, 2, 3],
            },
            WireMessage::Auth {
                kind: AuthMessageKind::Response,
                body: vec![4, 5, 6],
            },
            WireMessage::Auth {
                kind: AuthMessageKind::Confirm,
                body: vec![4],
            },
            session_start(),
            WireMessage::SessionAccept { session_id: id(1) },
            WireMessage::ManifestOffer {
                session_id: id(1),
                file_count: 1,
                total_size: 3,
                body: vec![9, 8, 7],
            },
            WireMessage::ManifestAccept { session_id: id(1) },
        ]
    }

    #[test]
    fn canonical_encoding_round_trips_without_payload_debug_leak() {
        let codec = WireCodec::default();
        let message = WireMessage::Auth {
            kind: AuthMessageKind::Response,
            body: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let first = codec.encode(WIRE_VERSION, 7, &message).unwrap();
        let second = codec.encode(WIRE_VERSION, 7, &message).unwrap();
        assert_eq!(first, second);
        let frame = codec.decode_exact(&first).unwrap();
        assert_eq!(frame.header.sequence, 7);
        assert_eq!(frame.header.correlation_id, 7);
        assert_eq!(frame.payload, FramePayload::Message(message.clone()));
        assert!(!format!("{message:?}").contains("dead"));

        let acknowledgement = WireMessage::HelloAck {
            version: WireVersion::new(1, 4),
            capabilities: vec![CapabilityOffer::optional(0x9000)],
        };
        let acknowledgement_frame = codec
            .decode_exact(
                &codec
                    .encode_with_correlation(WIRE_VERSION, 8, 7, &acknowledgement)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(acknowledgement_frame.header.correlation_id, 7);
        assert_eq!(
            acknowledgement_frame.payload,
            FramePayload::Message(acknowledgement)
        );

        let unsupported_ack = WireMessage::HelloAck {
            version: WireVersion::new(2, 0),
            capabilities: Vec::new(),
        };
        assert!(matches!(
            codec.encode(WIRE_VERSION, 9, &unsupported_ack),
            Err(WireError::UnsupportedVersion {
                found: WireVersion { major: 2, minor: 0 },
                supported_major: 1,
            })
        ));
    }

    #[test]
    fn decoder_handles_one_byte_fragments_and_coalesced_frames() {
        let codec = WireCodec::default();
        let first = codec.encode(WIRE_VERSION, 1, &hello()).unwrap();
        let second = codec
            .encode(WIRE_VERSION, 2, &WireMessage::Error { code: 9 })
            .unwrap();
        let mut decoder = FrameDecoder::new(codec);
        let mut frames = Vec::new();
        for byte in &first {
            frames.extend(decoder.push(std::slice::from_ref(byte)).unwrap());
        }
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header.sequence, 1);
        frames.extend(decoder.push(&second).unwrap());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].header.sequence, 2);
        decoder.finish().unwrap();
    }

    #[test]
    fn decoder_rejects_oversized_length_before_body_allocation() {
        let codec = WireCodec::new(WIRE_VERSION, WireLimits::new(32, 32, 4, 64));
        let mut bytes = vec![0_u8; WIRE_HEADER_LEN];
        bytes[..4].copy_from_slice(&WIRE_MAGIC);
        bytes[4] = WIRE_VERSION.major;
        bytes[5] = WIRE_VERSION.minor;
        bytes[7] = MessageType::Error as u8;
        bytes[8..12].copy_from_slice(&33_u32.to_be_bytes());
        assert!(matches!(
            codec.decode(&bytes),
            Err(WireError::FrameTooLarge {
                length: 33,
                max: 32
            })
        ));
    }

    #[test]
    fn unknown_mandatory_fails_and_unknown_optional_is_preserved() {
        let codec = WireCodec::default();
        let mut mandatory = vec![0_u8; WIRE_HEADER_LEN];
        mandatory[..4].copy_from_slice(&WIRE_MAGIC);
        mandatory[4] = WIRE_VERSION.major;
        mandatory[5] = WIRE_VERSION.minor;
        mandatory[7] = 0x7f;
        mandatory[8..12].copy_from_slice(&0_u32.to_be_bytes());
        assert!(matches!(
            codec.decode(&mandatory),
            Err(WireError::UnknownMandatoryMessage { message_type: 0x7f })
        ));

        let optional = codec
            .encode(WIRE_VERSION, 3, &WireMessage::Error { code: 4 })
            .unwrap();
        let mut optional_unknown = optional;
        optional_unknown[7] = OPTIONAL_MESSAGE_BIT | 0x7f;
        let frame = codec.decode_exact(&optional_unknown).unwrap();
        assert_eq!(
            frame.payload,
            FramePayload::UnknownOptional {
                message_type: OPTIONAL_MESSAGE_BIT | 0x7f,
                body: vec![0, 4],
            }
        );

        let mut known_optional = codec
            .encode(WIRE_VERSION, 4, &WireMessage::Error { code: 5 })
            .unwrap();
        known_optional[7] |= OPTIONAL_MESSAGE_BIT;
        assert!(matches!(
            codec.decode_exact(&known_optional),
            Err(WireError::KnownMessageMarkedOptional {
                message_type: MessageType::Error,
            })
        ));
    }

    #[test]
    fn version_negotiation_allows_future_minor_but_rejects_major_change() {
        assert_eq!(
            WIRE_VERSION.negotiate(WireVersion::new(1, 4)).unwrap(),
            WIRE_VERSION
        );
        assert!(matches!(
            WIRE_VERSION.negotiate(WireVersion::new(2, 0)),
            Err(WireError::UnsupportedVersion { .. })
        ));
        let codec = WireCodec::default();
        let frame = codec.encode(WireVersion::new(1, 9), 1, &hello()).unwrap();
        assert_eq!(codec.decode_exact(&frame).unwrap().header.version.minor, 9);
    }

    #[test]
    fn capability_validation_preserves_unknown_optional_and_rejects_required() {
        let codec = WireCodec::default();
        let message = hello();
        let frame = codec
            .decode_exact(&codec.encode(WIRE_VERSION, 1, &message).unwrap())
            .unwrap();
        assert_eq!(frame.payload, FramePayload::Message(message));

        let required = WireMessage::Hello {
            capabilities: vec![CapabilityOffer::required(0x9000)],
        };
        assert!(matches!(
            codec.encode(WIRE_VERSION, 1, &required),
            Err(WireError::UnsupportedCapability { id: 0x9000 })
        ));

        let duplicate = WireMessage::Hello {
            capabilities: vec![CapabilityOffer::optional(1), CapabilityOffer::optional(1)],
        };
        assert!(matches!(
            codec.encode(WIRE_VERSION, 1, &duplicate),
            Err(WireError::DuplicateCapability { id: 1 })
        ));
    }

    #[test]
    fn malformed_payload_and_trailing_bytes_fail_closed() {
        let codec = WireCodec::default();
        let mut bytes = codec
            .encode(WIRE_VERSION, 1, &WireMessage::Error { code: 1 })
            .unwrap();
        bytes.push(0);
        let payload_len = (bytes.len() - WIRE_HEADER_LEN) as u32;
        bytes[8..12].copy_from_slice(&payload_len.to_be_bytes());
        assert!(matches!(
            codec.decode_exact(&bytes),
            Err(WireError::TrailingBytes)
        ));

        let mut bad_flags = codec.encode(WIRE_VERSION, 1, &hello()).unwrap();
        bad_flags[6] = 1;
        assert!(matches!(
            codec.decode(&bad_flags),
            Err(WireError::ReservedFlags { flags: 1 })
        ));
    }

    #[test]
    fn wire_state_rejects_duplicate_negotiation_and_wrong_order() {
        let mut state = WireState::default();
        state.accept(&hello()).unwrap();
        assert_eq!(state.accept(&hello()), Err(WireError::DuplicateNegotiation));
        assert!(matches!(
            state.accept(&WireMessage::ChunkAck {
                session_id: id(1),
                file_id: id(2),
                index: 0,
                offset: 0,
                length: 1,
            }),
            Err(WireError::InvalidState { .. }) | Err(WireError::InvalidSession)
        ));
    }

    #[test]
    fn wire_state_rejects_auth_response_before_init() {
        let mut state = WireState::default();
        state.accept(&hello()).unwrap();
        state
            .accept(&WireMessage::HelloAck {
                version: WIRE_VERSION,
                capabilities: vec![CapabilityOffer::required(
                    KnownCapability::EncryptedRecords.id(),
                )],
            })
            .unwrap();
        assert!(matches!(
            state.accept(&WireMessage::Auth {
                kind: AuthMessageKind::Response,
                body: vec![1, 2],
            }),
            Err(WireError::InvalidState { .. })
        ));
    }

    #[test]
    fn wire_state_rejects_hello_ack_missing_required_capability() {
        let mut state = WireState::default();
        state.accept(&hello()).unwrap();

        assert_eq!(
            state.accept(&WireMessage::HelloAck {
                version: WIRE_VERSION,
                capabilities: Vec::new(),
            }),
            Err(WireError::MissingRequiredCapability {
                id: KnownCapability::EncryptedRecords.id(),
            })
        );
    }

    #[test]
    fn wire_state_requires_one_auth_response_before_confirmation() {
        let mut state = WireState::default();
        state.accept(&hello()).unwrap();
        state
            .accept(&WireMessage::HelloAck {
                version: WIRE_VERSION,
                capabilities: vec![CapabilityOffer::required(
                    KnownCapability::EncryptedRecords.id(),
                )],
            })
            .unwrap();
        state
            .accept(&WireMessage::Auth {
                kind: AuthMessageKind::Init,
                body: vec![1],
            })
            .unwrap();
        assert!(matches!(
            state.accept(&WireMessage::Auth {
                kind: AuthMessageKind::Confirm,
                body: vec![2],
            }),
            Err(WireError::InvalidState { .. })
        ));
        state
            .accept(&WireMessage::Auth {
                kind: AuthMessageKind::Response,
                body: vec![3],
            })
            .unwrap();
        assert!(matches!(
            state.accept(&WireMessage::Auth {
                kind: AuthMessageKind::Response,
                body: vec![4],
            }),
            Err(WireError::InvalidState { .. })
        ));
        state
            .accept(&WireMessage::Auth {
                kind: AuthMessageKind::Confirm,
                body: vec![5],
            })
            .unwrap();
        assert_eq!(state.phase(), ProtocolPhase::Authenticated);
    }

    #[test]
    fn wire_state_accepts_ordered_session_and_pause_resume() {
        let mut state = WireState::default();
        for message in authenticated_prefix() {
            state.accept(&message).unwrap();
        }
        assert_eq!(state.phase(), ProtocolPhase::Transferring);
        state
            .accept(&WireMessage::Pause { session_id: id(1) })
            .unwrap();
        assert!(state.paused());
        assert!(matches!(
            state.accept(&WireMessage::ChunkRequest {
                session_id: id(1),
                file_id: id(2),
                index: 0,
                offset: 0,
                length: 1,
            }),
            Err(WireError::InvalidState { .. })
        ));
        state
            .accept(&WireMessage::Resume { session_id: id(1) })
            .unwrap();
        assert!(!state.paused());
        state
            .accept(&WireMessage::Close {
                session_id: id(1),
                reason: 0,
            })
            .unwrap();
        assert_eq!(state.phase(), ProtocolPhase::Closed);
    }

    #[test]
    fn wire_state_rejects_duplicate_and_out_of_order_sequences() {
        let codec = WireCodec::default();
        let first = codec.encode(WIRE_VERSION, 4, &hello()).unwrap();
        let duplicate = codec
            .encode(WIRE_VERSION, 4, &WireMessage::Error { code: 1 })
            .unwrap();
        let earlier = codec
            .encode(WIRE_VERSION, 3, &WireMessage::Error { code: 2 })
            .unwrap();
        let first = codec.decode_exact(&first).unwrap();
        let duplicate = codec.decode_exact(&duplicate).unwrap();
        let earlier = codec.decode_exact(&earlier).unwrap();
        let mut state = WireState::default();

        state.accept_frame(&first).unwrap();
        assert_eq!(
            state.accept_frame(&duplicate),
            Err(WireError::InvalidSequence {
                sequence: 4,
                previous: 4,
            })
        );
        assert_eq!(
            state.accept_frame(&earlier),
            Err(WireError::InvalidSequence {
                sequence: 3,
                previous: 4,
            })
        );
    }

    #[test]
    fn every_known_message_has_deterministic_round_trip() {
        let codec = WireCodec::default();
        let messages = vec![
            hello(),
            WireMessage::HelloAck {
                version: WIRE_VERSION,
                capabilities: vec![CapabilityOffer::optional(0x9000)],
            },
            WireMessage::Auth {
                kind: AuthMessageKind::Init,
                body: vec![1, 2],
            },
            session_start(),
            WireMessage::SessionAccept { session_id: id(1) },
            WireMessage::ManifestOffer {
                session_id: id(1),
                file_count: 1,
                total_size: 2,
                body: vec![1, 2],
            },
            WireMessage::ManifestAccept { session_id: id(1) },
            WireMessage::ChunkRequest {
                session_id: id(1),
                file_id: id(2),
                index: 3,
                offset: 4,
                length: 5,
            },
            WireMessage::ChunkData {
                session_id: id(1),
                file_id: id(2),
                index: 3,
                offset: 4,
                length: 2,
                body: vec![7, 8],
            },
            WireMessage::ChunkAck {
                session_id: id(1),
                file_id: id(2),
                index: 3,
                offset: 4,
                length: 5,
            },
            WireMessage::Pause { session_id: id(1) },
            WireMessage::Resume { session_id: id(1) },
            WireMessage::Cancel {
                session_id: id(1),
                reason: 6,
            },
            WireMessage::Close {
                session_id: id(1),
                reason: 7,
            },
            WireMessage::Error { code: 8 },
        ];

        for (sequence, message) in messages.into_iter().enumerate() {
            let sequence = sequence as u64 + 1;
            let encoded = codec.encode_with_correlation(
                WIRE_VERSION,
                sequence,
                sequence.saturating_sub(1),
                &message,
            );
            let encoded = encoded.unwrap();
            let frame = codec.decode_exact(&encoded).unwrap();
            assert_eq!(frame.header.sequence, sequence);
            assert_eq!(frame.header.correlation_id, sequence.saturating_sub(1));
            assert_eq!(frame.payload, FramePayload::Message(message));
        }
    }

    #[test]
    fn decoder_buffer_limit_rejects_unbounded_input() {
        let codec = WireCodec::new(WIRE_VERSION, WireLimits::new(32, 32, 4, 64));
        let mut decoder = FrameDecoder::new(codec);
        assert_eq!(
            decoder.push(&[0; 65]),
            Err(WireError::BufferTooLarge { max: 64 })
        );
    }

    #[test]
    fn decoder_rejects_invalid_limits_before_buffering_input() {
        let codec = WireCodec::new(WIRE_VERSION, WireLimits::new(32, 33, 4, 64));
        let mut decoder = FrameDecoder::new(codec);

        assert_eq!(decoder.push(&[0]), Err(WireError::InvalidLimits));
    }

    #[test]
    fn mutated_frame_bytes_never_panic_or_allocate_over_configured_limit() {
        let codec = WireCodec::new(WIRE_VERSION, WireLimits::new(128, 64, 4, 256));
        let valid = codec
            .encode(
                WIRE_VERSION,
                1,
                &WireMessage::Auth {
                    kind: AuthMessageKind::Init,
                    body: vec![1, 2, 3, 4],
                },
            )
            .unwrap();

        for index in 0..valid.len() {
            for value in [0_u8, 1, 0x7f, 0xff] {
                let mut mutated = valid.clone();
                mutated[index] = value;
                let _ = codec.decode(&mutated);
            }
        }
    }

    #[test]
    fn wire_errors_map_to_neutral_backend_protocol_categories() {
        assert!(matches!(
            BackendError::from(WireError::UnsupportedVersion {
                found: WireVersion::new(2, 0),
                supported_major: 1,
            }),
            BackendError::Protocol {
                reason: crate::BackendProtocolError::UnsupportedVersion,
            }
        ));
        assert!(matches!(
            BackendError::from(WireError::FrameTooLarge { length: 9, max: 8 }),
            BackendError::Protocol {
                reason: crate::BackendProtocolError::ResourceLimit,
            }
        ));
        assert!(!format!(
            "{:?}",
            BackendError::from(WireError::MalformedMessage {
                message_type: MessageType::Auth,
                reason: "test",
            })
        )
        .contains("test"));
    }

    #[tokio::test]
    async fn async_read_write_handles_fragmented_duplex_stream() {
        let codec = WireCodec::default();
        let (mut writer, mut reader) = duplex(1024);
        let expected = WireMessage::Error { code: 42 };
        let write_codec = codec;
        let write_task = tokio::spawn(async move {
            write_codec
                .write_frame_with_correlation(&mut writer, WIRE_VERSION, 8, 6, &expected)
                .await
                .unwrap();
        });
        let frame = codec.read_frame(&mut reader).await.unwrap();
        write_task.await.unwrap();
        assert_eq!(frame.header.sequence, 8);
        assert_eq!(frame.header.correlation_id, 6);
        assert_eq!(
            frame.payload,
            FramePayload::Message(WireMessage::Error { code: 42 })
        );
    }

    #[tokio::test]
    async fn async_read_rejects_invalid_limits_before_reading_stream() {
        let codec = WireCodec::new(WIRE_VERSION, WireLimits::new(32, 33, 4, 64));
        let (_writer, mut reader) = duplex(1);

        assert_eq!(
            codec.read_frame(&mut reader).await,
            Err(WireError::InvalidLimits)
        );
    }
}
