use prost::Message;
use recolith_protocol::messages::{
    DocumentGetRequest, DocumentPutRequest, ErrorResponse, PingRequest, QueryRequest, opcode,
    status,
};

/// One unary request inside a batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchOperation {
    /// Request opcode.
    pub opcode: u16,
    /// Encoded Protobuf payload.
    pub payload: Vec<u8>,
}

impl BatchOperation {
    /// Creates a batch operation from raw parts.
    #[must_use]
    pub fn new(opcode: u16, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            opcode,
            payload: payload.into(),
        }
    }

    /// Creates a ping operation.
    #[must_use]
    pub fn ping(payload: impl Into<Vec<u8>>) -> Self {
        Self::new(
            opcode::PING,
            PingRequest {
                payload: payload.into(),
            }
            .encode_to_vec(),
        )
    }

    /// Creates a document read operation.
    #[must_use]
    pub fn document_get(request: &DocumentGetRequest) -> Self {
        Self::new(opcode::DOC_GET, request.encode_to_vec())
    }

    /// Creates a document write operation.
    #[must_use]
    pub fn document_put(request: &DocumentPutRequest) -> Self {
        Self::new(opcode::DOC_PUT, request.encode_to_vec())
    }

    /// Creates a query operation.
    #[must_use]
    pub fn query(request: &QueryRequest) -> Self {
        Self::new(opcode::QUERY, request.encode_to_vec())
    }
}

/// Response to one batched operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchResult {
    /// Request ID assigned to the operation.
    pub request_id: u64,
    /// Response opcode, or [`opcode::ERROR`] when the operation failed.
    pub opcode: u16,
    /// Frame status code.
    pub status: u16,
    /// Encoded response payload.
    pub payload: Vec<u8>,
}

impl BatchResult {
    /// Returns whether the operation succeeded.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.status == status::OK && self.opcode != opcode::ERROR
    }

    /// Decodes the server error message for a failed operation.
    #[must_use]
    pub fn error_message(&self) -> Option<String> {
        if self.opcode != opcode::ERROR {
            return None;
        }

        ErrorResponse::decode(self.payload.as_slice())
            .ok()
            .map(|error| error.message)
    }
}
