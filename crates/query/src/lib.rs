//! RQL parsing, query binding and planning, and document reads.
#![forbid(unsafe_code)]

pub mod engine;
pub mod error;
pub mod ir;
pub mod request;
pub mod rql;
pub mod scan;

pub use engine::{QueryEngine, QueryPlan};
pub use error::{QueryError, Result};
pub use ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
pub use request::DocumentReadRequest;
pub use rql::{
    BindParameters, RqlBindError, RqlDocumentQuery, RqlError, RqlLiteral, RqlOp, RqlOperand,
    RqlPath, RqlPredicate, RqlSelector, bind_document_query, bind_query, parse_document_query,
};
pub use scan::{BoundQuery, DocumentScan, KeyFilter};
