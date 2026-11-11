//! RQL parsing, query binding and planning, and document reads.
#![forbid(unsafe_code)]

pub mod aggregate;
mod compare;
pub mod engine;
pub mod error;
pub mod ir;
pub mod request;
pub mod rql;
pub mod scan;

pub use aggregate::{
    AggregateBuilder, AggregateExpression, AggregateFunction, AggregateGroup, AggregatePage,
    AggregateQuery, AggregateRow, MAX_AGGREGATE_GROUPS,
};
pub use engine::{QueryEngine, QueryPlan, ScanDriveInfo, ScanPage, ScanPlan, ScanPosition};
pub use error::{QueryError, Result};
pub use ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
pub use request::DocumentReadRequest;
pub use rql::{
    BindParameters, RqlAggregate, RqlAggregateFunction, RqlAggregateQuery, RqlBindError,
    RqlDocumentQuery, RqlError, RqlLiteral, RqlOp, RqlOperand, RqlOrder, RqlPath, RqlPredicate,
    RqlSelector, bind_aggregate_query, bind_document_query, bind_query, parse_aggregate_query,
    parse_document_query,
};
pub use scan::{BoundQuery, DocumentScan, KeyFilter, ScanOrder};
