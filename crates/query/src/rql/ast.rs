/// Comparison operator in RQL text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RqlOp {
    /// Equality.
    Eq,
    /// Inequality.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
}

/// Literal operand in RQL text.
#[derive(Clone, Debug, PartialEq)]
pub enum RqlLiteral {
    /// Signed integer.
    Integer(i64),
    /// Unsigned integer above the signed range.
    Unsigned(u64),
    /// Floating-point number.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Boolean.
    Bool(bool),
}

/// Operand in an RQL predicate.
#[derive(Clone, Debug, PartialEq)]
pub enum RqlOperand {
    /// Inline literal.
    Literal(RqlLiteral),
    /// Named bind parameter.
    Parameter(String),
}

/// Inline record selector on a `many` fragment.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlSelector {
    field: String,
    op: RqlOp,
    operand: RqlOperand,
}

impl RqlSelector {
    /// Creates a selector.
    #[must_use]
    pub fn new(field: impl Into<String>, op: RqlOp, operand: RqlOperand) -> Self {
        Self {
            field: field.into(),
            op,
            operand,
        }
    }

    /// Returns the selector field name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the selector comparison operator.
    #[must_use]
    pub fn op(&self) -> RqlOp {
        self.op
    }

    /// Returns the selector operand.
    #[must_use]
    pub fn operand(&self) -> &RqlOperand {
        &self.operand
    }
}

/// Fragment path such as `profile.level`, `equipment[slot = 1].item_id`, or an
/// unqualified document-key field such as `player_id`.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlPath {
    fragment: Option<String>,
    selector: Option<RqlSelector>,
    field: String,
}

impl RqlPath {
    /// Creates a fragment path.
    #[must_use]
    pub fn new(
        fragment: Option<String>,
        selector: Option<RqlSelector>,
        field: impl Into<String>,
    ) -> Self {
        Self {
            fragment,
            selector,
            field: field.into(),
        }
    }

    /// Returns the fragment name, or `None` for an unqualified key field.
    #[must_use]
    pub fn fragment(&self) -> Option<&str> {
        self.fragment.as_deref()
    }

    /// Returns the inline selector, if any.
    #[must_use]
    pub fn selector(&self) -> Option<&RqlSelector> {
        self.selector.as_ref()
    }

    /// Returns the field name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }
}

/// Predicate over one fragment field.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlPredicate {
    path: RqlPath,
    op: RqlOp,
    operand: RqlOperand,
}

impl RqlPredicate {
    /// Creates a predicate.
    #[must_use]
    pub fn new(path: RqlPath, op: RqlOp, operand: RqlOperand) -> Self {
        Self { path, op, operand }
    }

    /// Returns the predicate path.
    #[must_use]
    pub fn path(&self) -> &RqlPath {
        &self.path
    }

    /// Returns the comparison operator.
    #[must_use]
    pub fn op(&self) -> RqlOp {
        self.op
    }

    /// Returns the predicate operand.
    #[must_use]
    pub fn operand(&self) -> &RqlOperand {
        &self.operand
    }
}

/// Ordering clause over one fragment field.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlOrder {
    path: RqlPath,
    descending: bool,
}

impl RqlOrder {
    /// Creates an ordering clause.
    #[must_use]
    pub fn new(path: RqlPath, descending: bool) -> Self {
        Self { path, descending }
    }

    /// Returns the ordered path.
    #[must_use]
    pub fn path(&self) -> &RqlPath {
        &self.path
    }

    /// Returns whether ordering is descending.
    #[must_use]
    pub fn descending(&self) -> bool {
        self.descending
    }
}

/// Aggregate function in RQL text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RqlAggregateFunction {
    /// Counts matching documents.
    Count,
    /// Sums numeric field values.
    Sum,
    /// Returns the minimum comparable field value.
    Min,
    /// Returns the maximum comparable field value.
    Max,
    /// Returns the arithmetic mean of numeric field values.
    Avg,
}

/// One aggregate expression.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlAggregate {
    function: RqlAggregateFunction,
    path: Option<RqlPath>,
}

impl RqlAggregate {
    /// Creates an aggregate expression.
    #[must_use]
    pub fn new(function: RqlAggregateFunction, path: Option<RqlPath>) -> Self {
        Self { function, path }
    }

    /// Returns the aggregate function.
    #[must_use]
    pub const fn function(&self) -> RqlAggregateFunction {
        self.function
    }

    /// Returns the aggregated path; `COUNT(*)` has none.
    #[must_use]
    pub fn path(&self) -> Option<&RqlPath> {
        self.path.as_ref()
    }
}

/// Parsed aggregate query.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlAggregateQuery {
    aggregates: Vec<RqlAggregate>,
    collection: String,
    predicates: Vec<RqlPredicate>,
    fragments: Vec<String>,
    group_by: Option<RqlPath>,
    limit: Option<u64>,
}

impl RqlAggregateQuery {
    /// Creates an aggregate query.
    #[must_use]
    pub fn new(
        aggregates: Vec<RqlAggregate>,
        collection: impl Into<String>,
        predicates: Vec<RqlPredicate>,
        fragments: Vec<String>,
        group_by: Option<RqlPath>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            aggregates,
            collection: collection.into(),
            predicates,
            fragments,
            group_by,
            limit,
        }
    }

    /// Returns the aggregate expressions.
    #[must_use]
    pub fn aggregates(&self) -> &[RqlAggregate] {
        &self.aggregates
    }

    /// Returns the collection name.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Returns the predicates.
    #[must_use]
    pub fn predicates(&self) -> &[RqlPredicate] {
        &self.predicates
    }

    /// Returns the included fragment names.
    #[must_use]
    pub fn fragments(&self) -> &[String] {
        &self.fragments
    }

    /// Returns the grouping path, if any.
    #[must_use]
    pub fn group_by(&self) -> Option<&RqlPath> {
        self.group_by.as_ref()
    }

    /// Returns the group limit.
    #[must_use]
    pub fn limit(&self) -> Option<u64> {
        self.limit
    }
}

/// Parsed document query.
#[derive(Clone, Debug, PartialEq)]
pub struct RqlDocumentQuery {
    projections: Vec<RqlPath>,
    collection: String,
    predicates: Vec<RqlPredicate>,
    fragments: Vec<String>,
    order: Option<RqlOrder>,
    limit: Option<u64>,
}

impl RqlDocumentQuery {
    /// Creates a parsed document query.
    #[must_use]
    pub fn new(
        projections: Vec<RqlPath>,
        collection: impl Into<String>,
        predicates: Vec<RqlPredicate>,
        fragments: Vec<String>,
        order: Option<RqlOrder>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            projections,
            collection: collection.into(),
            predicates,
            fragments,
            order,
            limit,
        }
    }

    /// Returns the projected paths.
    #[must_use]
    pub fn projections(&self) -> &[RqlPath] {
        &self.projections
    }

    /// Returns the collection name.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Returns the predicates.
    #[must_use]
    pub fn predicates(&self) -> &[RqlPredicate] {
        &self.predicates
    }

    /// Returns the included fragment names.
    #[must_use]
    pub fn fragments(&self) -> &[String] {
        &self.fragments
    }

    /// Returns the ordering clause, if any.
    #[must_use]
    pub fn order(&self) -> Option<&RqlOrder> {
        self.order.as_ref()
    }

    /// Returns the result limit.
    #[must_use]
    pub fn limit(&self) -> Option<u64> {
        self.limit
    }
}
