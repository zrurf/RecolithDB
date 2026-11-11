use std::fmt;

use pest::Parser;
use pest::iterators::{Pair, Pairs};
use pest_derive::Parser;

use super::ast::{
    RqlAggregate, RqlAggregateFunction, RqlAggregateQuery, RqlDocumentQuery, RqlLiteral, RqlOp,
    RqlOperand, RqlOrder, RqlPath, RqlPredicate, RqlSelector,
};

#[derive(Parser)]
#[grammar = "rql/grammar.pest"]
struct RqlParser;

/// RQL parsing error.
#[derive(Debug)]
pub enum RqlError {
    /// Input does not match the grammar.
    Syntax {
        /// One-based line.
        line: usize,
        /// One-based column.
        column: usize,
        /// Parser message.
        message: String,
    },
    /// Input matches the grammar but violates a semantic rule.
    Invalid(String),
}

impl fmt::Display for RqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax {
                line,
                column,
                message,
            } => write!(
                formatter,
                "RQL syntax error at line {line}, column {column}: {message}"
            ),
            Self::Invalid(message) => write!(formatter, "invalid RQL: {message}"),
        }
    }
}

impl std::error::Error for RqlError {}

impl From<pest::error::Error<Rule>> for RqlError {
    fn from(error: pest::error::Error<Rule>) -> Self {
        let (line, column) = match error.line_col {
            pest::error::LineColLocation::Pos(position) => position,
            pest::error::LineColLocation::Span(position, _) => position,
        };

        Self::Syntax {
            line,
            column,
            message: error.variant.message().into_owned(),
        }
    }
}

type ParseResult<T> = std::result::Result<T, RqlError>;

/// Parses a document read query.
///
/// The grammar is a strict subset of the RQL surface: projections, fragment
/// paths, inline `many` selectors, conjunctive predicates, bind parameters,
/// the mandatory `INCLUDE FRAGMENTS` clause, and optional `ORDER BY`/`LIMIT`.
///
/// # Errors
///
/// Returns [`RqlError::Syntax`] for malformed text and [`RqlError::Invalid`] for
/// grammatically valid paths that do not select a field.
pub fn parse_document_query(input: &str) -> ParseResult<RqlDocumentQuery> {
    let mut pairs = RqlParser::parse(Rule::document_query, input)?;
    let query = pairs.next().ok_or_else(|| invalid("query is empty"))?;
    let mut inner = query.into_inner();

    let projections = parse_projections(expect(&mut inner, Rule::projection_list)?)?;
    let collection = expect(&mut inner, Rule::ident)?.as_str().to_owned();
    let predicates = parse_predicates(expect(&mut inner, Rule::predicate_list)?)?;
    let fragments = parse_fragments(expect(&mut inner, Rule::fragment_list)?);
    let mut order = None;
    let mut limit = None;
    while let Some(pair) = next_significant(&mut inner) {
        match pair.as_rule() {
            Rule::order_clause if order.is_none() => order = Some(parse_order(pair)?),
            Rule::integer if limit.is_none() => limit = Some(parse_limit(pair.as_str())?),
            other => {
                return Err(invalid(format!("unexpected query suffix: {other:?}")));
            }
        }
    }

    Ok(RqlDocumentQuery::new(
        projections,
        collection,
        predicates,
        fragments,
        order,
        limit,
    ))
}

/// Parses an aggregate query.
///
/// Aggregate queries support `COUNT(*)`, `SUM`, `MIN`, `MAX`, and `AVG`,
/// optional single-field `GROUP BY`, and `LIMIT` over groups.
///
/// # Errors
///
/// Returns [`RqlError::Syntax`] for malformed text and [`RqlError::Invalid`] for
/// malformed aggregate expressions.
pub fn parse_aggregate_query(input: &str) -> ParseResult<RqlAggregateQuery> {
    let mut pairs = RqlParser::parse(Rule::aggregate_query, input)?;
    let query = pairs.next().ok_or_else(|| invalid("query is empty"))?;
    let mut inner = query.into_inner();

    let aggregates = parse_aggregates(expect(&mut inner, Rule::aggregate_list)?)?;
    let collection = expect(&mut inner, Rule::ident)?.as_str().to_owned();
    let predicates = parse_predicates(expect(&mut inner, Rule::predicate_list)?)?;
    let fragments = parse_fragments(expect(&mut inner, Rule::fragment_list)?);
    let mut group_by = None;
    let mut limit = None;
    while let Some(pair) = next_significant(&mut inner) {
        match pair.as_rule() {
            Rule::group_clause if group_by.is_none() => group_by = Some(parse_group(pair)?),
            Rule::integer if limit.is_none() => limit = Some(parse_limit(pair.as_str())?),
            other => {
                return Err(invalid(format!("unexpected query suffix: {other:?}")));
            }
        }
    }

    Ok(RqlAggregateQuery::new(
        aggregates, collection, predicates, fragments, group_by, limit,
    ))
}

fn parse_aggregates(pair: Pair<'_, Rule>) -> ParseResult<Vec<RqlAggregate>> {
    pair.into_inner()
        .map(|aggregate| {
            let part = aggregate
                .into_inner()
                .next()
                .ok_or_else(|| invalid("aggregate expression is empty"))?;
            match part.as_rule() {
                Rule::aggregate_count => Ok(RqlAggregate::new(RqlAggregateFunction::Count, None)),
                Rule::aggregate_call => {
                    let mut inner = part.into_inner();
                    let name = inner
                        .next()
                        .ok_or_else(|| invalid("aggregate name is missing"))?;
                    let keyword = name
                        .into_inner()
                        .next()
                        .ok_or_else(|| invalid("aggregate name is empty"))?;
                    let function = match keyword.as_rule() {
                        Rule::keyword_sum => RqlAggregateFunction::Sum,
                        Rule::keyword_min => RqlAggregateFunction::Min,
                        Rule::keyword_max => RqlAggregateFunction::Max,
                        Rule::keyword_avg => RqlAggregateFunction::Avg,
                        other => {
                            return Err(invalid(format!("unknown aggregate function: {other:?}")));
                        }
                    };
                    let path = parse_path(expect(&mut inner, Rule::path)?, false)?;
                    Ok(RqlAggregate::new(function, Some(path)))
                }
                other => Err(invalid(format!("unexpected aggregate part: {other:?}"))),
            }
        })
        .collect()
}

fn parse_group(pair: Pair<'_, Rule>) -> ParseResult<RqlPath> {
    let mut inner = pair.into_inner();
    parse_path(expect(&mut inner, Rule::path)?, false)
}

fn parse_order(pair: Pair<'_, Rule>) -> ParseResult<RqlOrder> {
    let mut inner = pair.into_inner();
    let path = parse_path(expect(&mut inner, Rule::path)?, false)?;
    let descending = match inner.find(|part| part.as_rule() == Rule::order_direction) {
        Some(direction) => {
            let keyword = direction
                .into_inner()
                .next()
                .ok_or_else(|| invalid("order direction is empty"))?;
            match keyword.as_rule() {
                Rule::keyword_desc => true,
                Rule::keyword_asc => false,
                other => {
                    return Err(invalid(format!("unexpected order direction: {other:?}")));
                }
            }
        }
        None => false,
    };

    Ok(RqlOrder::new(path, descending))
}

fn parse_projections(pair: Pair<'_, Rule>) -> ParseResult<Vec<RqlPath>> {
    pair.into_inner()
        .map(|projection| {
            let path = projection
                .into_inner()
                .next()
                .ok_or_else(|| invalid("projection is empty"))?;
            parse_path(path, true)
        })
        .collect()
}

fn parse_predicates(pair: Pair<'_, Rule>) -> ParseResult<Vec<RqlPredicate>> {
    pair.into_inner()
        .filter(|predicate| !is_keyword(predicate.as_rule()))
        .map(parse_predicate)
        .collect()
}

fn parse_predicate(pair: Pair<'_, Rule>) -> ParseResult<RqlPredicate> {
    let mut inner = pair.into_inner();
    let path = parse_path(expect(&mut inner, Rule::path)?, false)?;
    let op = parse_op(expect(&mut inner, Rule::comparison)?)?;
    let operand = parse_operand(expect(&mut inner, Rule::operand)?)?;

    Ok(RqlPredicate::new(path, op, operand))
}

fn parse_path(pair: Pair<'_, Rule>, require_fragment: bool) -> ParseResult<RqlPath> {
    let mut inner = pair.into_inner();
    let first = expect(&mut inner, Rule::ident)?.as_str().to_owned();
    let mut selector = None;
    let mut second = None;

    for part in inner {
        match part.as_rule() {
            Rule::selector => selector = Some(parse_selector(part)?),
            Rule::ident => second = Some(part.as_str().to_owned()),
            other => return Err(invalid(format!("unexpected path part: {other:?}"))),
        }
    }

    match (second, selector) {
        (Some(field), selector) => Ok(RqlPath::new(Some(first), selector, field)),
        (None, Some(_)) => Err(invalid("selector requires a fragment field path")),
        (None, None) if require_fragment => Err(invalid("projection must name a fragment field")),
        (None, None) => Ok(RqlPath::new(None, None, first)),
    }
}

fn parse_selector(pair: Pair<'_, Rule>) -> ParseResult<RqlSelector> {
    let mut inner = pair.into_inner();
    let field = expect(&mut inner, Rule::ident)?.as_str().to_owned();
    let op = parse_op(expect(&mut inner, Rule::comparison)?)?;
    let operand = parse_operand(expect(&mut inner, Rule::operand)?)?;

    Ok(RqlSelector::new(field, op, operand))
}

fn parse_op(pair: Pair<'_, Rule>) -> ParseResult<RqlOp> {
    match pair.as_str() {
        "=" => Ok(RqlOp::Eq),
        "!=" => Ok(RqlOp::Ne),
        "<" => Ok(RqlOp::Lt),
        "<=" => Ok(RqlOp::Le),
        ">" => Ok(RqlOp::Gt),
        ">=" => Ok(RqlOp::Ge),
        other => Err(invalid(format!("unknown comparison operator: {other}"))),
    }
}

fn parse_operand(pair: Pair<'_, Rule>) -> ParseResult<RqlOperand> {
    let part = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("operand is empty"))?;

    match part.as_rule() {
        Rule::parameter => {
            let name = part
                .into_inner()
                .next()
                .ok_or_else(|| invalid("parameter is empty"))?
                .as_str()
                .to_owned();
            Ok(RqlOperand::Parameter(name))
        }
        Rule::literal => Ok(RqlOperand::Literal(parse_literal(part)?)),
        other => Err(invalid(format!("unexpected operand: {other:?}"))),
    }
}

fn parse_literal(pair: Pair<'_, Rule>) -> ParseResult<RqlLiteral> {
    let part = pair
        .into_inner()
        .next()
        .ok_or_else(|| invalid("literal is empty"))?;

    match part.as_rule() {
        Rule::string => Ok(RqlLiteral::String(unescape(part.as_str())?)),
        Rule::float => part
            .as_str()
            .parse::<f64>()
            .map(RqlLiteral::Float)
            .map_err(|_| invalid("float literal is out of range")),
        Rule::integer => parse_integer(part.as_str()),
        Rule::boolean => Ok(RqlLiteral::Bool(part.as_str().eq_ignore_ascii_case("true"))),
        other => Err(invalid(format!("unexpected literal: {other:?}"))),
    }
}

fn parse_limit(raw: &str) -> ParseResult<u64> {
    if raw.starts_with('-') {
        return Err(invalid("limit must not be negative"));
    }

    raw.parse::<u64>()
        .map_err(|_| invalid("limit is out of range"))
}

fn parse_integer(raw: &str) -> ParseResult<RqlLiteral> {
    if raw.starts_with('-') {
        return raw
            .parse::<i64>()
            .map(RqlLiteral::Integer)
            .map_err(|_| invalid("integer literal is out of range"));
    }

    match raw.parse::<i64>() {
        Ok(value) => Ok(RqlLiteral::Integer(value)),
        Err(_) => raw
            .parse::<u64>()
            .map(RqlLiteral::Unsigned)
            .map_err(|_| invalid("integer literal is out of range")),
    }
}

fn unescape(raw: &str) -> ParseResult<String> {
    let inner = raw
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .ok_or_else(|| invalid("string literal is not quoted"))?;
    let mut output = String::with_capacity(inner.len());
    let mut chars = inner.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }

        match chars.next() {
            Some('\'') => output.push('\''),
            Some('\\') => output.push('\\'),
            Some(other) => return Err(invalid(format!("unsupported escape: \\{other}"))),
            None => return Err(invalid("string literal ends with an escape")),
        }
    }

    Ok(output)
}

fn parse_fragments(pair: Pair<'_, Rule>) -> Vec<String> {
    pair.into_inner()
        .map(|fragment| fragment.as_str().to_owned())
        .collect()
}

fn expect<'a>(pairs: &mut Pairs<'a, Rule>, rule: Rule) -> ParseResult<Pair<'a, Rule>> {
    match next_significant(pairs) {
        Some(pair) if pair.as_rule() == rule => Ok(pair),
        Some(pair) => Err(invalid(format!(
            "expected {rule:?}, found {:?}",
            pair.as_rule()
        ))),
        None => Err(invalid(format!("expected {rule:?}, found end of input"))),
    }
}

fn next_significant<'a>(pairs: &mut Pairs<'a, Rule>) -> Option<Pair<'a, Rule>> {
    pairs.by_ref().find(|pair| !is_keyword(pair.as_rule()))
}

fn is_keyword(rule: Rule) -> bool {
    matches!(
        rule,
        Rule::EOI
            | Rule::keyword_select
            | Rule::keyword_from
            | Rule::keyword_where
            | Rule::keyword_and
            | Rule::keyword_include
            | Rule::keyword_fragments
            | Rule::keyword_limit
            | Rule::keyword_order
            | Rule::keyword_by
            | Rule::keyword_asc
            | Rule::keyword_desc
            | Rule::keyword_group
    )
}

fn invalid(message: impl Into<String>) -> RqlError {
    RqlError::Invalid(message.into())
}
