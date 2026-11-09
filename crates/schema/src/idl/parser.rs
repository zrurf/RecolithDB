use recolith_common::{FieldId, SchemaId, SchemaVersion};

use super::lexer::{Token, TokenKind, tokenize};
use crate::error::{Result, SchemaError};
use crate::field::Field;
use crate::record::RecordSchema;
use crate::types::{EnumSchema, Type};

/// Parses a minimal record IDL.
///
/// # Errors
///
/// Returns [`SchemaError::Idl`] for malformed input and validation errors for invalid schemas.
pub fn parse_record(input: &str, id: SchemaId, version: SchemaVersion) -> Result<RecordSchema> {
    let tokens = tokenize(input)?;
    let mut parser = Parser {
        tokens,
        position: 0,
    };

    parser.parse(id, version)
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn parse(&mut self, id: SchemaId, version: SchemaVersion) -> Result<RecordSchema> {
        self.expect_keyword("record")?;
        let name = self.take_ident()?;
        self.expect(TokenKind::LBrace)?;

        let mut fields = Vec::new();
        while !matches!(self.peek(), TokenKind::RBrace) {
            fields.push(self.parse_field()?);
        }

        self.expect(TokenKind::RBrace)?;
        self.expect(TokenKind::Eof)?;

        let record = RecordSchema::new(id, version, name, fields);
        record.validate()?;

        Ok(record)
    }

    fn parse_field(&mut self) -> Result<Field> {
        let optional = self.eat_keyword("optional");
        let name = self.take_ident()?;
        self.expect(TokenKind::Colon)?;
        let ty = self.parse_type()?;
        self.expect(TokenKind::At)?;
        let id = FieldId::new(self.take_number()?);
        self.expect(TokenKind::Semicolon)?;

        Ok(Field::new(id, name, ty, optional))
    }

    fn parse_type(&mut self) -> Result<Type> {
        let name = self.take_ident()?;

        match name.as_str() {
            "bool" => Ok(Type::Bool),
            "i32" => Ok(Type::I32),
            "i64" => Ok(Type::I64),
            "u32" => Ok(Type::U32),
            "u64" => Ok(Type::U64),
            "f32" => Ok(Type::F32),
            "f64" => Ok(Type::F64),
            "string" => Ok(Type::String),
            "bytes" => Ok(Type::Bytes),
            "timestamp" => Ok(Type::Timestamp),
            "duration" => Ok(Type::Duration),
            "list" => {
                self.expect(TokenKind::Lt)?;
                let inner = self.parse_type()?;
                self.expect(TokenKind::Gt)?;
                Ok(Type::List(Box::new(inner)))
            }
            "map" => {
                self.expect(TokenKind::Lt)?;
                let key = self.parse_type()?;
                self.expect(TokenKind::Comma)?;
                let value = self.parse_type()?;
                self.expect(TokenKind::Gt)?;
                Ok(Type::Map(Box::new(key), Box::new(value)))
            }
            "enum" => self.parse_enum(),
            _ => Err(self.error(format!("unknown type `{name}`"))),
        }
    }

    fn parse_enum(&mut self) -> Result<Type> {
        let name = if matches!(self.peek(), TokenKind::Ident(_)) && self.next_is_lbrace() {
            self.take_ident()?
        } else {
            "anonymous".to_owned()
        };

        self.expect(TokenKind::LBrace)?;
        let mut variants = Vec::new();

        while !matches!(self.peek(), TokenKind::RBrace) {
            variants.push(self.take_ident()?);

            if self.eat_comma() {
                continue;
            }
            if !matches!(self.peek(), TokenKind::RBrace) {
                return Err(self.error("expected `,` or `}` after enum variant"));
            }
        }

        self.expect(TokenKind::RBrace)?;
        Ok(Type::Enum(EnumSchema::new(name, variants)))
    }

    fn next_is_lbrace(&self) -> bool {
        self.tokens
            .get(self.position + 1)
            .is_some_and(|token| matches!(token.kind, TokenKind::LBrace))
    }

    fn peek(&self) -> &TokenKind {
        &self.tokens[self.position].kind
    }

    fn take_ident(&mut self) -> Result<String> {
        let token = self.tokens[self.position].clone();
        match token.kind {
            TokenKind::Ident(value) => {
                self.position += 1;
                Ok(value)
            }
            _ => Err(SchemaError::Idl {
                offset: token.offset,
                message: format!("expected identifier, found {}", token.kind.description()),
            }),
        }
    }

    fn take_number(&mut self) -> Result<u32> {
        let token = self.tokens[self.position].clone();
        match token.kind {
            TokenKind::Number(value) => {
                self.position += 1;
                Ok(value)
            }
            _ => Err(SchemaError::Idl {
                offset: token.offset,
                message: format!("expected number, found {}", token.kind.description()),
            }),
        }
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        if matches!(self.peek(), TokenKind::Ident(value) if value == keyword) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn eat_comma(&mut self) -> bool {
        if matches!(self.peek(), TokenKind::Comma) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.eat_keyword(keyword) {
            Ok(())
        } else {
            Err(self.error(format!("expected keyword `{keyword}`")))
        }
    }

    fn expect(&mut self, expected: TokenKind) -> Result<()> {
        if self.peek() == &expected {
            self.position += 1;
            Ok(())
        } else {
            Err(self.error(format!(
                "expected {}, found {}",
                expected.description(),
                self.peek().description()
            )))
        }
    }

    fn error(&self, message: impl Into<String>) -> SchemaError {
        SchemaError::Idl {
            offset: self.tokens[self.position].offset,
            message: message.into(),
        }
    }
}
