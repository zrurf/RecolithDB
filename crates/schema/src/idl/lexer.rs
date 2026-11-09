use crate::error::{Result, SchemaError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Ident(String),
    Number(u32),
    LBrace,
    RBrace,
    Colon,
    At,
    Semicolon,
    Comma,
    Lt,
    Gt,
    Eof,
}

impl TokenKind {
    pub(crate) fn description(&self) -> String {
        match self {
            Self::Ident(value) => format!("identifier `{value}`"),
            Self::Number(value) => format!("number `{value}`"),
            Self::LBrace => "`{`".to_owned(),
            Self::RBrace => "`}`".to_owned(),
            Self::Colon => "`:`".to_owned(),
            Self::At => "`@`".to_owned(),
            Self::Semicolon => "`;`".to_owned(),
            Self::Comma => "`,`".to_owned(),
            Self::Lt => "`<`".to_owned(),
            Self::Gt => "`>`".to_owned(),
            Self::Eof => "end of input".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) offset: usize,
}

pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();

    while let Some((offset, character)) = chars.next() {
        if character.is_whitespace() {
            continue;
        }

        if character == '/' && chars.peek().is_some_and(|(_, next)| *next == '/') {
            for (_, next) in chars.by_ref() {
                if next == '\n' {
                    break;
                }
            }
            continue;
        }

        let kind = if character.is_ascii_alphabetic() || character == '_' {
            let mut value = String::from(character);
            while let Some((_, next)) = chars.peek() {
                if next.is_ascii_alphanumeric() || *next == '_' {
                    value.push(*next);
                    chars.next();
                } else {
                    break;
                }
            }
            TokenKind::Ident(value)
        } else if character.is_ascii_digit() {
            let mut value = String::from(character);
            while let Some((_, next)) = chars.peek() {
                if next.is_ascii_digit() {
                    value.push(*next);
                    chars.next();
                } else {
                    break;
                }
            }
            let number = value.parse::<u32>().map_err(|_| SchemaError::Idl {
                offset,
                message: "field ID overflows u32".to_owned(),
            })?;
            TokenKind::Number(number)
        } else {
            match character {
                '{' => TokenKind::LBrace,
                '}' => TokenKind::RBrace,
                ':' => TokenKind::Colon,
                '@' => TokenKind::At,
                ';' => TokenKind::Semicolon,
                ',' => TokenKind::Comma,
                '<' => TokenKind::Lt,
                '>' => TokenKind::Gt,
                _ => {
                    return Err(SchemaError::Idl {
                        offset,
                        message: format!("unexpected character `{character}`"),
                    });
                }
            }
        };

        tokens.push(Token { kind, offset });
    }

    tokens.push(Token {
        kind: TokenKind::Eof,
        offset: input.len(),
    });

    Ok(tokens)
}
