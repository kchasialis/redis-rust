use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::string::ParseError;
use ordered_float::OrderedFloat;
use crate::resp_types::RespValue::NullArray;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RespKey {
    SimpleString(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Boolean(bool),
    Double(OrderedFloat<f64>),
    BigNumber(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
    NullArray,
    Boolean(bool),
    Double(OrderedFloat<f64>),
    BigNumber(String),
    BulkError(Vec<u8>),
    VerbatimString {
        format: String,
        content: Vec<u8>,
    },
    Map(HashMap<RespKey, RespValue>),
    Attribute(HashMap<RespKey, RespValue>),
    Set(HashSet<RespKey>),
    Push(Vec<RespValue>),
}

impl RespKey {
    fn from(val: RespValue) -> Self {
        match val {
            RespValue::SimpleString(v) => RespKey::SimpleString(v),
            RespValue::Integer(v) => RespKey::Integer(v),
            RespValue::BulkString(v) => RespKey::BulkString(v),
            RespValue::Boolean(v) => RespKey::Boolean(v),
            RespValue::Double(v) => RespKey::Double(v),
            RespValue::BigNumber(v) => RespKey::BigNumber(v),
            _ => panic!("Unsupported type for map key: {:?}", val),
        }
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            RespKey::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespKey::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespKey::BulkString(data) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespKey::Boolean(b) => {
                buf.push(b'#');
                buf.push(if *b { b't' } else { b'f' });
                buf.extend_from_slice(b"\r\n");
            }
            RespKey::Double(d) => {
                buf.push(b',');
                buf.extend_from_slice(d.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespKey::BigNumber(s) => {
                buf.push(b'(');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
    }
}

impl RespValue {
    pub fn deserialize(input: &[u8]) -> Result<RespValue> {
        Ok(parse_resp_bytes(input)?.0)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf);
        buf
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::SimpleError(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(data) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Array(arr) => {
                buf.push(b'*');
                buf.extend_from_slice(arr.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in arr {
                    elem.serialize_into(buf);
                }
            }
            RespValue::Null => {
                buf.extend_from_slice(b"_\r\n");
            }
            RespValue::NullArray => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Boolean(b) => {
                buf.push(b'#');
                buf.push(if *b { b't' } else { b'f' });
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Double(d) => {
                buf.push(b',');
                if d.is_infinite() && d.is_sign_positive() {
                    buf.extend_from_slice(b"inf");
                } else if d.is_infinite() && d.is_sign_negative() {
                    buf.extend_from_slice(b"-inf");
                } else if d.is_nan() {
                    buf.extend_from_slice(b"nan");
                } else {
                    buf.extend_from_slice(d.to_string().as_bytes());
                }
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BigNumber(s) => {
                buf.push(b'(');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkError(data) => {
                buf.push(b'!');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Map(map) => {
                buf.push(b'%');
                buf.extend_from_slice(map.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (key, value) in map {
                    key.serialize_into(buf);
                    value.serialize_into(buf);
                }
            }
            RespValue::Set(set) => {
                buf.push(b'~');
                buf.extend_from_slice(set.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for key in set {
                    key.serialize_into(buf);
                }
            }
            RespValue::VerbatimString { format, content } => {
                buf.push(b'=');
                let total_len = 3 + 1 + content.len(); // "fmt" + ":" + content
                buf.extend_from_slice(total_len.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format.as_bytes());
                buf.push(b':');
                buf.extend_from_slice(content);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Attribute(map) => {
                buf.push(b'|');
                buf.extend_from_slice(map.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (key, value) in map {
                    key.serialize_into(buf);
                    value.serialize_into(buf);
                }
            }
            RespValue::Push(arr) => {
                buf.push(b'>');
                buf.extend_from_slice(arr.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in arr {
                    elem.serialize_into(buf);
                }
            }
        }
    }
}

pub enum ParserError {
    Error(String)
}

impl ParserError {
    fn new(msg: &str) -> Self {
        ParserError::Error(msg.to_string())
    }
}

impl Debug for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::Error(str) => write!(f, "Failed to parse input: {}", str)
        }
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::Error(msg) => write!(f, "Failed to parse input: {}", msg)
        }
    }
}

impl std::error::Error for ParserError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub type Result<T> = std::result::Result<T, ParserError>;

fn parse_simple_str(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let end = input.iter()
        .position(|&b| b == b'\r')
        .ok_or(ParserError::new("Received wrong input at parse_simple_str"))?;

    let s = String::from_utf8_lossy(&input[1..end]).to_string();
    Ok((RespValue::SimpleString(s), &input[end + 2..]))
}

fn parse_simple_err(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let v = parse_simple_str(input)?;
    match v.0 {
        RespValue::SimpleString(s) => {
            Ok((RespValue::SimpleError(s), v.1))
        }
        _ => Err(ParserError::new("Failed to extract simple error from simple string"))
    }
}

fn parse_int(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let v = parse_simple_str(input)?;
    match v.0 {
        RespValue::SimpleString(s) => {
            let val = i64::from_str(s.as_str())
                .map_err(|_| ParserError::new("Invalid integer"))?;
            Ok((RespValue::Integer(val), v.1))
        }
        _ => Err(ParserError::new("Failed to extract integer from simple string"))
    }
}

fn parse_bulk_str(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (length, rest) = parse_int(input)?;
    match length {
        RespValue::Integer(length) => {
            let length = length as usize;
            let data_vec: Vec<u8> = Vec::from(&rest[0..length]);
            Ok((RespValue::BulkString(data_vec), &rest[length+2..]))
        }
        _ => Err(ParserError::new("Failed to extract string length for bulk string"))
    }
}

fn parse_array(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (length, mut rest) = parse_int(input)?;
    match length {
        RespValue::Integer(length) => {
            if length == -1 {
                return Ok((NullArray, rest))
            }

            let mut elements = Vec::new();
            for _ in 0..length {
                let (value, rem) = parse_resp_bytes(rest)?;
                elements.push(value);
                rest = rem;
            }
            Ok((RespValue::Array(elements), rest))
        }
        _ => Err(ParserError::new("Failed to extract array length for array type"))
    }
}

fn parse_null(input: &[u8]) -> Result<(RespValue, &[u8])> {
    Ok((RespValue::Null, &input[3..]))
}

fn parse_boolean(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (str_val, rest) = parse_simple_str(input)?;
    match str_val {
        RespValue::SimpleString(s) => {
            if s.eq("f") {
                Ok((RespValue::Boolean(false), rest))
            } else if s.eq("t") {
                Ok((RespValue::Boolean(true), rest))
            } else {
                Err(ParserError::new("Invalid value for boolean, expected t or f"))
            }
        }
        _ => Err(ParserError::new("Failed to parse boolean value"))
    }
}

fn parse_double(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (str_val, rest) = parse_simple_str(input)?;
    match str_val {
        RespValue::SimpleString(s) => {
            if s.eq("inf") {
                return Ok((RespValue::Double(OrderedFloat::from(f64::INFINITY)), rest));
            } else if s.eq("-inf") {
                return Ok((RespValue::Double(OrderedFloat::from(f64::NEG_INFINITY)), rest));
            } else if s.eq("nan") {
                return Ok((RespValue::Double(OrderedFloat::from(f64::NAN)), rest));
            }

            let val = f64::from_str(s.as_str()).unwrap();
            Ok((RespValue::Double(OrderedFloat::from(val)), rest))
        }
        _ => Err(ParserError::new("Failed to parse double value")),
    }
}

fn parse_big_number(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (str_val, rest) = parse_simple_str(input)?;
    match str_val {
        RespValue::SimpleString(s) => {
            Ok((RespValue::BigNumber(s), rest))
        }
        _ => Err(ParserError::new("Failed to parse double value")),
    }
}

fn parse_bulk_error(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (val_bulk_str, rest) = parse_bulk_str(input)?;
    match val_bulk_str {
        RespValue::BulkString(s) => {
            Ok((RespValue::BulkError(s), rest))
        }
        _ => Err(ParserError::new("Failed to extract simple error from simple string"))
    }
}
fn parse_verbatim_str(input: &[u8]) -> Result<(RespValue, &[u8])> {
    Err(ParserError::new("Verbatim string type not supported yet!"))
}

fn parse_map(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (length, mut rest) = parse_int(input)?;
    match length {
        RespValue::Integer(length) => {
            let mut elements = HashMap::new();
            for _ in 0..length {
                let (key, rem) = parse_resp_bytes(rest)?;
                let (value, rem) = parse_resp_bytes(rem)?;
                elements.insert(RespKey::from(key), value);
                rest = rem;
            }
            Ok((RespValue::Map(elements), rest))
        }
        _ => Err(ParserError::new("Failed to extract map length for map type"))
    }
}
fn parse_attribute(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (map, rest) = parse_map(input)?;
    match map {
        RespValue::Map(map) => {
            Ok((RespValue::Attribute(map), rest))
        },
        _ => Err(ParserError::new("Failed to parse map"))
    }
}
fn parse_set(input: &[u8]) -> Result<(RespValue, &[u8])> {
    let (arr, rest) = parse_array(input)?;
    match arr {
        RespValue::Array(arr) => {
            let mut set = HashSet::new();
            for elem in arr {
                set.insert(RespKey::from(elem));
            }
            Ok((RespValue::Set(set), rest))
        }
        _ => Err(ParserError::new("Failed to parse set"))
    }
}
fn parse_push(input: &[u8]) -> Result<(RespValue, &[u8])> {
    Err(ParserError::new("Push type supported yet!"))
}

fn parse_resp_bytes(input: &[u8]) -> Result<(RespValue, &[u8])> {
    match input[0] {
        b'+' => {
            parse_simple_str(input)
        },
        b'-' => {
            parse_simple_err(input)
        },
        b':' => {
            parse_int(input)
        },
        b'$' => {
            parse_bulk_str(input)
        },
        b'*' => {
            parse_array(input)
        },
        b'_' => {
            parse_null(input)
        },
        b'#' => {
            parse_boolean(input)
        },
        b',' => {
            parse_double(input)
        },
        b'(' => {
            parse_big_number(input)
        },
        b'!' => {
            parse_bulk_error(input)
        },
        b'=' => {
            parse_verbatim_str(input)
        },
        b'%' => {
            parse_map(input)
        },
        b'|' => {
            parse_attribute(input)
        },
        b'~' => {
            parse_set(input)
        },
        b'>' => {
            parse_push(input)
        }
        _ => Err(ParserError::new("Unknown type byte"))
    }
}