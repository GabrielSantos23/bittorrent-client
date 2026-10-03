use std::collections::BTreeMap;

use crate::error::BencodeError;

const MAX_DEPTH: usize = 128;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Value::Dict(entries) => Some(entries),
            _ => None,
        }
    }
}

pub fn decode(input: &[u8]) -> Result<Value, BencodeError> {
    let (value, end) = decode_value(input, 0, 0)?;
    if end != input.len() {
        return Err(BencodeError::TrailingBytes(end));
    }
    Ok(value)
}

pub fn decode_prefix(input: &[u8]) -> Result<(Value, usize), BencodeError> {
    decode_value(input, 0, 0)
}

pub fn encode_into(value: &Value, out: &mut Vec<u8>) {
    write_value(value, out);
}

pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(value, &mut out);
    out
}

pub fn info_dict_bytes(raw: &[u8]) -> Result<Option<&[u8]>, BencodeError> {
    match raw.first() {
        Some(&b'd') => {}
        _ => return Err(BencodeError::InvalidMarker(0)),
    }
    let mut pos = 1;
    loop {
        match raw.get(pos) {
            Some(&b'e') => return Ok(None),
            None => return Err(BencodeError::UnexpectedEof(pos)),
            Some(_) => {}
        }
        let (key, next) = decode_value(raw, pos, 1)?;
        let Some(key_bytes) = key.as_bytes() else {
            return Err(BencodeError::InvalidKey(pos));
        };
        let start = next;
        let (_, end) = decode_value(raw, next, 1)?;
        if key_bytes == b"info" {
            return Ok(Some(&raw[start..end]));
        }
        pos = end;
    }
}

fn decode_value(input: &[u8], pos: usize, depth: usize) -> Result<(Value, usize), BencodeError> {
    if depth > MAX_DEPTH {
        return Err(BencodeError::MaxDepthExceeded(pos));
    }
    match input.get(pos) {
        Some(&b'i') => decode_int(input, pos),
        Some(&b'l') => decode_list(input, pos, depth),
        Some(&b'd') => decode_dict(input, pos, depth),
        Some(&byte) if byte.is_ascii_digit() => decode_bytes(input, pos),
        Some(_) => Err(BencodeError::InvalidMarker(pos)),
        None => Err(BencodeError::UnexpectedEof(pos)),
    }
}

fn decode_int(input: &[u8], pos: usize) -> Result<(Value, usize), BencodeError> {
    let start = pos;
    let mut cursor = pos + 1;
    let negative = input.get(cursor) == Some(&b'-');
    if negative {
        cursor += 1;
    }
    let digits_start = cursor;
    while matches!(input.get(cursor), Some(&byte) if byte.is_ascii_digit()) {
        cursor += 1;
    }
    if input.get(cursor) != Some(&b'e') {
        return Err(BencodeError::InvalidInteger(start));
    }
    let digits = &input[digits_start..cursor];
    if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') || (negative && digits == b"0")
    {
        return Err(BencodeError::InvalidInteger(start));
    }
    let text = std::str::from_utf8(digits).map_err(|_| BencodeError::InvalidInteger(start))?;
    let magnitude: i64 = text
        .parse()
        .map_err(|_| BencodeError::InvalidInteger(start))?;
    let value = if negative { -magnitude } else { magnitude };
    Ok((Value::Int(value), cursor + 1))
}

fn decode_bytes(input: &[u8], pos: usize) -> Result<(Value, usize), BencodeError> {
    let start = pos;
    let mut cursor = pos;
    while matches!(input.get(cursor), Some(&byte) if byte.is_ascii_digit()) {
        cursor += 1;
    }
    if input.get(cursor) != Some(&b':') {
        return Err(BencodeError::InvalidStringLength(start));
    }
    let digits = &input[start..cursor];
    if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
        return Err(BencodeError::InvalidStringLength(start));
    }
    let text = std::str::from_utf8(digits).map_err(|_| BencodeError::InvalidStringLength(start))?;
    let length: usize = text
        .parse()
        .map_err(|_| BencodeError::InvalidStringLength(start))?;
    let data_start = cursor + 1;
    let data_end = data_start
        .checked_add(length)
        .ok_or(BencodeError::StringTooLong(length))?;
    if data_end > input.len() {
        return Err(BencodeError::StringTooLong(length));
    }
    Ok((Value::Bytes(input[data_start..data_end].to_vec()), data_end))
}

fn decode_list(input: &[u8], pos: usize, depth: usize) -> Result<(Value, usize), BencodeError> {
    let mut items = Vec::new();
    let mut cursor = pos + 1;
    loop {
        match input.get(cursor) {
            Some(&b'e') => return Ok((Value::List(items), cursor + 1)),
            Some(_) => {
                let (item, next) = decode_value(input, cursor, depth + 1)?;
                items.push(item);
                cursor = next;
            }
            None => return Err(BencodeError::UnexpectedEof(cursor)),
        }
    }
}

fn decode_dict(input: &[u8], pos: usize, depth: usize) -> Result<(Value, usize), BencodeError> {
    let mut entries = BTreeMap::new();
    let mut cursor = pos + 1;
    loop {
        match input.get(cursor) {
            Some(&b'e') => return Ok((Value::Dict(entries), cursor + 1)),
            Some(&byte) if byte.is_ascii_digit() => {
                let (key, next) = decode_value(input, cursor, depth + 1)?;
                let Some(key_bytes) = key.as_bytes() else {
                    return Err(BencodeError::InvalidKey(cursor));
                };
                if entries.contains_key(key_bytes) {
                    return Err(BencodeError::DuplicateKey(cursor));
                }
                let (value, next) = decode_value(input, next, depth + 1)?;
                entries.insert(key_bytes.to_vec(), value);
                cursor = next;
            }
            Some(_) => return Err(BencodeError::InvalidKey(cursor)),
            None => return Err(BencodeError::UnexpectedEof(cursor)),
        }
    }
}

fn write_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(number) => {
            out.push(b'i');
            out.extend_from_slice(number.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(bytes) => {
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(bytes);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                write_value(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(entries) => {
            out.push(b'd');
            for (key, item) in entries {
                out.extend_from_slice(key.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(key);
                write_value(item, out);
            }
            out.push(b'e');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(entries: &[(&str, Value)]) -> Value {
        Value::Dict(
            entries
                .iter()
                .map(|(key, value)| (key.as_bytes().to_vec(), value.clone()))
                .collect(),
        )
    }

    #[test]
    fn decodes_integers() {
        assert_eq!(decode(b"i0e").unwrap(), Value::Int(0));
        assert_eq!(decode(b"i42e").unwrap(), Value::Int(42));
        assert_eq!(decode(b"i-7e").unwrap(), Value::Int(-7));
        assert_eq!(
            decode(b"i9223372036854775807e").unwrap(),
            Value::Int(i64::MAX)
        );
    }

    #[test]
    fn rejects_malformed_integers() {
        assert!(matches!(
            decode(b"ie"),
            Err(BencodeError::InvalidInteger(0))
        ));
        assert!(matches!(
            decode(b"i-e"),
            Err(BencodeError::InvalidInteger(0))
        ));
        assert!(matches!(
            decode(b"i-0e"),
            Err(BencodeError::InvalidInteger(0))
        ));
        assert!(matches!(
            decode(b"i07e"),
            Err(BencodeError::InvalidInteger(0))
        ));
        assert!(matches!(
            decode(b"i1x"),
            Err(BencodeError::InvalidInteger(0))
        ));
        assert!(matches!(
            decode(b"i9223372036854775808e"),
            Err(BencodeError::InvalidInteger(0))
        ));
    }

    #[test]
    fn decodes_byte_strings() {
        assert_eq!(decode(b"0:").unwrap(), Value::Bytes(Vec::new()));
        assert_eq!(decode(b"4:spam").unwrap(), Value::Bytes(b"spam".to_vec()));
        assert_eq!(
            decode(b"12:hello\0world!").unwrap(),
            Value::Bytes(b"hello\0world!".to_vec())
        );
    }

    #[test]
    fn rejects_malformed_byte_strings() {
        assert!(matches!(
            decode(b"4:spa"),
            Err(BencodeError::StringTooLong(4))
        ));
        assert!(matches!(
            decode(b"5:spam"),
            Err(BencodeError::StringTooLong(5))
        ));
        assert!(matches!(
            decode(b"-1:a"),
            Err(BencodeError::InvalidMarker(0))
        ));
        assert!(matches!(
            decode(b"01:a"),
            Err(BencodeError::InvalidStringLength(0))
        ));
        assert!(matches!(
            decode(b"99999999999999999999999:x"),
            Err(BencodeError::InvalidStringLength(0))
        ));
    }

    #[test]
    fn decodes_lists() {
        assert_eq!(decode(b"le").unwrap(), Value::List(Vec::new()));
        assert_eq!(
            decode(b"l4:spami42ee").unwrap(),
            Value::List(vec![Value::Bytes(b"spam".to_vec()), Value::Int(42)])
        );
        assert_eq!(
            decode(b"lli1eee").unwrap(),
            Value::List(vec![Value::List(vec![Value::Int(1)])])
        );
        assert!(matches!(decode(b"l"), Err(BencodeError::UnexpectedEof(1))));
    }

    #[test]
    fn decodes_dictionaries() {
        let value = decode(b"d3:bari2e3:foo4:spame").unwrap();
        assert_eq!(
            value,
            dict(&[
                ("bar", Value::Int(2)),
                ("foo", Value::Bytes(b"spam".to_vec()))
            ])
        );
        assert!(matches!(
            decode(b"d1:a1:b1:ac1:de"),
            Err(BencodeError::DuplicateKey(7))
        ));
        assert!(matches!(
            decode(b"di1e1:ae"),
            Err(BencodeError::InvalidKey(1))
        ));
        assert!(matches!(
            decode(b"d1:a"),
            Err(BencodeError::UnexpectedEof(4))
        ));
    }

    #[test]
    fn rejects_trailing_bytes() {
        assert!(matches!(
            decode(b"i1ei2e"),
            Err(BencodeError::TrailingBytes(3))
        ));
        assert!(matches!(
            decode(b"4:spamx"),
            Err(BencodeError::TrailingBytes(6))
        ));
    }

    #[test]
    fn rejects_deep_nesting() {
        let count = MAX_DEPTH + 2;
        let mut input = vec![b'l'; count];
        input.resize(count * 2, b'e');
        assert!(matches!(
            decode(&input),
            Err(BencodeError::MaxDepthExceeded(_))
        ));
    }

    #[test]
    fn encodes_canonical_forms() {
        assert_eq!(encode(&Value::Int(-7)), b"i-7e");
        assert_eq!(encode(&Value::Bytes(b"spam".to_vec())), b"4:spam");
        assert_eq!(encode(&Value::List(Vec::new())), b"le");
        let value = dict(&[("foo", Value::Int(1)), ("bar", Value::Int(2))]);
        assert_eq!(encode(&value), b"d3:bari2e3:fooi1ee");
    }

    #[test]
    fn round_trips() {
        let samples: [&[u8]; 5] = [
            b"i0e",
            b"i-42e",
            b"0:",
            b"l4:spami1eli2eed3:bar4:list3:fooi-9eee",
            b"d4:infod6:lengthi6e4:name5:a.txt12:piece lengthi16384e6:pieces20:\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14e4:nexti777ee",
        ];
        for sample in samples {
            assert_eq!(encode(&decode(sample).unwrap()), sample);
        }
    }

    #[test]
    fn string_access_requires_utf8() {
        let value = decode(b"2:\xff\xfe").unwrap();
        assert_eq!(value.as_str(), None);
        assert_eq!(value.as_bytes(), Some(&b"\xff\xfe"[..]));
    }

    #[test]
    fn extracts_info_dict_bytes() {
        let raw = b"d8:announce8:http://x4:infod4:name4:testee";
        let info = info_dict_bytes(raw).unwrap().unwrap();
        assert_eq!(info, b"d4:name4:teste");
        assert!(info_dict_bytes(b"d8:announce8:http://xe")
            .unwrap()
            .is_none());
        assert!(matches!(
            info_dict_bytes(b"i1e"),
            Err(BencodeError::InvalidMarker(0))
        ));
    }
}
