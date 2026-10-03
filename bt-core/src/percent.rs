const UPPER_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

pub fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &byte in bytes {
        out.push('%');
        out.push(UPPER_DIGITS[(byte >> 4) as usize] as char);
        out.push(UPPER_DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_every_byte() {
        assert_eq!(percent_encode(b""), "");
        assert_eq!(percent_encode(b"A"), "%41");
        assert_eq!(
            percent_encode(&[0x00, 0x0f, 0x7f, 0x80, 0xff]),
            "%00%0F%7F%80%FF"
        );
        assert_eq!(percent_encode(&[0xde, 0xad, 0xbe, 0xef]), "%DE%AD%BE%EF");
    }

    #[test]
    fn encodes_azureus_prefix() {
        assert_eq!(percent_encode(b"-BT0001-"), "%2D%42%54%30%30%30%31%2D");
    }
}
