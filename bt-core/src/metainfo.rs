use std::collections::BTreeMap;

use sha1::{Digest, Sha1};

use crate::bencode::{self, Value};
use crate::error::MetaInfoError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaInfo {
    pub info_hash: [u8; 20],
    pub info: Info,
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub name: String,
    pub piece_length: u32,
    pub pieces: Vec<[u8; 20]>,
    pub private: bool,
    pub content: Content,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    Single { length: u64 },
    Multi { files: Vec<FileEntry> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub length: u64,
    pub path: Vec<String>,
}

impl Info {
    pub fn total_length(&self) -> Result<u64, MetaInfoError> {
        total_length_of(&self.content)
    }
}

fn total_length_of(content: &Content) -> Result<u64, MetaInfoError> {
    match content {
        Content::Single { length } => Ok(*length),
        Content::Multi { files } => files.iter().try_fold(0u64, |total, file| {
            total
                .checked_add(file.length)
                .ok_or(MetaInfoError::LengthOverflow)
        }),
    }
}

impl MetaInfo {
    pub fn from_bytes(raw: &[u8]) -> Result<MetaInfo, MetaInfoError> {
        let root = bencode::decode(raw)?;
        let root = root.as_dict().ok_or(MetaInfoError::NotADictionary)?;
        let info_bytes = bencode::info_dict_bytes(raw)?.ok_or(MetaInfoError::MissingInfo)?;
        let info_hash: [u8; 20] = Sha1::digest(info_bytes).into();
        let info_dict = root
            .get("info".as_bytes())
            .and_then(Value::as_dict)
            .ok_or(MetaInfoError::MissingInfo)?;
        let announce = get_opt_str(root, "announce")?;
        let announce_list = match root.get("announce-list".as_bytes()) {
            Some(value) => parse_announce_list(value)?,
            None => Vec::new(),
        };
        let comment = get_opt_str(root, "comment")?;
        let created_by = get_opt_str(root, "created by")?;
        let creation_date = get_opt_int(root, "creation date")?;
        let info = parse_info(info_dict)?;
        Ok(MetaInfo {
            info_hash,
            info,
            announce,
            announce_list,
            comment,
            created_by,
            creation_date,
            raw: raw.to_vec(),
        })
    }
}

fn parse_info(dict: &BTreeMap<Vec<u8>, Value>) -> Result<Info, MetaInfoError> {
    let name = get_str(dict, "name")?;
    validate_component(&name, "name")?;
    let piece_length = get_u64(dict, "piece length")?;
    if piece_length == 0 || piece_length > u32::MAX as u64 {
        return Err(MetaInfoError::InvalidPieceLength(u32::MAX as u64));
    }
    let pieces = get_pieces(dict)?;
    let private = match dict.get("private".as_bytes()) {
        Some(Value::Int(flag)) => *flag != 0,
        _ => false,
    };
    let content = match dict.get("files".as_bytes()) {
        Some(files) => Content::Multi {
            files: parse_files(files)?,
        },
        None => Content::Single {
            length: get_u64(dict, "length")?,
        },
    };
    let total = total_length_of(&content)?;
    let expected_pieces = total.div_ceil(u64::from(piece_length as u32));
    if pieces.len() as u64 != expected_pieces {
        return Err(MetaInfoError::PieceCountMismatch {
            expected: expected_pieces,
            actual: pieces.len() as u64,
        });
    }
    validate_path_layout(&name, &content)?;
    Ok(Info {
        name,
        piece_length: piece_length as u32,
        pieces,
        private,
        content,
    })
}

/// Validates the torrent's file layout under the native platform's rules:
/// per-component rules for the name and every path segment, plus a whole
/// torrent check that no two files would end up as the same file on disk.
fn validate_path_layout(name: &str, content: &Content) -> Result<(), MetaInfoError> {
    use crate::paths::{self, PathPlatform};
    let platform = PathPlatform::native();
    paths::validate_component(platform, name)
        .map_err(|err| MetaInfoError::InvalidComponent { key: "name", err })?;
    let paths: Vec<Vec<String>> = match content {
        Content::Single { .. } => vec![vec![name.to_string()]],
        Content::Multi { files } => files
            .iter()
            .map(|file| {
                let mut components = vec![name.to_string()];
                components.extend(file.path.iter().cloned());
                components
            })
            .collect(),
    };
    if let Some(conflict) = paths::detect_path_conflicts(platform, &paths).err() {
        return Err(MetaInfoError::ConflictingPaths {
            conflict: conflict.to_string(),
        });
    }
    Ok(())
}

fn get_pieces(dict: &BTreeMap<Vec<u8>, Value>) -> Result<Vec<[u8; 20]>, MetaInfoError> {
    let raw = get_bytes(dict, "pieces")?;
    if raw.is_empty() || raw.len() % 20 != 0 {
        return Err(MetaInfoError::InvalidPieces);
    }
    Ok(raw
        .chunks_exact(20)
        .map(|chunk| {
            let mut hash = [0u8; 20];
            hash.copy_from_slice(chunk);
            hash
        })
        .collect())
}

fn parse_files(value: &Value) -> Result<Vec<FileEntry>, MetaInfoError> {
    let list = value.as_list().ok_or(MetaInfoError::WrongType("files"))?;
    list.iter().map(parse_file).collect()
}

fn parse_file(value: &Value) -> Result<FileEntry, MetaInfoError> {
    let dict = value.as_dict().ok_or(MetaInfoError::WrongType("files"))?;
    let length = get_u64(dict, "length")?;
    let path = get_path(dict)?;
    Ok(FileEntry { length, path })
}

fn get_path(dict: &BTreeMap<Vec<u8>, Value>) -> Result<Vec<String>, MetaInfoError> {
    let value = dict
        .get("path".as_bytes())
        .ok_or(MetaInfoError::MissingKey("path"))?;
    let list = value.as_list().ok_or(MetaInfoError::WrongType("path"))?;
    let path = list
        .iter()
        .map(|segment| {
            segment
                .as_bytes()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .ok_or(MetaInfoError::WrongType("path"))
        })
        .collect::<Result<Vec<_>, MetaInfoError>>()?;
    if path.is_empty() {
        return Err(MetaInfoError::InvalidComponent {
            key: "path",
            err: crate::paths::PathError::Empty,
        });
    }
    for segment in &path {
        validate_component(segment, "path")?;
    }
    Ok(path)
}

fn validate_component(value: &str, key: &'static str) -> Result<(), MetaInfoError> {
    crate::paths::validate_component(crate::paths::PathPlatform::native(), value)
        .map_err(|err| MetaInfoError::InvalidComponent { key, err })
}

/// Traversal-only safety check, used before deleting files by path. The
/// metainfo parser has already applied the full native platform rules.
pub(crate) fn validate_path_component(value: &str, key: &'static str) -> Result<(), MetaInfoError> {
    crate::paths::validate_component(crate::paths::PathPlatform::Unix, value)
        .map_err(|err| MetaInfoError::InvalidComponent { key, err })
}

fn parse_announce_list(value: &Value) -> Result<Vec<Vec<String>>, MetaInfoError> {
    let tiers = value
        .as_list()
        .ok_or(MetaInfoError::WrongType("announce-list"))?;
    tiers.iter().map(parse_tier).collect()
}

fn parse_tier(value: &Value) -> Result<Vec<String>, MetaInfoError> {
    let urls = value
        .as_list()
        .ok_or(MetaInfoError::WrongType("announce-list"))?;
    urls.iter()
        .map(|url| {
            url.as_bytes()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .ok_or(MetaInfoError::WrongType("announce-list"))
        })
        .collect()
}

fn get_int(dict: &BTreeMap<Vec<u8>, Value>, key: &'static str) -> Result<i64, MetaInfoError> {
    let value = dict
        .get(key.as_bytes())
        .ok_or(MetaInfoError::MissingKey(key))?;
    value.as_int().ok_or(MetaInfoError::WrongType(key))
}

fn get_opt_int(
    dict: &BTreeMap<Vec<u8>, Value>,
    key: &'static str,
) -> Result<Option<i64>, MetaInfoError> {
    match dict.get(key.as_bytes()) {
        Some(value) => value
            .as_int()
            .ok_or(MetaInfoError::WrongType(key))
            .map(Some),
        None => Ok(None),
    }
}

fn get_u64(dict: &BTreeMap<Vec<u8>, Value>, key: &'static str) -> Result<u64, MetaInfoError> {
    match get_int(dict, key)? {
        n if n >= 0 => Ok(n as u64),
        _ => Err(MetaInfoError::WrongType(key)),
    }
}

fn get_bytes<'a>(
    dict: &'a BTreeMap<Vec<u8>, Value>,
    key: &'static str,
) -> Result<&'a [u8], MetaInfoError> {
    let value = dict
        .get(key.as_bytes())
        .ok_or(MetaInfoError::MissingKey(key))?;
    value.as_bytes().ok_or(MetaInfoError::WrongType(key))
}

fn get_str(dict: &BTreeMap<Vec<u8>, Value>, key: &'static str) -> Result<String, MetaInfoError> {
    get_bytes(dict, key).map(|bytes| String::from_utf8_lossy(bytes).into_owned())
}

fn get_opt_str(
    dict: &BTreeMap<Vec<u8>, Value>,
    key: &'static str,
) -> Result<Option<String>, MetaInfoError> {
    match dict.get(key.as_bytes()) {
        Some(value) => value
            .as_bytes()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .ok_or(MetaInfoError::WrongType(key))
            .map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE_HASH: [u8; 20] = [0xab; 20];

    fn push_string(raw: &mut Vec<u8>, text: &str) {
        raw.extend_from_slice(text.len().to_string().as_bytes());
        raw.push(b':');
        raw.extend_from_slice(text.as_bytes());
    }

    fn push_pieces(raw: &mut Vec<u8>) {
        raw.extend_from_slice(b"6:pieces20:");
        raw.extend_from_slice(&PIECE_HASH);
    }

    fn single_file_torrent() -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d4:infod6:lengthi6e4:name5:a.txt12:piece lengthi16384e");
        push_pieces(&mut raw);
        raw.extend_from_slice(b"e8:announce");
        push_string(&mut raw, "http://tracker.example/announce");
        raw.push(b'e');
        raw
    }

    fn full_torrent() -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d");
        push_string(&mut raw, "announce");
        push_string(&mut raw, "http://tracker.example/announce");
        push_string(&mut raw, "announce-list");
        raw.extend_from_slice(b"ll");
        push_string(&mut raw, "http://a.example/announce");
        push_string(&mut raw, "http://b.example/announce");
        raw.extend_from_slice(b"el");
        push_string(&mut raw, "udp://c.example:1337/announce");
        raw.extend_from_slice(b"ee");
        push_string(&mut raw, "comment");
        push_string(&mut raw, "a comment");
        push_string(&mut raw, "created by");
        push_string(&mut raw, "bt-core-tests");
        push_string(&mut raw, "creation date");
        raw.extend_from_slice(b"i1600000000e");
        push_string(&mut raw, "info");
        raw.extend_from_slice(b"d6:lengthi6e4:name5:a.txt12:piece lengthi16384e");
        push_pieces(&mut raw);
        push_string(&mut raw, "private");
        raw.extend_from_slice(b"i1e");
        raw.extend_from_slice(b"ee");
        raw
    }

    fn torrent_with_info(info: &[u8]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d4:info");
        raw.extend_from_slice(info);
        raw.push(b'e');
        raw
    }

    #[test]
    fn parses_single_file_torrent() {
        let meta = MetaInfo::from_bytes(&single_file_torrent()).unwrap();
        assert_eq!(meta.info.name, "a.txt");
        assert_eq!(meta.info.piece_length, 16384);
        assert_eq!(meta.info.pieces, vec![PIECE_HASH]);
        assert_eq!(meta.info.total_length().unwrap(), 6);
        assert!(!meta.info.private);
        assert_eq!(
            meta.announce.as_deref(),
            Some("http://tracker.example/announce")
        );
        assert!(meta.announce_list.is_empty());
        assert_eq!(meta.comment, None);
    }

    #[test]
    fn computes_info_hash_from_raw_info_bytes() {
        let meta = MetaInfo::from_bytes(&single_file_torrent()).unwrap();
        let mut expected_input = Vec::new();
        expected_input
            .extend_from_slice(b"d6:lengthi6e4:name5:a.txt12:piece lengthi16384e6:pieces20:");
        expected_input.extend_from_slice(&PIECE_HASH);
        expected_input.push(b'e');
        let expected: [u8; 20] = Sha1::digest(&expected_input).into();
        assert_eq!(meta.info_hash, expected);
    }

    #[test]
    fn parses_multi_file_torrent() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d4:infod5:filesl");
        raw.extend_from_slice(b"d6:lengthi1e4:pathl3:fooee");
        raw.extend_from_slice(b"d6:lengthi2e4:pathl3:bar3:bazee");
        raw.extend_from_slice(b"e4:name3:dir12:piece lengthi32768e");
        push_pieces(&mut raw);
        raw.extend_from_slice(b"ee");
        let meta = MetaInfo::from_bytes(&raw).unwrap();
        assert_eq!(meta.info.name, "dir");
        let Content::Multi { files } = &meta.info.content else {
            panic!("expected multi-file content");
        };
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, vec!["foo".to_string()]);
        assert_eq!(files[0].length, 1);
        assert_eq!(files[1].path, vec!["bar".to_string(), "baz".to_string()]);
        assert_eq!(files[1].length, 2);
        assert_eq!(meta.info.total_length().unwrap(), 3);
    }

    #[test]
    fn reads_optional_fields() {
        let meta = MetaInfo::from_bytes(&full_torrent()).unwrap();
        assert_eq!(
            meta.announce_list,
            vec![
                vec![
                    "http://a.example/announce".to_string(),
                    "http://b.example/announce".to_string(),
                ],
                vec!["udp://c.example:1337/announce".to_string()],
            ]
        );
        assert_eq!(meta.comment.as_deref(), Some("a comment"));
        assert_eq!(meta.created_by.as_deref(), Some("bt-core-tests"));
        assert_eq!(meta.creation_date, Some(1_600_000_000));
        assert!(meta.info.private);
    }

    #[test]
    fn rejects_missing_info_dict() {
        assert!(matches!(
            MetaInfo::from_bytes(b"d8:announce8:http://xe"),
            Err(MetaInfoError::MissingInfo)
        ));
    }

    #[test]
    fn rejects_non_dictionary_root() {
        assert!(matches!(
            MetaInfo::from_bytes(b"i42e"),
            Err(MetaInfoError::NotADictionary)
        ));
    }

    #[test]
    fn rejects_missing_name() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi6e12:piece lengthi16384e");
        push_pieces(&mut info);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::MissingKey("name"))
        ));
    }

    #[test]
    fn rejects_bad_pieces_length() {
        let info = b"d6:lengthi6e4:name5:a.txt12:piece lengthi16384e6:pieces7:1234567e";
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(info)),
            Err(MetaInfoError::InvalidPieces)
        ));
    }

    #[test]
    fn rejects_negative_length() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi-6e4:name5:a.txt12:piece lengthi16384e");
        push_pieces(&mut info);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::WrongType("length"))
        ));
    }

    #[test]
    fn rejects_zero_piece_length() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi6e4:name5:a.txt12:piece lengthi0e");
        push_pieces(&mut info);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::InvalidPieceLength(_))
        ));
    }

    #[test]
    fn rejects_wrong_piece_count() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi6e4:name5:a.txt12:piece lengthi16384e6:pieces40:");
        info.extend_from_slice(&PIECE_HASH);
        info.extend_from_slice(&PIECE_HASH);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::PieceCountMismatch {
                expected: 1,
                actual: 2
            })
        ));
    }

    #[test]
    fn accepts_exact_piece_boundary() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi32768e4:name5:a.txt12:piece lengthi16384e6:pieces40:");
        info.extend_from_slice(&PIECE_HASH);
        info.extend_from_slice(&PIECE_HASH);
        info.push(b'e');
        let meta = MetaInfo::from_bytes(&torrent_with_info(&info)).unwrap();
        assert_eq!(meta.info.pieces.len(), 2);
    }

    #[test]
    fn accepts_partial_last_piece() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d6:lengthi32769e4:name5:a.txt12:piece lengthi16384e6:pieces60:");
        info.extend_from_slice(&PIECE_HASH);
        info.extend_from_slice(&PIECE_HASH);
        info.extend_from_slice(&PIECE_HASH);
        info.push(b'e');
        let meta = MetaInfo::from_bytes(&torrent_with_info(&info)).unwrap();
        assert_eq!(meta.info.pieces.len(), 3);
    }

    #[test]
    fn rejects_unsafe_names() {
        for name in ["", ".", "..", "a/b", "a\\b", "a\0b", "/etc/passwd"] {
            let mut info = Vec::new();
            info.extend_from_slice(b"d6:lengthi6e4:name");
            push_string(&mut info, name);
            info.extend_from_slice(b"12:piece lengthi16384e");
            push_pieces(&mut info);
            info.push(b'e');
            assert!(
                matches!(
                    MetaInfo::from_bytes(&torrent_with_info(&info)),
                    Err(MetaInfoError::InvalidComponent { key: "name", .. })
                ),
                "name {name:?} should be rejected"
            );
        }
    }

    #[test]
    fn colon_names_follow_the_native_platform_rules() {
        for name in ["C:\\temp", "C:", "a:b"] {
            let mut info = Vec::new();
            info.extend_from_slice(b"d6:lengthi6e4:name");
            push_string(&mut info, name);
            info.extend_from_slice(b"12:piece lengthi16384e");
            push_pieces(&mut info);
            info.push(b'e');
            let result = MetaInfo::from_bytes(&torrent_with_info(&info));
            if cfg!(windows) {
                assert!(
                    matches!(
                        result,
                        Err(MetaInfoError::InvalidComponent { key: "name", .. })
                    ),
                    "name {name:?} must be rejected on Windows"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "name {name:?} must be accepted where the colon is an ordinary character"
                );
            }
        }
    }

    #[test]
    fn rejects_unsafe_path_segments() {
        for segment in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
            let mut info = Vec::new();
            info.extend_from_slice(b"d5:filesld6:lengthi6e4:pathl");
            push_string(&mut info, segment);
            info.extend_from_slice(b"eee4:name3:dir12:piece lengthi16384e");
            push_pieces(&mut info);
            info.push(b'e');
            assert!(
                matches!(
                    MetaInfo::from_bytes(&torrent_with_info(&info)),
                    Err(MetaInfoError::InvalidComponent { key: "path", .. })
                ),
                "segment {segment:?} should be rejected"
            );
        }
    }

    #[test]
    fn path_segments_follow_the_native_windows_rules() {
        for segment in ["CON", "con.txt", "a.", "a ", "C:"] {
            let mut info = Vec::new();
            info.extend_from_slice(b"d5:filesld6:lengthi6e4:pathl");
            push_string(&mut info, segment);
            info.extend_from_slice(b"eee4:name3:dir12:piece lengthi16384e");
            push_pieces(&mut info);
            info.push(b'e');
            let result = MetaInfo::from_bytes(&torrent_with_info(&info));
            if cfg!(windows) {
                assert!(
                    matches!(
                        result,
                        Err(MetaInfoError::InvalidComponent { key: "path", .. })
                    ),
                    "segment {segment:?} must be rejected on Windows"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "segment {segment:?} must be accepted on Unix"
                );
            }
        }
    }

    #[test]
    fn case_colliding_files_fail_the_torrent_on_windows() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d5:filesl");
        for name in ["A.txt", "a.txt"] {
            info.extend_from_slice(b"d6:lengthi2e4:pathl");
            push_string(&mut info, name);
            info.extend_from_slice(b"ee");
        }
        info.extend_from_slice(b"e4:name3:dir12:piece lengthi16384e");
        push_pieces(&mut info);
        info.push(b'e');
        let result = MetaInfo::from_bytes(&torrent_with_info(&info));
        if cfg!(windows) {
            assert!(
                matches!(result, Err(MetaInfoError::ConflictingPaths { .. })),
                "case-colliding files must fail the torrent on Windows"
            );
        } else {
            assert!(
                result.is_ok(),
                "case-differing files are distinct files on Unix"
            );
        }
    }

    #[test]
    fn identical_file_paths_fail_the_torrent_on_every_platform() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d5:filesl");
        for _ in 0..2 {
            info.extend_from_slice(b"d6:lengthi2e4:pathl");
            push_string(&mut info, "same");
            info.extend_from_slice(b"ee");
        }
        info.extend_from_slice(b"e4:name3:dir12:piece lengthi16384e");
        push_pieces(&mut info);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::ConflictingPaths { .. })
        ));
    }

    #[test]
    fn rejects_empty_path_list() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d5:filesld6:lengthi6e4:pathleee4:name3:dir12:piece lengthi16384e");
        push_pieces(&mut info);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::InvalidComponent {
                key: "path",
                err: crate::paths::PathError::Empty
            })
        ));
    }

    #[test]
    fn rejects_total_length_overflow() {
        let mut info = Vec::new();
        info.extend_from_slice(b"d5:filesl");
        for (name, length) in [
            ("a", "9223372036854775807"),
            ("b", "9223372036854775807"),
            ("c", "2"),
        ] {
            info.extend_from_slice(b"d6:lengthi");
            info.extend_from_slice(length.as_bytes());
            info.extend_from_slice(b"e4:pathl");
            push_string(&mut info, name);
            info.extend_from_slice(b"ee");
        }
        info.extend_from_slice(b"e4:name3:dir12:piece lengthi16384e");
        push_pieces(&mut info);
        info.push(b'e');
        assert!(matches!(
            MetaInfo::from_bytes(&torrent_with_info(&info)),
            Err(MetaInfoError::LengthOverflow)
        ));
    }

    #[test]
    fn total_length_reports_overflow() {
        let info = Info {
            name: "dir".to_string(),
            piece_length: 16384,
            pieces: Vec::new(),
            private: false,
            content: Content::Multi {
                files: vec![
                    FileEntry {
                        length: u64::MAX,
                        path: vec!["a".to_string()],
                    },
                    FileEntry {
                        length: 1,
                        path: vec!["b".to_string()],
                    },
                ],
            },
        };
        assert!(matches!(
            info.total_length(),
            Err(MetaInfoError::LengthOverflow)
        ));
    }
}
