//! Pure, platform-parameterized path validation and Windows path helpers.
//!
//! Everything here is a pure function of its inputs, taking an explicit
//! [`PathPlatform`], so the Windows rule set can be tested on every OS.

use std::path::{Path, PathBuf};

/// Which platform's filesystem rules apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathPlatform {
    Windows,
    Unix,
}

impl PathPlatform {
    pub fn native() -> PathPlatform {
        if cfg!(windows) {
            PathPlatform::Windows
        } else {
            PathPlatform::Unix
        }
    }
}

/// Maximum length of a single path component in UTF-16 units on Windows.
pub const MAX_COMPONENT_UTF16_UNITS: usize = 255;
/// Paths of at least this many UTF-16 units need the `\\?\` long path prefix
/// on Windows; below it the plain Win32 path works everywhere.
pub const MAX_PATH_UTF16_UNITS_WITHOUT_PREFIX: usize = 260;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    Empty,
    CurrentDirectory,
    ParentDirectory,
    PathSeparator,
    NulByte,
    ControlCharacter(char),
    ForbiddenCharacter(char),
    /// Only rejected where the platform treats the colon specially (Windows).
    Colon,
    ReservedDeviceName(String),
    TrailingDotOrSpace,
    TooLongUtf16(usize),
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathError::Empty => write!(f, "component is empty"),
            PathError::CurrentDirectory => write!(f, "component is '.'"),
            PathError::ParentDirectory => write!(f, "component is '..'"),
            PathError::PathSeparator => write!(f, "component contains a path separator"),
            PathError::NulByte => write!(f, "component contains a NUL byte"),
            PathError::ControlCharacter(c) => {
                write!(f, "component contains control character U+{:04X}", u32::from(*c))
            }
            PathError::ForbiddenCharacter(c) => {
                write!(f, "component contains the forbidden character {c:?}")
            }
            PathError::Colon => write!(f, "component contains a colon"),
            PathError::ReservedDeviceName(name) => {
                write!(f, "component is a reserved Windows device name: {name}")
            }
            PathError::TrailingDotOrSpace => {
                write!(f, "component ends with a dot or a space")
            }
            PathError::TooLongUtf16(len) => write!(
                f,
                "component is {len} UTF-16 units long, above the limit of {MAX_COMPONENT_UTF16_UNITS}"
            ),
        }
    }
}

/// Reserved Windows device names, matched case-insensitively against the part
/// of a component before its first extension dot ("CON" and "CON.txt" are
/// both reserved).
const RESERVED_DEVICE_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

pub fn is_reserved_device_name(component: &str) -> bool {
    let base = component.split('.').next().unwrap_or(component);
    RESERVED_DEVICE_NAMES
        .iter()
        .any(|reserved| base.eq_ignore_ascii_case(reserved))
}

fn utf16_len(value: &str) -> usize {
    value.chars().map(char::len_utf16).sum()
}

/// Validates one path component under the platform's rules. The traversal
/// rules (`.`, `..`, separators, NUL) apply on every platform; the rest only
/// on Windows, where the colon, reserved names, trailing dots and spaces,
/// forbidden characters and component length are filesystem-level hazards.
pub fn validate_component(platform: PathPlatform, value: &str) -> Result<(), PathError> {
    if value.is_empty() {
        return Err(PathError::Empty);
    }
    if value == "." {
        return Err(PathError::CurrentDirectory);
    }
    if value == ".." {
        return Err(PathError::ParentDirectory);
    }
    if value.contains('/') {
        return Err(PathError::PathSeparator);
    }
    if value.contains('\\') {
        return Err(PathError::PathSeparator);
    }
    if value.contains('\0') {
        return Err(PathError::NulByte);
    }
    if platform == PathPlatform::Windows {
        if let Some(c) = value.chars().find(|c| c.is_control()) {
            return Err(PathError::ControlCharacter(c));
        }
        if let Some(c) = value
            .chars()
            .find(|c| matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        {
            if c == ':' {
                return Err(PathError::Colon);
            }
            return Err(PathError::ForbiddenCharacter(c));
        }
        if is_reserved_device_name(value) {
            return Err(PathError::ReservedDeviceName(
                value.split('.').next().unwrap_or(value).to_string(),
            ));
        }
        if value.ends_with('.') || value.ends_with(' ') {
            return Err(PathError::TrailingDotOrSpace);
        }
        let len = utf16_len(value);
        if len > MAX_COMPONENT_UTF16_UNITS {
            return Err(PathError::TooLongUtf16(len));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathConflict {
    /// Two files listed with the exact same path (any platform).
    Exact { first: String, second: String },
    /// Two files whose paths collide when compared case-insensitively;
    /// they are the same file on Windows.
    CaseInsensitive { first: String, second: String },
}

impl std::fmt::Display for PathConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathConflict::Exact { first, second } => {
                write!(f, "'{first}' and '{second}'")
            }
            PathConflict::CaseInsensitive { first, second } => {
                write!(f, "'{first}' and '{second}' (same file on Windows)")
            }
        }
    }
}

/// Detects files inside one torrent that would end up as the same file on
/// disk. Exact duplicates and directory/file prefix conflicts are checked on
/// every platform; case-insensitive collisions are checked on Windows, where
/// `A.txt` and `a.txt` overwrite each other. `paths` are the relative file
/// paths, split into components.
pub fn detect_path_conflicts(
    platform: PathPlatform,
    paths: &[Vec<String>],
) -> Result<(), PathConflict> {
    let joined: Vec<String> = paths
        .iter()
        .map(|components| components.join("/"))
        .collect();
    let mut seen_exact: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for path in &joined {
        if let Some(first) = seen_exact.get(path.as_str()) {
            return Err(PathConflict::Exact {
                first: (*first).to_string(),
                second: path.clone(),
            });
        }
        seen_exact.insert(path.as_str());
    }
    if platform == PathPlatform::Windows {
        let mut seen_folded: std::collections::HashSet<String> = std::collections::HashSet::new();
        for path in &joined {
            let folded = path.to_lowercase();
            if let Some(first) = seen_folded.get(&folded) {
                return Err(PathConflict::CaseInsensitive {
                    first: first.clone(),
                    second: path.clone(),
                });
            }
            seen_folded.insert(folded);
        }
    }
    // A file whose path is also used as a directory by another file cannot
    // exist, on every platform. On Windows the comparison is additionally
    // case-insensitive.
    let exact: std::collections::HashSet<&str> = joined.iter().map(|path| path.as_str()).collect();
    let folded: std::collections::HashSet<String> = if platform == PathPlatform::Windows {
        joined.iter().map(|path| path.to_lowercase()).collect()
    } else {
        std::collections::HashSet::new()
    };
    for path in &joined {
        let components: Vec<&str> = path.split('/').collect();
        for split in 1..components.len() {
            let ancestor = components[..split].join("/");
            if exact.contains(ancestor.as_str()) {
                return Err(PathConflict::Exact {
                    first: ancestor,
                    second: path.clone(),
                });
            }
            if platform == PathPlatform::Windows {
                let ancestor = ancestor.to_lowercase();
                if folded.contains(&ancestor) {
                    return Err(PathConflict::CaseInsensitive {
                        first: ancestor,
                        second: path.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Paths of at least this many UTF-16 units bypass the classic `MAX_PATH`
/// limit on Windows when given to the filesystem with a `\\?\` prefix.
pub fn needs_long_path_prefix(platform: PathPlatform, utf16_units: usize) -> bool {
    platform == PathPlatform::Windows && utf16_units >= MAX_PATH_UTF16_UNITS_WITHOUT_PREFIX
}

/// Builds the verbatim `\\?\` form used by Windows to bypass the classic
/// `MAX_PATH` limit. `prefix` is the drive or UNC share prefix without
/// separators (`"C:"` or `"\\server\share"`), `components` the normalized
/// path components below it. Verbatim paths disable Win32 normalization, so
/// this must only ever receive absolute, normalized, validated paths.
pub fn verbatim_from_parts(prefix: &str, components: &[&str]) -> String {
    if let Some(unc) = prefix.strip_prefix("\\\\") {
        format!(
            "\\\\?\\UNC\\{}\\{}",
            unc.trim_start_matches('\\'),
            components.join("\\")
        )
    } else {
        format!("\\\\?\\{}\\{}", prefix, components.join("\\"))
    }
}

#[cfg(windows)]
pub fn prepare_file_path(path: &Path) -> PathBuf {
    use std::os::windows::ffi::OsStrExt;
    if !needs_long_path_prefix(
        PathPlatform::Windows,
        path.as_os_str().encode_wide().count(),
    ) {
        return path.to_path_buf();
    }
    // A verbatim path disables `.` and `..` normalization; refuse to build
    // one from a path that still contains them and let the plain call fail
    // with a typed error instead.
    let mut components = path.components();
    let prefix = match components.next() {
        Some(std::path::Component::Prefix(prefix)) => {
            prefix.as_os_str().to_string_lossy().into_owned()
        }
        _ => return path.to_path_buf(),
    };
    if !matches!(components.next(), Some(std::path::Component::RootDir)) {
        return path.to_path_buf();
    }
    let mut normals = Vec::new();
    for component in components {
        match component {
            std::path::Component::Normal(part) => normals.push(part.to_string_lossy().into_owned()),
            _ => return path.to_path_buf(),
        }
    }
    PathBuf::from(verbatim_from_parts(
        &prefix,
        &normals.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

#[cfg(not(windows))]
pub fn prepare_file_path(path: &Path) -> PathBuf {
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traversal_rules_apply_on_every_platform() {
        for platform in [PathPlatform::Windows, PathPlatform::Unix] {
            for bad in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
                assert!(
                    validate_component(platform, bad).is_err(),
                    "{bad:?} must be rejected on {platform:?}"
                );
            }
        }
    }

    #[test]
    fn windows_reserved_device_names_with_and_without_extension() {
        for name in [
            "CON",
            "con",
            "Con",
            "PRN",
            "prn.txt",
            "AUX",
            "NUL",
            "nul.tar.gz",
            "COM1",
            "com9",
            "COM5",
            "LPT1",
            "lpt9",
        ] {
            assert!(
                validate_component(PathPlatform::Windows, name).is_err(),
                "{name:?} must be a reserved name on Windows"
            );
        }
        for ok in [
            "CONX",
            "xCON",
            "COM0",
            "com0",
            "LPT0",
            "COM10",
            "console",
            "null.exe.x",
        ] {
            assert!(
                validate_component(PathPlatform::Windows, ok).is_ok(),
                "{ok:?} must be accepted on Windows"
            );
        }
    }

    #[test]
    fn windows_forbidden_characters_and_control_bytes() {
        for name in [
            "a<b", "a>b", "a:b", "a\"b", "a|b", "a?b", "a*b", "a\x01b", "a\x1fb",
        ] {
            assert!(
                validate_component(PathPlatform::Windows, name).is_err(),
                "{name:?} must be rejected on Windows"
            );
        }
    }

    #[test]
    fn windows_trailing_dots_and_spaces_are_rejected() {
        for name in ["a.", "a ", "a. ", "  "] {
            assert!(
                validate_component(PathPlatform::Windows, name).is_err(),
                "{name:?} must be rejected on Windows"
            );
        }
        assert!(validate_component(PathPlatform::Windows, "a .b").is_ok());
    }

    #[test]
    fn windows_components_are_limited_to_255_utf16_units() {
        let ok = "é".repeat(MAX_COMPONENT_UTF16_UNITS);
        assert_eq!(utf16_len(&ok), MAX_COMPONENT_UTF16_UNITS);
        assert!(validate_component(PathPlatform::Windows, &ok).is_ok());
        let too_long = "é".repeat(MAX_COMPONENT_UTF16_UNITS + 1);
        assert_eq!(
            validate_component(PathPlatform::Windows, &too_long),
            Err(PathError::TooLongUtf16(MAX_COMPONENT_UTF16_UNITS + 1))
        );
    }

    #[test]
    fn unix_accepts_windows_only_hazards() {
        for name in [
            "a:b", "CON", "con.txt", "com1", "a.", "a ", "a<b", "a>b", "a\"b", "a|b", "a?b", "a*b",
        ] {
            assert!(
                validate_component(PathPlatform::Unix, name).is_ok(),
                "{name:?} must be accepted on Unix"
            );
        }
    }

    #[test]
    fn collision_detection_finds_case_insensitive_clashes_only_for_windows() {
        let paths = vec![vec!["A.txt".to_string()], vec!["a.txt".to_string()]];
        assert!(matches!(
            detect_path_conflicts(PathPlatform::Windows, &paths),
            Err(PathConflict::CaseInsensitive { .. })
        ));
        assert!(detect_path_conflicts(PathPlatform::Unix, &paths).is_ok());
    }

    #[test]
    fn collision_detection_finds_exact_duplicates_everywhere() {
        let paths = vec![vec!["a".to_string()], vec!["a".to_string()]];
        for platform in [PathPlatform::Windows, PathPlatform::Unix] {
            assert!(matches!(
                detect_path_conflicts(platform, &paths),
                Err(PathConflict::Exact { .. })
            ));
        }
    }

    #[test]
    fn collision_detection_finds_file_and_directory_clashes() {
        let paths = vec![
            vec!["a".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];
        for platform in [PathPlatform::Windows, PathPlatform::Unix] {
            assert!(detect_path_conflicts(platform, &paths).is_err());
        }
    }

    #[test]
    fn collision_detection_finds_case_clashing_directories_on_windows() {
        let paths = vec![
            vec!["dir".to_string(), "x".to_string()],
            vec!["DIR".to_string(), "x".to_string()],
        ];
        assert!(matches!(
            detect_path_conflicts(PathPlatform::Windows, &paths),
            Err(PathConflict::CaseInsensitive { .. })
        ));
        assert!(detect_path_conflicts(PathPlatform::Unix, &paths).is_ok());
    }

    #[test]
    fn distinct_paths_pass_collision_detection() {
        let paths = vec![
            vec!["dir".to_string(), "a.txt".to_string()],
            vec!["dir".to_string(), "b.txt".to_string()],
            vec!["other".to_string(), "c".to_string()],
        ];
        for platform in [PathPlatform::Windows, PathPlatform::Unix] {
            assert!(detect_path_conflicts(platform, &paths).is_ok());
        }
    }

    #[test]
    fn long_path_prefix_decision_is_pure() {
        assert!(!needs_long_path_prefix(PathPlatform::Unix, 10_000));
        assert!(!needs_long_path_prefix(
            PathPlatform::Windows,
            MAX_PATH_UTF16_UNITS_WITHOUT_PREFIX - 1
        ));
        assert!(needs_long_path_prefix(
            PathPlatform::Windows,
            MAX_PATH_UTF16_UNITS_WITHOUT_PREFIX
        ));
    }

    #[test]
    fn verbatim_prefix_builds_drive_and_unc_forms() {
        assert_eq!(
            verbatim_from_parts("C:", &["dir", "file.txt"]),
            "\\\\?\\C:\\dir\\file.txt"
        );
        assert_eq!(
            verbatim_from_parts("\\\\server\\share", &["dir"]),
            "\\\\?\\UNC\\server\\share\\dir"
        );
    }

    #[cfg(windows)]
    #[test]
    fn prepare_file_path_only_touches_long_paths() {
        use std::os::windows::ffi::OsStrExt;
        let short = Path::new("C:\\dir\\file.txt");
        assert_eq!(prepare_file_path(short), short);
        let deep: PathBuf = std::iter::once(PathBuf::from("C:\\"))
            .chain((0..40).map(|i| PathBuf::from(format!("level-{i:02}-directory-name"))))
            .collect();
        assert!(
            deep.as_os_str().encode_wide().count() >= MAX_PATH_UTF16_UNITS_WITHOUT_PREFIX,
            "the test path must exceed MAX_PATH"
        );
        let prepared = prepare_file_path(&deep);
        let text = prepared.to_string_lossy().into_owned();
        assert!(text.starts_with("\\\\?\\C:\\"), "{text}");
        assert!(
            !text.contains('/'),
            "verbatim paths must use backslashes: {text}"
        );
    }
}
