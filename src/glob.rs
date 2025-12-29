pub(crate) fn literal_prefix(pattern: &str) -> Option<String> {
    let mut parts = Vec::new();
    let mut found_glob = false;

    for component in pattern.split('/') {
        if has_glob_chars(component) {
            found_glob = true;
            break;
        }

        parts.push(component);
    }

    if found_glob {
        return Some(parts.join("/"));
    }

    None
}

pub(crate) fn split_glob_path(pattern: &str) -> (String, String) {
    let mut base_parts = Vec::new();
    let mut glob_parts = Vec::new();
    let mut found_glob = false;

    for component in pattern.split('/') {
        if component.is_empty() {
            continue;
        }

        if !found_glob && !has_glob_chars(component) {
            base_parts.push(component);
        } else {
            found_glob = true;
            glob_parts.push(component);
        }
    }

    (base_parts.join("/"), glob_parts.join("/"))
}

pub(crate) fn has_glob_chars(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '*' | '?' | '[' | '{'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_prefix() {
        let test_cases = vec![
            ("path/to/some/**/*.txt", Some("path/to/some".to_string())),
            ("path/to/s?me/**/*.txt", Some("path/to".to_string())),
            ("path/to/som[ae]/**/*.txt", Some("path/to".to_string())),
            ("path/to/some/file{,.txt}", Some("path/to/some".to_string())),
            ("path/to/some/file", None),
        ];

        for (input, expected) in test_cases {
            let result = literal_prefix(input);
            assert_eq!(result, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_split_glob_path() {
        let test_cases = vec![
            (
                "path/to/some/**/*.txt",
                ("path/to/some".to_string(), "**/*.txt".to_string()),
            ),
            (
                "path/to/s?me/**/*.txt",
                ("path/to".to_string(), "s?me/**/*.txt".to_string()),
            ),
            (
                "path/to/som[ae]/**/*.txt",
                ("path/to".to_string(), "som[ae]/**/*.txt".to_string()),
            ),
            (
                "path/to/some/file{,.txt}",
                ("path/to/some".to_string(), "file{,.txt}".to_string()),
            ),
        ];

        for (input, expected) in test_cases {
            let result = split_glob_path(input);
            assert_eq!(result, expected, "Failed for input: {}", input);
        }
    }
}
