pub fn strip_line_comments(line: &str) -> &str {
    let mut in_str = false;
    let mut escaped = false;
    let bytes = line.as_bytes();
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        let ch = bytes[i] as char;
        if in_str {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if ch == '"' {
            in_str = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'/' && bytes[i + 1] == b'/' {
            return &line[..i];
        }
        i += 1;
    }
    line
}

pub fn is_separator_line(line: &str) -> bool {
    let t = line.trim();
    !t.is_empty() && t.chars().all(|c| c == '=')
}

pub fn split_qual(name: &str, sep: &str) -> Vec<String> {
    name.split(sep)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}
