//! A small emulation of the command-line semantics upstream gets from urfave/cli v2
//! (v2.27.7) on top of Go's `flag` package.
//!
//! The parts that are observable through `NewConfigFromCLI` are reproduced:
//!
//! * Go `flag` syntax: `-name` and `--name` are equivalent, values come as
//!   `--name=value` or `--name value`, boolean flags never consume the next
//!   argument, parsing stops at the first non-flag argument or after `--`, and the
//!   error texts (`flag provided but not defined: -x`, `invalid value ...`).
//! * Environment variables are read when the flag set is built (urfave `Apply`):
//!   a value that does not parse is an error, an empty value sets string and bool
//!   flags (to `""` and `false`) but not numeric ones.
//! * Integers parse like Go `strconv.ParseInt(s, 0, 64)` (base prefixes and
//!   digit-separating underscores).
//! * The help text (urfave `AppHelpTemplate` with flag categories, rendered
//!   through a `text/tabwriter` with padding 2).

use std::sync::{Arc, Mutex};

use cabby_core::toml_config::go_quote;

use super::options::State;

/// A flag value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Val {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Str(String),
}

/// Where a parsed value goes.
#[derive(Clone, Copy)]
pub(crate) enum Setter {
    Bool(fn(&mut State, bool)),
    Int(fn(&mut State, i64)),
    Uint(fn(&mut State, u64)),
    Str(fn(&mut State, String)),
}

impl Setter {
    fn set(self, state: &mut State, v: &Val) {
        match (self, v) {
            (Setter::Bool(f), Val::Bool(b)) => f(state, *b),
            (Setter::Int(f), Val::Int(i)) => f(state, *i),
            (Setter::Uint(f), Val::Uint(u)) => f(state, *u),
            (Setter::Str(f), Val::Str(s)) => f(state, s.clone()),
            _ => unreachable!("flag value and setter types always match"),
        }
    }
}

/// How a flag's value reaches the configuration.
#[derive(Clone)]
pub(crate) enum Binding {
    /// Read from the parse context only (`ctx.Bool("ignore-config-path")`).
    Unbound,
    /// Upstream `withDefaults`: the destination gets the flag's default when the
    /// flag is defined, and the parsed value from a flag action, which urfave runs
    /// after the `Before` hook (the configuration file) and only when the flag was
    /// set on the command line or through the environment.
    Action { init: bool, set: Setter },
    /// A plain Go `flag` destination (flags `withDefaults` does not wrap): written
    /// when the flag set is built (default or environment value) and while
    /// parsing, so a configuration file loaded afterwards overrides it.
    Plain(Setter),
    /// A plain string destination owned by the embedding application.
    Custom(Arc<Mutex<String>>),
}

/// A flag definition (urfave `cli.Flag`).
#[derive(Clone)]
pub(crate) struct Flag {
    pub name: String,
    pub aliases: Vec<String>,
    pub usage: String,
    /// Help category (`""`: uncategorized).
    pub category: &'static str,
    pub hidden: bool,
    pub env_vars: Vec<String>,
    /// urfave `Value`; its variant is the flag type.
    pub default: Val,
    pub required: bool,
    /// urfave `DisableDefaultText` (help and version flags).
    pub no_default_text: bool,
    pub binding: Binding,
}

impl Flag {
    pub fn new(name: &str, default: Val, usage: &str, binding: Binding) -> Self {
        Flag {
            name: name.to_owned(),
            aliases: Vec::new(),
            usage: usage.to_owned(),
            category: "",
            hidden: false,
            env_vars: Vec::new(),
            default,
            required: false,
            no_default_text: false,
            binding,
        }
    }

    fn names(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.name.as_str()).chain(self.aliases.iter().map(String::as_str))
    }

    fn is_bool(&self) -> bool {
        matches!(self.default, Val::Bool(_))
    }

    /// urfave `stringifyFlag`: `--name value, -n value\tusage (default: x) [$ENV]`.
    fn help_line(&self) -> String {
        let placeholder = if self.is_bool() { "" } else { " value" };
        let names: Vec<String> = self
            .names()
            .filter(|n| !n.is_empty())
            .map(|n| format!("{}{n}{placeholder}", if n.chars().count() == 1 { "-" } else { "--" }))
            .collect();

        let default = match &self.default {
            Val::Bool(_) if self.no_default_text => String::new(),
            Val::Bool(b) => format!(" (default: {b})"),
            Val::Int(i) => format!(" (default: {i})"),
            Val::Uint(u) => format!(" (default: {u})"),
            Val::Str(s) if s.is_empty() => String::new(),
            Val::Str(s) => format!(" (default: {})", go_quote(s)),
        };
        let usage = format!("{}{default}", self.usage);

        let mut line = format!("{}\t{}", names.join(", "), usage.trim());
        if !self.env_vars.is_empty() {
            line.push_str(&format!(" [${}]", self.env_vars.join(", $")));
        }
        line
    }
}

/// Builds the upstream environment variable name of a flag (`nameToEnvVarName`):
/// `ANYCABLE_` plus the name upper-cased, `-` and `_` both becoming `_`.
pub(crate) fn env_var_name(name: &str) -> String {
    format!("ANYCABLE_{}", name.replace('-', "_").to_uppercase())
}

/// The application (urfave `cli.App`) settings that show in help and version output.
pub(crate) struct App {
    pub name: String,
    pub version: String,
    pub usage: String,
}

impl App {
    /// urfave `AppHelpTemplate`, rendered through `tabwriter.NewWriter(out, 1, 8, 2, ' ', 0)`.
    pub fn help(&self, flags: &[Flag]) -> String {
        let usage = if self.usage.is_empty() { "A new cli application" } else { &self.usage };

        let mut out = format!("NAME:\n   {} - {usage}\n\nUSAGE:\n   {} ", self.name, self.name);
        let visible: Vec<&Flag> = flags.iter().filter(|f| !f.hidden).collect();
        if !visible.is_empty() {
            out.push_str("[global options]");
        }
        if !self.version.is_empty() {
            out.push_str(&format!("\n\nVERSION:\n   {}", self.version));
        }

        let categorized = visible.iter().any(|f| !f.category.is_empty());
        if categorized {
            out.push_str("\n\nGLOBAL OPTIONS:");
            let mut categories: Vec<&str> = visible.iter().map(|f| f.category).collect();
            categories.sort_unstable();
            categories.dedup();
            for category in categories {
                out.push_str("\n   ");
                if !category.is_empty() {
                    out.push_str(category);
                    out.push_str("\n\n   ");
                }
                let mut lines: Vec<String> =
                    visible.iter().filter(|f| f.category == category).map(|f| f.help_line()).collect();
                lines.sort_unstable();
                lines.dedup();
                out.push_str(&lines.join("\n   "));
                out.push('\n');
            }
        } else if !visible.is_empty() {
            out.push_str("\n\nGLOBAL OPTIONS:");
            for f in &visible {
                out.push_str("\n   ");
                out.push_str(&f.help_line());
            }
        }
        out.push('\n');

        tabwrite(&out)
    }
}

/// Go `text/tabwriter` with minwidth 1, padding 2 and space padding, for text
/// whose lines hold at most one tab: each run of consecutive lines containing a
/// tab is a column block, its first cells padded to the widest one plus 2.
fn tabwrite(text: &str) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let mut out = String::with_capacity(text.len() + 256);
    let mut i = 0;
    while i < lines.len() {
        if !lines[i].contains('\t') {
            out.push_str(lines[i]);
            if i + 1 < lines.len() {
                out.push('\n');
            }
            i += 1;
            continue;
        }
        let end = (i..lines.len()).find(|&j| !lines[j].contains('\t')).unwrap_or(lines.len());
        let width = lines[i..end]
            .iter()
            .map(|l| l.split_once('\t').map_or(0, |(cell, _)| cell.chars().count()))
            .max()
            .unwrap_or(0)
            .max(1)
            + 2;
        for line in &lines[i..end] {
            let (cell, rest) = line.split_once('\t').unwrap_or((line, ""));
            out.push_str(cell);
            out.extend(std::iter::repeat_n(' ', width - cell.chars().count()));
            out.push_str(rest);
            out.push('\n');
        }
        if end == lines.len() {
            out.pop();
        }
        i = end;
    }
    out
}

// --- Go strconv ----------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumError {
    Syntax,
    Range,
}

impl NumError {
    /// Go `flag` package wording (`numError`).
    fn flag_text(self) -> &'static str {
        match self {
            NumError::Syntax => "parse error",
            NumError::Range => "value out of range",
        }
    }

    /// Go `strconv.NumError` wording.
    fn strconv_text(self) -> &'static str {
        match self {
            NumError::Syntax => "invalid syntax",
            NumError::Range => "value out of range",
        }
    }
}

/// Go `strconv.ParseBool`.
pub(crate) fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// Go `underscoreOK`: underscores only between digits (or after a base prefix).
fn underscore_ok(s: &str) -> bool {
    let s = s.strip_prefix(['+', '-']).unwrap_or(s).as_bytes();
    let mut saw = b'^';
    let mut i = 0;
    let mut hex = false;
    if s.len() >= 2 && s[0] == b'0' && matches!(s[1].to_ascii_lowercase(), b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = s[1].eq_ignore_ascii_case(&b'x');
    }
    while i < s.len() {
        let c = s[i];
        i += 1;
        if c.is_ascii_digit() || hex && matches!(c.to_ascii_lowercase(), b'a'..=b'f') {
            saw = b'0';
            continue;
        }
        if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
            continue;
        }
        if saw == b'_' {
            return false;
        }
        saw = b'!';
    }
    saw != b'_'
}

/// Go `strconv.ParseUint(s, 0, 64)`.
pub(crate) fn parse_uint(s0: &str) -> Result<u64, NumError> {
    if s0.is_empty() {
        return Err(NumError::Syntax);
    }
    let b = s0.as_bytes();
    let (base, digits): (u64, &[u8]) = if b[0] == b'0' && b.len() >= 3 && b[1].eq_ignore_ascii_case(&b'b') {
        (2, &b[2..])
    } else if b[0] == b'0' && b.len() >= 3 && b[1].eq_ignore_ascii_case(&b'o') {
        (8, &b[2..])
    } else if b[0] == b'0' && b.len() >= 3 && b[1].eq_ignore_ascii_case(&b'x') {
        (16, &b[2..])
    } else if b[0] == b'0' {
        (8, &b[1..])
    } else {
        (10, b)
    };

    let mut underscores = false;
    let mut n: u64 = 0;
    for &c in digits {
        let d = match c {
            b'_' => {
                underscores = true;
                continue;
            }
            b'0'..=b'9' => u64::from(c - b'0'),
            _ if c.is_ascii_alphabetic() => u64::from(c.to_ascii_lowercase() - b'a') + 10,
            _ => return Err(NumError::Syntax),
        };
        if d >= base {
            return Err(NumError::Syntax);
        }
        n = n.checked_mul(base).and_then(|n| n.checked_add(d)).ok_or(NumError::Range)?;
    }
    if underscores && !underscore_ok(s0) {
        return Err(NumError::Syntax);
    }
    Ok(n)
}

/// Go `strconv.ParseInt(s, 0, 64)`.
pub(crate) fn parse_int(s: &str) -> Result<i64, NumError> {
    if s.is_empty() {
        return Err(NumError::Syntax);
    }
    let (neg, rest) = match s.as_bytes()[0] {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    let un = parse_uint(rest)?;
    if !neg && un > i64::MAX as u64 || neg && un > i64::MAX as u64 + 1 {
        return Err(NumError::Range);
    }
    Ok(if neg { (un as i64).wrapping_neg() } else { un as i64 })
}

// --- Parsing --------------------------------------------------------------------

/// The parsed state of every flag (urfave's `flag.FlagSet` plus `HasBeenSet`).
pub(crate) struct Parsed {
    /// Current value: default, environment, then command line.
    pub values: Vec<Val>,
    /// Set on the command line.
    pub on_cli: Vec<bool>,
    /// Set through the environment (urfave `HasBeenSet`).
    pub from_env: Vec<bool>,
}

impl Parsed {
    fn index(flags: &[Flag], name: &str) -> Option<usize> {
        flags.iter().position(|f| f.names().any(|n| n == name))
    }

    pub fn bool(&self, flags: &[Flag], name: &str) -> bool {
        Self::index(flags, name).is_some_and(|i| self.values[i] == Val::Bool(true))
    }

    pub fn string(&self, flags: &[Flag], name: &str) -> String {
        match Self::index(flags, name).map(|i| &self.values[i]) {
            Some(Val::Str(s)) => s.clone(),
            _ => String::new(),
        }
    }

    /// urfave `Context.IsSet`.
    pub fn is_set(&self, i: usize) -> bool {
        self.on_cli[i] || self.from_env[i]
    }
}

fn write_plain(state: &mut State, flag: &Flag, v: &Val) {
    match (&flag.binding, v) {
        (Binding::Plain(setter), v) => setter.set(state, v),
        (Binding::Custom(dest), Val::Str(s)) => *dest.lock().unwrap_or_else(|e| e.into_inner()) = s.clone(),
        _ => {}
    }
}

/// urfave `Flag.Apply` for every flag in order: environment values, then the
/// initial value of plain destinations. Returns the first environment error.
pub(crate) fn apply_env(flags: &[Flag], env: &crate::config::EnvMap, state: &mut State) -> Result<Parsed, String> {
    let mut parsed =
        Parsed { values: Vec::with_capacity(flags.len()), on_cli: vec![false; flags.len()], from_env: Vec::new() };

    for flag in flags {
        let mut value = flag.default.clone();
        let mut from_env = false;

        let found = flag.env_vars.iter().find_map(|k| env.get(k.trim()).map(|v| (k, v)));
        if let Some((key, raw)) = found {
            let source = format!("environment variable {}", go_quote(key));
            match &flag.default {
                Val::Str(_) => {
                    value = Val::Str(raw.clone());
                    from_env = true;
                }
                Val::Bool(_) => {
                    value = Val::Bool(if raw.is_empty() {
                        false
                    } else {
                        parse_bool(raw).ok_or_else(|| {
                            format!(
                                "could not parse {} as bool value from {source} for flag {}: strconv.ParseBool: parsing {}: invalid syntax",
                                go_quote(raw),
                                flag.name,
                                go_quote(raw)
                            )
                        })?
                    });
                    from_env = true;
                }
                Val::Int(_) if !raw.is_empty() => {
                    let v = parse_int(raw).map_err(|e| {
                        format!(
                            "could not parse {} as int value from {source} for flag {}: strconv.ParseInt: parsing {}: {}",
                            go_quote(raw),
                            flag.name,
                            go_quote(raw),
                            e.strconv_text()
                        )
                    })?;
                    value = Val::Int(v);
                    from_env = true;
                }
                Val::Uint(_) if !raw.is_empty() => {
                    let v = parse_uint(raw).map_err(|e| {
                        format!(
                            "could not parse {} as uint64 value from {source} for flag {}: strconv.ParseUint: parsing {}: {}",
                            go_quote(raw),
                            flag.name,
                            go_quote(raw),
                            e.strconv_text()
                        )
                    })?;
                    value = Val::Uint(v);
                    from_env = true;
                }
                Val::Int(_) | Val::Uint(_) => {}
            }
        }

        write_plain(state, flag, &value);
        parsed.values.push(value);
        parsed.from_env.push(from_env);
    }

    Ok(parsed)
}

/// Go `FlagSet.Parse` over `args` (without the program name), then urfave
/// `normalizeFlags`. Parsing stops at the first non-flag argument; the remaining
/// arguments are ignored (the application takes none).
pub(crate) fn parse_args(flags: &[Flag], args: &[&str], parsed: &mut Parsed, state: &mut State) -> Result<(), String> {
    let mut visited: Vec<&str> = Vec::new();
    let mut rest = args;

    while let Some((&s, tail)) = rest.split_first() {
        let b = s.as_bytes();
        if b.len() < 2 || b[0] != b'-' {
            break;
        }
        let mut minuses = 1;
        if b[1] == b'-' {
            minuses = 2;
            if b.len() == 2 {
                break; // "--" terminates the flags
            }
        }
        let name = &s[minuses..];
        if name.is_empty() || name.starts_with('-') || name.starts_with('=') {
            return Err(format!("bad flag syntax: {s}"));
        }
        rest = tail;

        let (name, value) = match name.split_once('=') {
            Some((n, v)) => (n, Some(v)),
            None => (name, None),
        };

        let Some(i) = Parsed::index(flags, name) else {
            return Err(format!("flag provided but not defined: -{name}"));
        };
        let flag = &flags[i];

        let v = match &flag.default {
            Val::Bool(_) => match value {
                None => Val::Bool(true),
                Some(v) => Val::Bool(
                    parse_bool(v)
                        .ok_or_else(|| format!("invalid boolean value {} for -{name}: parse error", go_quote(v)))?,
                ),
            },
            kind => {
                let v = match value {
                    Some(v) => v,
                    None => match rest.split_first() {
                        Some((&next, tail)) => {
                            rest = tail;
                            next
                        }
                        None => return Err(format!("flag needs an argument: -{name}")),
                    },
                };
                let invalid =
                    |e: NumError| format!("invalid value {} for flag -{name}: {}", go_quote(v), e.flag_text());
                match kind {
                    Val::Int(_) => Val::Int(parse_int(v).map_err(invalid)?),
                    Val::Uint(_) => Val::Uint(parse_uint(v).map_err(invalid)?),
                    _ => Val::Str(v.to_owned()),
                }
            }
        };

        write_plain(state, flag, &v);
        parsed.values[i] = v;
        parsed.on_cli[i] = true;
        if !visited.contains(&name) {
            visited.push(name);
        }
    }

    // urfave normalizeFlags: two names of the same flag on the command line.
    for flag in flags.iter().filter(|f| !f.aliases.is_empty()) {
        let mut first: Option<&str> = None;
        for name in flag.names() {
            if visited.contains(&name) {
                if let Some(other) = first {
                    return Err(format!("Cannot use two forms of the same flag: {name} {other}"));
                }
                first = Some(name);
            }
        }
    }

    Ok(())
}

/// urfave `checkRequiredFlags`.
pub(crate) fn check_required(flags: &[Flag], parsed: &Parsed) -> Result<(), String> {
    let missing: Vec<&str> = flags
        .iter()
        .enumerate()
        .filter(|(i, f)| f.required && !parsed.is_set(*i))
        .map(|(_, f)| f.name.as_str())
        .collect();
    match missing.as_slice() {
        [] => Ok(()),
        [one] => Err(format!("Required flag {} not set", go_quote(one))),
        many => Err(format!("Required flags {} not set", go_quote(&many.join(", ")))),
    }
}

/// urfave `runFlagActions`: the destinations of `withDefaults` flags set on the
/// command line or through the environment, in definition order.
pub(crate) fn run_actions(flags: &[Flag], parsed: &Parsed, state: &mut State) {
    for (i, flag) in flags.iter().enumerate() {
        if let Binding::Action { set, .. } = &flag.binding
            && parsed.is_set(i)
        {
            set.set(state, &parsed.values[i]);
        }
    }
}

/// The construction-time half of `withDefaults`: destinations get the flag defaults.
pub(crate) fn init_destinations(flags: &[Flag], state: &mut State) {
    for flag in flags {
        if let Binding::Action { init: true, set } = &flag.binding {
            set.set(state, &flag.default);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int_matches_go_base_zero() {
        assert_eq!(parse_int("42"), Ok(42));
        assert_eq!(parse_int("-42"), Ok(-42));
        assert_eq!(parse_int("+7"), Ok(7));
        assert_eq!(parse_int("0x1F"), Ok(31));
        assert_eq!(parse_int("0o17"), Ok(15));
        assert_eq!(parse_int("017"), Ok(15));
        assert_eq!(parse_int("0b101"), Ok(5));
        assert_eq!(parse_int("1_000"), Ok(1000));
        assert_eq!(parse_int("0"), Ok(0));
        assert_eq!(parse_int("-9223372036854775808"), Ok(i64::MIN));
        assert_eq!(parse_int("9223372036854775808"), Err(NumError::Range));
        assert_eq!(parse_int("_1"), Err(NumError::Syntax));
        assert_eq!(parse_int("1__0"), Err(NumError::Syntax));
        assert_eq!(parse_int("1_"), Err(NumError::Syntax));
        assert_eq!(parse_int("08"), Err(NumError::Syntax));
        assert_eq!(parse_int("0x"), Err(NumError::Syntax));
        assert_eq!(parse_int(""), Err(NumError::Syntax));
        assert_eq!(parse_int("-"), Err(NumError::Syntax));
        assert_eq!(parse_int("abc"), Err(NumError::Syntax));
        assert_eq!(parse_uint("-1"), Err(NumError::Syntax));
        assert_eq!(parse_uint("18446744073709551615"), Ok(u64::MAX));
        assert_eq!(parse_uint("18446744073709551616"), Err(NumError::Range));
    }

    #[test]
    fn env_var_names_follow_upstream() {
        assert_eq!(env_var_name("max-conn"), "ANYCABLE_MAX_CONN");
        assert_eq!(env_var_name("redis_url"), "ANYCABLE_REDIS_URL");
    }

    #[test]
    fn tabwriter_aligns_blocks_separately() {
        let text = "A:\n   --a\tx\n   --long\ty\n\n   --b\tz\n";
        assert_eq!(tabwrite(text), "A:\n   --a     x\n   --long  y\n\n   --b  z\n");
    }
}
