//! StatsD interval writer (upstream `metrics/statsd_writer.go`, built on
//! `smira/go-statsd`).

use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::Mutex;

use cabby_core::common::StringMap;
use cabby_core::logger::{Level, Logger};

use super::{IntervalWriter, Metrics, MetricsError, StatsdConfig};

/// go-statsd's logger (upstream `StatsdLogger`: the runner's logger with
/// `service=statsd`; messages containing `Error` at error level).
fn statsd_log(level: Level, msg: &str) {
    Logger::default().with("service", "statsd").log(level, msg, &[]);
}

/// Where tags go and how they are separated (go-statsd `TagFormat`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TagFormat {
    /// Tags follow the metric name (InfluxDB, Graphite) rather than the type (Datadog).
    in_name: bool,
    first: &'static str,
    other: char,
    key_value: char,
}

fn resolve_tag_format(name: &str) -> Result<TagFormat, MetricsError> {
    match name {
        "datadog" => Ok(TagFormat { in_name: false, first: "|#", other: ',', key_value: ':' }),
        "influxdb" => Ok(TagFormat { in_name: true, first: ",", other: ',', key_value: '=' }),
        "graphite" => Ok(TagFormat { in_name: true, first: ";", other: ';', key_value: '=' }),
        other => Err(MetricsError(format!("unknown StatsD tags format: {other}"))),
    }
}

#[derive(Debug)]
struct Client {
    socket: Option<UdpSocket>,
    /// Pre-rendered tags (with the leading separator), placed per `in_name`.
    tags: String,
    in_name: bool,
}

/// Sends counter interval values (`|c`) and gauge values (`|g`) over UDP with the
/// configured prefix and default tags in the configured tag format.
#[derive(Debug)]
pub struct StatsdWriter {
    config: StatsdConfig,
    tags: Option<StringMap>,
    client: Mutex<Option<Client>>,
}

impl StatsdWriter {
    pub fn new(config: StatsdConfig, tags: Option<StringMap>) -> Self {
        Self { config, tags, client: Mutex::new(None) }
    }

    fn connect(&self) -> Option<UdpSocket> {
        let connect = || -> std::io::Result<UdpSocket> {
            let addr = self
                .config
                .host
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no address found"))?;
            let local = if addr.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
            let socket = UdpSocket::bind(local)?;
            socket.connect(addr)?;
            socket.set_nonblocking(true)?;
            Ok(socket)
        };
        match connect() {
            Ok(socket) => Some(socket),
            Err(e) => {
                statsd_log(Level::Error, &format!("Error connecting to server: {e}"));
                None
            }
        }
    }
}

impl IntervalWriter for StatsdWriter {
    fn run(&self, interval: u64) -> Result<(), MetricsError> {
        let mut rendered = String::new();
        let mut in_name = false;
        if let Some(tags) = &self.tags {
            let format = resolve_tag_format(&self.config.tag_format)?;
            in_name = format.in_name;
            // Upstream passes a Go map (random order); sorted here.
            let mut sorted: Vec<_> = tags.iter().collect();
            sorted.sort();
            for (i, (k, v)) in sorted.into_iter().enumerate() {
                if i == 0 {
                    rendered.push_str(format.first);
                } else {
                    rendered.push(format.other);
                }
                rendered.push_str(k);
                rendered.push(format.key_value);
                rendered.push_str(v);
            }
        }
        let socket = self.connect();
        *self.client.lock().unwrap_or_else(|e| e.into_inner()) = Some(Client { socket, tags: rendered, in_name });

        let tags_info = match &self.tags {
            // Go's `%v` of a map: sorted `map[k:v ...]`.
            Some(tags) => {
                let mut sorted: Vec<_> = tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
                sorted.sort();
                format!(", tags=map[{}], style={}", sorted.join(" "), self.config.tag_format)
            }
            None => String::new(),
        };
        // Upstream logs through the runner's logger (no context).
        Logger::default().log(
            Level::Info,
            &format!(
                "Send statsd metrics to {} with every {interval}s (prefix={}{tags_info})",
                self.config.host, self.config.prefix
            ),
            &[],
        );
        Ok(())
    }

    fn stop(&self) {
        *self.client.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    fn write(&self, metrics: &Metrics) -> Result<(), MetricsError> {
        let mut guard = self.client.lock().unwrap_or_else(|e| e.into_inner());
        let Some(client) = guard.as_mut() else {
            return Ok(());
        };
        if client.socket.is_none() {
            client.socket = self.connect();
        }

        let prefix = &self.config.prefix;
        let mut lines: Vec<String> = Vec::new();
        let mut line = |name: &str, value: i64, kind: &str| {
            let mut l = String::with_capacity(prefix.len() + name.len() + client.tags.len() + 24);
            l.push_str(prefix);
            l.push_str(name);
            if client.in_name {
                l.push_str(&client.tags);
            }
            l.push(':');
            l.push_str(&value.to_string());
            l.push('|');
            l.push_str(kind);
            if !client.in_name {
                l.push_str(&client.tags);
            }
            l.push('\n');
            lines.push(l);
        };

        metrics.each_counter(&mut |c| {
            // go-statsd skips zero increments.
            let v = c.interval_value() as i64;
            if v != 0 {
                line(c.name(), v, "c");
            }
        });
        metrics.each_gauge(&mut |g| {
            let v = g.value() as i64;
            // go-statsd resets a gauge to 0 before sending a negative value.
            if v < 0 {
                line(g.name(), 0, "g");
            }
            line(g.name(), v, "g");
        });

        let Some(socket) = client.socket.as_ref() else {
            return Ok(());
        };
        let max = usize::try_from(self.config.max_packet_size).unwrap_or(0).max(1);
        let mut packets = Vec::new();
        let mut packet = String::with_capacity(max);
        for l in lines {
            if !packet.is_empty() && packet.len() + l.len() > max {
                packets.push(std::mem::take(&mut packet));
            }
            packet.push_str(&l);
        }
        if !packet.is_empty() {
            packets.push(packet);
        }
        for packet in packets {
            if let Err(e) = socket.send(packet.as_bytes()) {
                // go-statsd logs the failure, closes the socket and reconnects later.
                statsd_log(Level::Error, &format!("Error writing to socket: {e}"));
                client.socket = None;
                break;
            }
        }
        Ok(())
    }
}
