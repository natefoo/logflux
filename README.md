# logflux

Parse log messages and send regex-matched values to InfluxDB. Supports rsyslog (via Unix socket) and journald (via systemd journal API) as log sources.

## Installation

```
pip install logflux
```

For journald support, you also need `python-systemd` and `tzlocal`. `python-systemd` is a C binding for
`libsystemd` and should be installed via your system package manager:

```
# Fedora/RHEL/CentOS
dnf install python3-systemd

# Debian/Ubuntu
apt install python3-systemd

# Then install tzlocal
pip install tzlocal
```

## Usage

```
logflux [-s {rsyslog,journald}] [-c CONFIG] [-v] [-d] [-t]
```

| Option | Description |
|--------|-------------|
| `-s`, `--source` | Log source: `rsyslog` (default) or `journald` |
| `-c`, `--config` | Config file path (default: `logflux.yaml`) |
| `-v`, `--verbose` | Print InfluxDB line protocol to stdout |
| `-d`, `--debug` | Enable debug logging |
| `-t`, `--telegraf` | Telegraf mode: process new entries since last run and exit (journald only) |

## rsyslog

rsyslog 8.33.1 and newer supports creating log output in JSON format, which is preferred. To use, configure rsyslog (the
template format is important) with:

```
template(name="logflux" type="list" option.jsonf="on") {
	property(outname="@timestamp" name="timereported" dateFormat="rfc3339" format="jsonf")
	property(outname="host" name="hostname" format="jsonf")
	property(outname="severity" name="syslogseverity-text" caseConversion="upper" format="jsonf")
	property(outname="facility" name="syslogfacility-text" format="jsonf")
	property(outname="syslog-tag" name="syslogtag" format="jsonf")
	property(outname="source" name="app-name" format="jsonf")
	property(outname="message" name="msg" format="jsonf")
}

module(load="omuxsock")
$OMUxSockSocket /tmp/logflux.sock
*.*	:omuxsock:;logflux
```

If you have older versions of rsyslog that do not support JSON output, you can use the "legacy" format with the
following template:

```
template(name="logflux" type="list") {
	constant(value="@timestamp: ")
	property(outname="@timestamp" name="timereported" dateFormat="rfc3339")
	constant(value="\nhost: ")
	property(outname="host" name="hostname")
	constant(value="\nseverity: ")
	property(outname="severity" name="syslogseverity-text" caseConversion="upper")
	constant(value="\nfacility: ")
	property(outname="facility" name="syslogfacility-text")
	constant(value="\nsyslog-tag: ")
	property(outname="syslog-tag" name="syslogtag")
	constant(value="\nsource: ")
	property(outname="source" name="app-name")
	constant(value="\n\n")
	property(outname="message" name="msg")
}
```

logflux will automatically detect the format of the first message received and assume this format for all subsequent
messages. If you change message formats, restart logflux.

Note: You may only want to send a subset of syslog messages to logflux, you can do so with [filter
conditions](https://www.rsyslog.com/doc/v8-stable/configuration/filters.html). Note that advanced/RainerScript
configuration syntax is not supported with `omuxsock` as of rsyslog 8.34.0.

## journald

logflux can read directly from the systemd journal. In continuous mode (default), it tails the journal and processes new
entries as they appear. In telegraf mode (`-t`), it processes all entries since the last run and exits, making it
suitable for use as a Telegraf `exec` input plugin.

### Telegraf integration

To use logflux as a Telegraf exec input, add to your `telegraf.conf`:

```toml
[[inputs.exec]]
    commands = ["logflux -s journald -t -c /etc/logflux.yaml"]
    data_format = "influx"
```

In telegraf mode, logflux tracks its position using a timestamp file (default: `.last_timestamp`, configurable via
`last_timestamp_file` in the config). It writes InfluxDB line protocol to stdout instead of connecting to InfluxDB
directly.

### journald configuration

journald mode uses `filters` to select which journal entries to process:

```yaml
---

last_timestamp_file: /var/lib/telegraf/logflux_last_timestamp

filters:
  - key: _SYSTEMD_UNIT
    value: nginx.service

rules:
  - name: nginx_rate_limit
    match:
      key: MESSAGE
      regex: '^.*\[error\].* limiting requests, excess: (?P<excess>\d+\.\d+) by zone "(?P<zone>[^"]+)", client: (?P<client>[^,]+), server: [^,]+, request: "(?P<method>\S+) (?P<path>\S+) \S+", host: "[^"]+"'
    fields:
      value:
        lookup: MESSAGE.excess
        type: float
    tags:
      zone: MESSAGE.zone
      client: MESSAGE.client
      path:
        lookup: MESSAGE.path
        transform:
          - match: '(?<=/)[0-9a-fA-F]{16,}(?=(?:/|$|[/?]))'
            sub: 'ID'
          - match: '\?.*'
            sub: ''
```

Filters correspond to systemd journal fields (e.g. `_SYSTEMD_UNIT`, `_HOSTNAME`). Multiple filters with the same key
are ORed together, as per the [systemd journal API](https://www.freedesktop.org/software/systemd/python-systemd/journal.html#systemd.journal.Reader.add_match).

Note that journald message fields use different names than rsyslog. The journal message body is in the `MESSAGE` field
(uppercase), and timestamps are handled automatically from `__REALTIME_TIMESTAMP`.

## Configuration

logflux uses a YAML configuration file (default: `logflux.yaml`, override with `-c`).

### rsyslog-specific options

```yaml
socket: /tmp/logflux.sock       # Unix socket path (default: /run/logflux.sock)
message_format: json             # "json", "legacy", or omit for auto-detection
server_type: threading           # "forking", "threading", or omit for single-threaded
```

### journald-specific options

```yaml
last_timestamp_file: .last_timestamp   # Timestamp file for telegraf mode
namespace: mynamespace                  # Journal namespace (requires python-systemd >= 235)
filters:                                # Journal match filters
  - key: _SYSTEMD_UNIT
    value: nginx.service
```

The `namespace` option reads from a specific journald namespace (see `systemd-journald.service(8)`). This requires
`python-systemd` version 235 or newer. If `namespace` is set but the installed `python-systemd` does not support it,
logflux will exit with an error at startup. Omit the option to read from the default namespace.

### Common options

```yaml
influx:                          # InfluxDB connection parameters (not used in telegraf mode)
    host: localhost
    port: 8086

database: logflux                # InfluxDB database name
```

### Rules

Rules define how log messages are matched and converted to InfluxDB points:

```yaml
rules:
  - name: measurement_name       # InfluxDB measurement name
    match:
      key: message                # Message field to match against
      regex: '(?P<group>pattern)' # Regex with named capture groups
    fields:                       # InfluxDB fields (at least one required)
      field_name: message.group   # Simple: reference a capture group
      field_name:                 # Extended: with type conversion
        lookup: message.group
        type: float               # "int" or "float"
    tags:                         # InfluxDB tags (optional)
      tag_name: message.group     # Simple: reference a capture group
      tag_name: host              # Or reference a top-level message field
      tag_name:                   # Extended: with transforms
        lookup: message.group
        transform:                # Apply regex substitutions in order
          - match: 'pattern'
            sub: 'replacement'
```

#### Field and tag lookups

Values for fields and tags are specified as lookups:

- **`message_key`** - Reference a top-level field from the message (e.g. `host`, `severity`)
- **`match_key.group_name`** - Reference a named capture group from the regex match (e.g. `message.excess`). The
  `match_key` must be the same key used in the rule's `match.key`.

#### Type conversion

Fields can specify a `type` to convert the captured string value:

```yaml
fields:
  value:
    lookup: message.excess
    type: float    # Convert to float (also supports "int")
```

If no type is specified, the value is stored as a string.

#### Transforms

Tags (and fields) support a `transform` list that applies regex substitutions to the value before storing it. Transforms
are applied in order, each operating on the result of the previous one.

This is useful for normalizing high-cardinality values. For example, to aggregate nginx rate limit metrics by URL path
while replacing unique IDs:

```yaml
tags:
  path:
    lookup: MESSAGE.path
    transform:
      - match: '(?<=/)[0-9a-fA-F]{16,}(?=(?:/|$|[/?]))'
        sub: 'ID'
      - match: '\?.*'
        sub: ''
```

This replaces long hex IDs in URL paths with `ID` (e.g. `/api/datasets/abcdef1234567890` becomes `/api/datasets/ID`)
and strips query strings, reducing cardinality for InfluxDB tags while preserving meaningful path structure.

If no fields are specified, the default field is `{value: message}` for rsyslog and `{value: MESSAGE}` for journald,
which stores the full message body.
