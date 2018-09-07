# logflux

Read messages from rsyslog via a socket and send regex-parsed values to InfluxDB

## usage

### rsyslog

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

### logflux

logflux uses a YAML-syntax configuration style. The following example shows the syntax and available options:

```yaml
---

socket: /tmp/logflux.sock

influx:
    host: localhost
    port: 8086

database: logflux

rules:
  - name: nginx_rate_limit
    match:
      key: message
      regex: '^nginx:.* limiting requests, excess: (?P<excess>\d+\.\d+) by zone "(?P<zone>[^"]+)", client: (?P<client>[^,]+)'
    fields:
      value: message.excess
    tags:
      zone: message.zone
      client: message.client
      hostname: host
```

By default, logflux looks for its configuration in `logflux.yaml` in the current directory, this can be overridden with
the `-c` or `--config` command line option.
