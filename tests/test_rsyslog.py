import json
import re
from argparse import Namespace
from unittest.mock import MagicMock

from logflux.rsyslog import RsyslogApplication


def make_rsyslog_app(rules=None, config_extra=None):
    """Create an RsyslogApplication with setup() bypassed."""
    args = Namespace(config="dummy.yaml", debug=False, verbose=False, telegraf=False)

    class TestRsyslogApp(RsyslogApplication):
        def setup(self):
            self.rules = rules or []
            self.config = config_extra or {}

    return TestRsyslogApp(args)


class TestLoadMessageJson:
    def test_valid_json(self):
        app = make_rsyslog_app()
        raw = json.dumps(
            {"message": "hello", "@timestamp": "2024-01-01T00:00:00Z"}
        ).encode("utf-8")
        result = app.load_message_json(raw)
        assert result["message"] == "hello"

    def test_json_with_trailing_whitespace(self):
        app = make_rsyslog_app()
        raw = json.dumps({"key": "val"}).encode("utf-8") + b"\n"
        result = app.load_message_json(raw)
        assert result["key"] == "val"


class TestLoadMessageLegacy:
    def test_basic(self):
        app = make_rsyslog_app()
        raw = b"host: web1\nprogram: sshd\n\nFailed password for root"
        result = app.load_message_legacy(raw)
        assert result["host"] == "web1"
        assert result["program"] == "sshd"
        assert result["message"] == "Failed password for root"

    def test_multiline_body(self):
        app = make_rsyslog_app()
        raw = b"host: web1\n\nline1\nline2"
        result = app.load_message_legacy(raw)
        assert result["message"] == "line1\nline2"


class TestLoadMessageAutoDetect:
    def test_autodetects_json(self):
        app = make_rsyslog_app()
        raw = json.dumps({"message": "test"}).encode("utf-8")
        result = app.load_message(raw)
        assert result["message"] == "test"
        assert app.message_loader == app.load_message_json

    def test_autodetects_legacy(self):
        app = make_rsyslog_app()
        raw = b"host: web1\n\nbody text"
        result = app.load_message(raw)
        assert result["host"] == "web1"
        assert app.message_loader == app.load_message_legacy

    def test_explicit_json_config(self):
        app = make_rsyslog_app(config_extra={"message_format": "json"})
        app.read_config = lambda: None
        app.config = {"message_format": "json"}
        # simulate what read_config does for format
        app.message_loader = app.load_message_json
        raw = json.dumps({"key": "val"}).encode("utf-8")
        result = app.load_message(raw)
        assert result["key"] == "val"


class TestMakePoint:
    def test_basic_point(self):
        rules = [
            {
                "name": "test_metric",
                "match": {"key": "message", "regex": re.compile(r".*")},
            }
        ]
        app = make_rsyslog_app(rules=rules)
        msg = {"message": "hello world", "@timestamp": "2024-01-01T00:00:00Z"}
        match = re.match(rules[0]["match"]["regex"], msg["message"])
        point = app.make_point(rules[0], msg, match)
        assert point["measurement"] == "test_metric"
        assert point["time"] == "2024-01-01T00:00:00Z"
        assert point["fields"]["value"] == "hello world"

    def test_with_tags(self):
        rules = [
            {
                "name": "test_metric",
                "match": {"key": "message", "regex": re.compile(r"(?P<word>\w+)")},
                "tags": {"host": "hostname"},
            }
        ]
        app = make_rsyslog_app(rules=rules)
        msg = {
            "message": "hello",
            "@timestamp": "2024-01-01T00:00:00Z",
            "hostname": "web1",
        }
        match = re.match(rules[0]["match"]["regex"], msg["message"])
        point = app.make_point(rules[0], msg, match)
        assert point["tags"]["host"] == "web1"


class TestHandle:
    def test_full_pipeline(self):
        rules = [
            {
                "name": "test_metric",
                "match": {"key": "message", "regex": re.compile(r"cpu=(?P<cpu>\d+)")},
                "fields": {"cpu": {"lookup": "message.cpu", "type": "int"}},
            }
        ]
        app = make_rsyslog_app(rules=rules)
        app._LogFluxApplication__client = MagicMock()

        msg = json.dumps(
            {
                "message": "cpu=95",
                "@timestamp": "2024-01-01T00:00:00Z",
            }
        ).encode("utf-8")

        handler = MagicMock()
        handler.request = (msg, None)
        # need a server for log_msg
        app.server = MagicMock()

        app.handle(handler)
        app._LogFluxApplication__client.write_points.assert_called_once()
        points = app._LogFluxApplication__client.write_points.call_args[0][0]
        assert points[0]["fields"]["cpu"] == 95
