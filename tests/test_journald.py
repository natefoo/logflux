import datetime
import os
import re
import sys
from argparse import Namespace
from unittest.mock import MagicMock

# Mock the systemd module before importing journald
sys.modules["systemd"] = MagicMock()
sys.modules["systemd.journal"] = MagicMock()

from logflux.journald import LAST_TIMESTAMP_FILE, JournaldApplication  # noqa: E402


def make_journald_app(rules=None, config_extra=None, telegraf=False):
    """Create a JournaldApplication with setup() bypassed."""
    args = Namespace(config="dummy.yaml", debug=False, verbose=False, telegraf=telegraf)

    class TestJournaldApp(JournaldApplication):
        def setup(self):
            self.rules = rules or []
            self.config = config_extra or {}
            self.filters = self.config.get("filters", [])

    return TestJournaldApp(args)


class TestMakePoint:
    def test_basic_point(self):
        rules = [
            {
                "name": "test_metric",
                "match": {"key": "MESSAGE", "regex": re.compile(r".*")},
            }
        ]
        app = make_journald_app(rules=rules)
        stamp = datetime.datetime(2024, 1, 1, 12, 0, 0)
        msg = {"MESSAGE": "hello world", "__REALTIME_TIMESTAMP": stamp}
        match = re.match(rules[0]["match"]["regex"], msg["MESSAGE"])
        point = app.make_point(rules[0], msg, match)
        assert point["measurement"] == "test_metric"
        assert point["fields"]["value"] == "hello world"
        assert isinstance(point["time"], int)

    def test_with_custom_fields(self):
        rules = [
            {
                "name": "test_metric",
                "match": {"key": "MESSAGE", "regex": re.compile(r"temp=(?P<temp>\d+)")},
                "fields": {"temp": {"lookup": "MESSAGE.temp", "type": "float"}},
            }
        ]
        app = make_journald_app(rules=rules)
        stamp = datetime.datetime(2024, 1, 1, 12, 0, 0)
        msg = {"MESSAGE": "temp=72", "__REALTIME_TIMESTAMP": stamp}
        match = re.match(rules[0]["match"]["regex"], msg["MESSAGE"])
        point = app.make_point(rules[0], msg, match)
        assert point["fields"]["temp"] == 72.0


class TestLastTimestampFile:
    def test_default(self):
        app = make_journald_app()
        assert app.last_timestamp_file == LAST_TIMESTAMP_FILE

    def test_from_config(self):
        app = make_journald_app(config_extra={"last_timestamp_file": "/tmp/custom_ts"})
        assert app.last_timestamp_file == "/tmp/custom_ts"


class TestHandleAll:
    def test_processes_messages(self):
        rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": re.compile(r"ok")},
            }
        ]
        app = make_journald_app(rules=rules)
        app._LogFluxApplication__client = MagicMock()

        stamp = datetime.datetime(2024, 6, 15, 10, 30, 0)
        messages = [
            {"MESSAGE": "ok", "__REALTIME_TIMESTAMP": stamp},
            {},  # empty dict = end of journal
        ]
        journal = MagicMock()
        journal.get_next = MagicMock(side_effect=messages)

        result = app.handle_all(journal)
        assert result is not None
        app._LogFluxApplication__client.write_points.assert_called_once()

    def test_no_matching_messages(self):
        rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": re.compile(r"NEVER")},
            }
        ]
        app = make_journald_app(rules=rules)
        app._LogFluxApplication__client = MagicMock()

        messages = [
            {
                "MESSAGE": "something else",
                "__REALTIME_TIMESTAMP": datetime.datetime.now(),
            },
            {},
        ]
        journal = MagicMock()
        journal.get_next = MagicMock(side_effect=messages)

        result = app.handle_all(journal)
        assert result is None
        app._LogFluxApplication__client.write_points.assert_not_called()


class TestSendPointsTelegraf:
    def test_telegraf_prints_but_does_not_write(self, capsys):
        app = make_journald_app(telegraf=True)
        app._LogFluxApplication__client = MagicMock()
        points = [{"measurement": "test", "time": 123, "fields": {"value": 1}}]
        app.send_points(points)
        app._LogFluxApplication__client.write_points.assert_not_called()
        captured = capsys.readouterr()
        assert "test" in captured.out

    def test_non_telegraf_writes(self):
        app = make_journald_app(telegraf=False)
        app._LogFluxApplication__client = MagicMock()
        points = [{"measurement": "test", "time": 123, "fields": {"value": 1}}]
        app.send_points(points)
        app._LogFluxApplication__client.write_points.assert_called_once()


class TestRunOnce:
    def test_writes_timestamp_file(self, tmp_path):
        rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": re.compile(r"ok")},
            }
        ]
        ts_file = str(tmp_path / "last_ts")
        app = make_journald_app(rules=rules, config_extra={"last_timestamp_file": ts_file}, telegraf=True)

        stamp = datetime.datetime(2024, 6, 15, 10, 30, 0)
        messages = [
            {"MESSAGE": "ok", "__REALTIME_TIMESTAMP": stamp},
            {},
        ]
        journal = MagicMock()
        journal.get_next = MagicMock(side_effect=messages)

        app.run_once(journal)
        assert os.path.exists(ts_file)
        saved = float(open(ts_file).read())
        assert saved > stamp.timestamp()

    def test_reads_existing_timestamp(self, tmp_path):
        ts_file = str(tmp_path / "last_ts")
        with open(ts_file, "w") as f:
            f.write("1718444400.0")

        app = make_journald_app(config_extra={"last_timestamp_file": ts_file}, telegraf=True)

        journal = MagicMock()
        journal.get_next = MagicMock(return_value={})

        app.run_once(journal)
        journal.seek_realtime.assert_called_once()


class TestFilters:
    def test_filters_from_config(self):
        config = {
            "filters": [
                {"key": "_SYSTEMD_UNIT", "value": "nginx.service"},
                {"key": "_SYSTEMD_UNIT", "value": "httpd.service"},
            ],
        }
        app = make_journald_app(config_extra=config)
        assert len(app.filters) == 2
        assert app.filters[0]["key"] == "_SYSTEMD_UNIT"
