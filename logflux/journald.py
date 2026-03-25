#!/usr/bin/env python
from __future__ import annotations

import datetime
import glob
import os.path
import re
from time import sleep

import tzlocal
from systemd import journal

from .base import LogFluxApplication, Message, Point, Rule, fmtfields, fmttags, log

LAST_TIMESTAMP_FILE = ".last_timestamp"


def _reader_supports_namespace() -> bool:
    """Check if systemd.journal.Reader supports the namespace parameter (python-systemd >= 235)."""
    import inspect

    sig = inspect.signature(journal.Reader.__init__)
    return "namespace" in sig.parameters


def _namespace_journal_path(namespace: str) -> str:
    """Find the journal directory for a namespace, for use with Reader(path=...).

    Namespace journals are stored at /var/log/journal/<machine-id>.<namespace>/.
    """
    pattern = f"/var/log/journal/*.{namespace}"
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError(f"no journal directory found for namespace '{namespace}' (expected {pattern})")
    if len(matches) > 1:
        raise RuntimeError(f"multiple journal directories found for namespace '{namespace}': {matches}")
    return matches[0]


class JournaldApplication(LogFluxApplication):
    filters: list[dict[str, str]]
    namespace: str | None

    @property
    def last_timestamp_file(self) -> str:
        rv: str = self.config.get("last_timestamp_file", LAST_TIMESTAMP_FILE)
        return rv

    def setup(self) -> None:
        self.read_config()
        self.compile_rules()
        if not self.args.telegraf:
            self.setup_influx()

    def read_config(self) -> None:
        super().read_config()
        self.filters = self.config.get("filters", [])
        self.namespace = self.config.get("namespace")

    def send_points(self, points: list[Point]) -> None:
        for point in points:
            tags = ""
            if point.get("tags"):
                tags = "," + fmttags(point.get("tags", {}))
            if self.args.telegraf or self.args.verbose:
                print(f"{point['measurement']}{tags} {fmtfields(point['fields'])} {point['time']}")
        if points and not self.args.telegraf:
            self.client.write_points(points)

    def make_point(self, rule: Rule, msg: Message, match: re.Match[str]) -> Point:
        measurement = rule["name"]
        stamp = msg["__REALTIME_TIMESTAMP"].replace(tzinfo=tzlocal.get_localzone())
        fields = self.get_fields_tags("fields", rule, msg, match, default={"value": "MESSAGE"})
        assert fields, "Unable to populate field values"
        tags = self.get_fields_tags("tags", rule, msg, match)
        m: Point = {
            "measurement": measurement,
            "time": int(stamp.timestamp() * 1e9),
            "fields": fields,
        }
        if tags:
            m["tags"] = tags
        return m

    def handle_all(self, j: journal.Reader) -> float | None:
        stamp: float | None = None
        while msg := j.get_next():
            stamp = msg["__REALTIME_TIMESTAMP"].timestamp()
            points = self.parse_message(msg)
            if points:
                self.send_points(points)
        return stamp

    def _save_timestamp(self, stamp: float) -> None:
        with open(self.last_timestamp_file, "w") as f:
            f.write(str(stamp + 0.000001))

    def run_once(self, j: journal.Reader) -> None:
        if os.path.exists(self.last_timestamp_file):
            with open(self.last_timestamp_file) as f:
                j.seek_realtime(datetime.datetime.fromtimestamp(float(f.read())))
        stamp: float | None = None
        try:
            while msg := j.get_next():
                stamp = msg["__REALTIME_TIMESTAMP"].timestamp()
                try:
                    points = self.parse_message(msg)
                    if points:
                        self.send_points(points)
                except Exception:
                    log("error processing journal entry", exception=True)
        except Exception:
            log("error reading journal", exception=True)
        finally:
            if stamp is not None:
                self._save_timestamp(stamp)

    def run_continuous(self, j: journal.Reader) -> None:
        j.seek_tail()
        while True:
            self.handle_all(j)
            sleep(1)

    def _open_reader(self) -> journal.Reader:
        if self.namespace:
            if _reader_supports_namespace():
                log("opening journal reader for namespace: {}", self.namespace)
                return journal.Reader(namespace=self.namespace)
            path = _namespace_journal_path(self.namespace)
            log(
                "python-systemd does not support namespace parameter, falling back to path: {}",
                path,
            )
            return journal.Reader(path=path)
        return journal.Reader()

    def run(self) -> None:
        j = self._open_reader()
        # add as list of strings to allow for duplicate key ORing as per docs:
        # https://www.freedesktop.org/software/systemd/python-systemd/journal.html#systemd.journal.Reader.add_match
        j.add_match(*[f"{f['key']}={f['value']}" for f in self.filters])
        if self.args.telegraf:
            self.run_once(j)
        else:
            self.run_continuous(j)
