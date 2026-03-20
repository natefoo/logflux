#!/usr/bin/env python
from __future__ import annotations

import datetime
import os.path
import re
from time import sleep

import tzlocal
from systemd import journal

from .base import LogFluxApplication, Message, Point, Rule, fmtarg

LAST_TIMESTAMP_FILE = ".last_timestamp"


class JournaldApplication(LogFluxApplication):
    filters: list[dict[str, str]]

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

    def send_points(self, points: list[Point]) -> None:
        for point in points:
            tags = ""
            if point.get("tags"):
                tags = "," + fmtarg(point.get("tags", {}))
            if self.args.telegraf or self.args.verbose:
                print(f"{point['measurement']}{tags} {fmtarg(point['fields'])} {point['time']}")
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
            points = self.parse_message(msg)
            if points:
                stamp = msg["__REALTIME_TIMESTAMP"].timestamp()
                self.send_points(points)
        return stamp

    def run_once(self, j: journal.Reader) -> None:
        if os.path.exists(self.last_timestamp_file):
            j.seek_realtime(datetime.datetime.fromtimestamp(float(open(self.last_timestamp_file).read())))
        stamp = self.handle_all(j)
        if stamp:
            open(self.last_timestamp_file, "w").write(str(stamp + 0.000001))

    def run_continuous(self, j: journal.Reader) -> None:
        j.seek_tail()
        while True:
            self.handle_all(j)
            sleep(1)

    def run(self) -> None:
        j = journal.Reader()
        # add as list of strings to allow for duplicate key ORing as per docs:
        # https://www.freedesktop.org/software/systemd/python-systemd/journal.html#systemd.journal.Reader.add_match
        j.add_match(*[f"{f['key']}={f['value']}" for f in self.filters])
        if self.args.telegraf:
            self.run_once(j)
        else:
            self.run_continuous(j)
