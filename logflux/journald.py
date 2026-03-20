#!/usr/bin/env python
import datetime
import os.path
from time import sleep

import tzlocal
from systemd import journal

from .base import LogFluxApplication, fmtarg

LAST_TIMESTAMP_FILE = ".last_timestamp"


class JournaldApplication(LogFluxApplication):
    @property
    def last_timestamp_file(self):
        return self.config.get("last_timestamp_file", LAST_TIMESTAMP_FILE)

    def setup(self):
        self.read_config()
        self.compile_rules()
        if not self.args.telegraf:
            self.setup_influx()

    def read_config(self):
        super().read_config()
        self.filters = self.config.get("filters", [])

    def send_points(self, points):
        for point in points:
            tags = ""
            if point.get("tags"):
                tags = "," + fmtarg(point.get("tags", {}))
            if self.args.telegraf or self.args.verbose:
                print(
                    "{measurement}{tags} {fields} {timestamp}".format(
                        measurement=point["measurement"],
                        tags=tags,
                        fields=fmtarg(point["fields"]),
                        timestamp=point["time"],
                    )
                )
        if points and not self.args.telegraf:
            self.client.write_points(points)

    def make_point(self, rule, msg, match):
        measurement = rule["name"]
        stamp = msg["__REALTIME_TIMESTAMP"].replace(tzinfo=tzlocal.get_localzone())
        fields = self.get_fields_tags("fields", rule, msg, match, default={"value": "MESSAGE"})
        assert fields, "Unable to populate field values"
        tags = self.get_fields_tags("tags", rule, msg, match)
        m = {
            "measurement": measurement,
            "time": int(stamp.timestamp() * 1e9),
            "fields": fields,
        }
        if tags:
            m["tags"] = tags
        return m

    def handle_all(self, j):
        stamp = None
        while msg := j.get_next():
            points = self.parse_message(msg)
            if points:
                stamp = msg["__REALTIME_TIMESTAMP"].timestamp()
                self.send_points(points)
        return stamp

    def run_once(self, j):
        if os.path.exists(self.last_timestamp_file):
            j.seek_realtime(datetime.datetime.fromtimestamp(float(open(self.last_timestamp_file).read())))
        stamp = self.handle_all(j)
        if stamp:
            open(self.last_timestamp_file, "w").write(str(stamp + 0.000001))

    def run_continuous(self, j):
        j.seek_tail()
        while True:
            self.handle_all(j)
            sleep(1)

    def run(self):
        j = journal.Reader()
        # add as list of strings to allow for duplicate key ORing as per docs:
        # https://www.freedesktop.org/software/systemd/python-systemd/journal.html#systemd.journal.Reader.add_match
        j.add_match(*[f"{f['key']}={f['value']}" for f in self.filters])
        if self.args.telegraf:
            self.run_once(j)
        else:
            self.run_continuous(j)
