#!/usr/bin/env python
import re
import sys
import traceback

import yaml
from influxdb import InfluxDBClient

CONFIG_DEFAULT = "logflux.yaml"
DATABASE = "logflux"
TYPE_MAP = {
    "int": int,
    "float": float,
}
VERBOSE = False


class LogFluxApplication(object):
    def __init__(self, args):
        self.args = args
        self.config = None
        self.rules = []
        self.__client = None
        self.setup()

    @property
    def influx_config(self):
        return self.config.get("influx", {})

    @property
    def database(self):
        return self.config.get("database", DATABASE)

    @property
    def client(self):
        if self.__client is None:
            self.__client = InfluxDBClient(**self.influx_config)
        return self.__client

    def debug(self, msg, *args, **kwargs):
        if self.args.debug:
            log(msg, *args, **kwargs)

    def setup(self):
        self.read_config()
        self.compile_rules()
        self.setup_influx()

    def read_config(self):
        log("reading config from: {}", self.args.config)
        with open(self.args.config) as config_fh:
            self.config = yaml.safe_load(config_fh)
        self.rules = self.config.get("rules", [])

    def compile_rules(self):
        log("compiling rule regular expressions...")
        for rule in self.rules:
            key = rule["match"]["key"]
            pattern = rule["match"]["regex"]
            log("{}: '{}' regexp: {}", rule["name"], key, pattern)
            rule["match"]["regex"] = re.compile(pattern)
            for key, val in rule.get("tags", {}).items():
                if isinstance(val, dict) and "transform" in val:
                    for transform in val["transform"]:
                        pattern = transform["match"]
                        log("{}: '{}' regexp: {}", rule["name"], key, pattern)
                        transform["match"] = re.compile(pattern)
        log("done")

    def setup_influx(self):
        # TODO: support per-rule DB
        self.client.create_database(self.database)
        self.client.switch_database(self.database)

    def check_re(self, msg, key, pattern):
        try:
            val = msg[key]
            if isinstance(val, bytes):
                val = val.decode("utf-8", errors="replace")
            return re.match(pattern, val.strip())
        except KeyError:
            log("expected key '{}' not in message", key)

    def rule_value_match_lookup(self, rule, match, lookup):
        rule_key = rule["match"]["key"]
        key, matchkey = lookup.split(".", 1)
        if key != rule_key:
            raise Exception(
                "invalid key, fields/tags using regex lookup cannot be performed on message parts "
                "other than the '{}' key: {}".format(rule_key, key)
            )
        return match.groupdict()[matchkey]

    def rule_value_lookup(self, rule, msg, match, lookup):
        value = None
        valtypef = None
        transforms = None
        if isinstance(lookup, dict):
            if "type" in lookup:
                valtypef = TYPE_MAP[lookup["type"]]
            transforms = lookup.get("transform")
            lookup = lookup["lookup"]
        try:
            if "." in lookup:
                value = self.rule_value_match_lookup(rule, match, lookup)
            else:
                value = msg[lookup]
        except KeyError:
            log("error: invalid field/tag reference: {}; matches were:", lookup)
            for k, v in match.groupdict().items():
                log("  {}: {}", k, v)
        if value and transforms:
            for transform in transforms:
                value = re.sub(transform["match"], transform["sub"], value)
        if value and valtypef:
            value = valtypef(value)
        return value

    def get_fields_tags(self, lookup_type, rule, msg, match, default=None):
        r = {}
        for k, lookup in rule.get(lookup_type, default or {}).items():
            v = self.rule_value_lookup(rule, msg, match, lookup)
            if v:
                r[k] = v
        return r

    def make_point(self, rule, msg, match):
        raise NotImplementedError

    def parse_message(self, msg):
        points = []
        for rule in self.rules:
            key = rule["match"]["key"]
            pattern = rule["match"]["regex"]
            m = self.check_re(msg, key, pattern)
            if m:
                try:
                    points.append(self.make_point(rule, msg, m))
                except Exception as exc:
                    log("Failed to generate point: {}", str(exc))
        return points

    def send_points(self, points):
        for point in points:
            tags = ""
            if point.get("tags"):
                tags = "," + fmtarg(point.get("tags", {}))
            if self.args.verbose:
                print(
                    "{measurement}{tags} {fields} {timestamp}".format(
                        measurement=point["measurement"],
                        tags=tags,
                        fields=fmtarg(point["fields"]),
                        timestamp=point["time"],
                    )
                )
        if points:
            self.client.write_points(points)

    def run(self):
        raise NotImplementedError


def log(msg, *args, **kwargs):
    exception = kwargs.pop("exception", False)
    if exception and sys.exc_info()[0] is not None:
        print(traceback.format_exc(), end="", file=sys.stderr)
    if VERBOSE:
        print(msg.format(*args, **kwargs), file=sys.stderr)


def influxarg(v):
    if isinstance(v, float):
        return str(v)
    elif isinstance(v, int):
        return str(v) + "i"
    else:
        return '"' + str(v).replace('"', '\\"') + '"'


def fmtarg(d):
    return ",".join(["{}={}".format(k, influxarg(v)) for k, v in d.items()])
