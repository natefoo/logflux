#!/usr/bin/env python
from __future__ import annotations

import argparse
import re
import sys
import traceback
from typing import Any

import yaml
from influxdb import InfluxDBClient

CONFIG_DEFAULT = "logflux.yaml"
DATABASE = "logflux"
TYPE_MAP: dict[str, type] = {
    "int": int,
    "float": float,
}
VERBOSE = False

# Type aliases for the loosely-typed config/message dicts
Rule = dict[str, Any]
Message = dict[str, Any]
Point = dict[str, Any]


class LogFluxApplication:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.config: dict[str, Any] = {}
        self.rules: list[Rule] = []
        self.__client: InfluxDBClient | None = None
        self.setup()

    @property
    def influx_config(self) -> dict[str, Any]:
        rv: dict[str, Any] = self.config.get("influx", {})
        return rv

    @property
    def database(self) -> str:
        rv: str = self.config.get("database", DATABASE)
        return rv

    @property
    def client(self) -> InfluxDBClient:
        if self.__client is None:
            self.__client = InfluxDBClient(**self.influx_config)
        return self.__client

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.args.debug:
            log(msg, *args, **kwargs)

    def setup(self) -> None:
        self.read_config()
        self.compile_rules()
        self.setup_influx()

    def read_config(self) -> None:
        log("reading config from: {}", self.args.config)
        with open(self.args.config) as config_fh:
            self.config = yaml.safe_load(config_fh)
        self.rules = self.config.get("rules", [])

    def compile_rules(self) -> None:
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

    def setup_influx(self) -> None:
        # TODO: support per-rule DB
        self.client.create_database(self.database)
        self.client.switch_database(self.database)

    def check_re(self, msg: Message, key: str, pattern: re.Pattern[str]) -> re.Match[str] | None:
        try:
            val = msg[key]
            if isinstance(val, bytes):
                val = val.decode("utf-8", errors="replace")
            return re.match(pattern, val.strip())
        except KeyError:
            log("expected key '{}' not in message", key)
            return None

    def rule_value_match_lookup(self, rule: Rule, match: re.Match[str], lookup: str) -> str:
        rule_key = rule["match"]["key"]
        key, matchkey = lookup.split(".", 1)
        if key != rule_key:
            raise Exception(
                f"invalid key, fields/tags using regex lookup cannot be performed on message parts "
                f"other than the '{rule_key}' key: {key}"
            )
        return match.groupdict()[matchkey]

    def rule_value_lookup(self, rule: Rule, msg: Message, match: re.Match[str], lookup: str | dict[str, Any]) -> Any:
        value: Any = None
        valtypef: type | None = None
        transforms: list[dict[str, Any]] | None = None
        if isinstance(lookup, dict):
            if "type" in lookup:
                valtypef = TYPE_MAP[lookup["type"]]
            transforms = lookup.get("transform")
            lookup = lookup["lookup"]
        assert isinstance(lookup, str)  # narrowed: dict branch always extracts ["lookup"] as str above
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

    def get_fields_tags(
        self,
        lookup_type: str,
        rule: Rule,
        msg: Message,
        match: re.Match[str],
        default: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        r: dict[str, Any] = {}
        for k, lookup in rule.get(lookup_type, default or {}).items():
            v = self.rule_value_lookup(rule, msg, match, lookup)
            if v:
                r[k] = v
        return r

    def make_point(self, rule: Rule, msg: Message, match: re.Match[str]) -> Point:
        raise NotImplementedError

    def parse_message(self, msg: Message) -> list[Point]:
        points: list[Point] = []
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

    def send_points(self, points: list[Point]) -> None:
        for point in points:
            tags = ""
            if point.get("tags"):
                tags = "," + fmtarg(point.get("tags", {}))
            if self.args.verbose:
                print(f"{point['measurement']}{tags} {fmtarg(point['fields'])} {point['time']}")
        if points:
            self.client.write_points(points)

    def run(self) -> None:
        raise NotImplementedError


def log(msg: str, *args: Any, **kwargs: Any) -> None:
    exception = kwargs.pop("exception", False)
    if exception and sys.exc_info()[0] is not None:
        print(traceback.format_exc(), end="", file=sys.stderr)
    if VERBOSE:
        print(msg.format(*args, **kwargs), file=sys.stderr)


def influxarg(v: int | float | str) -> str:
    if isinstance(v, float):
        return str(v)
    elif isinstance(v, int):
        return str(v) + "i"
    else:
        return '"' + str(v).replace('"', '\\"') + '"'


def fmtarg(d: dict[str, Any]) -> str:
    return ",".join([f"{k}={influxarg(v)}" for k, v in d.items()])
