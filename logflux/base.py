#!/usr/bin/env python
from __future__ import annotations

import argparse
import ast
import math
import operator
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

# Safe operators for math expression evaluation
_SAFE_OPS: dict[type, Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
}

# Allowed math module functions
_SAFE_MATH: dict[str, Any] = {
    name: getattr(math, name)
    for name in ("ceil", "floor", "log", "log2", "log10", "sqrt", "abs", "pow")
    if hasattr(math, name)
}
_SAFE_MATH["abs"] = abs


def safe_eval_math(expr: str, variables: dict[str, float | int]) -> float | int:
    """Evaluate a math expression with variables, without using eval().

    Supports: +, -, *, /, //, %, ** operators and math functions
    (ceil, floor, log, log2, log10, sqrt, abs, pow).
    """
    tree = ast.parse(expr, mode="eval")

    def _eval(node: ast.expr) -> float | int:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        elif isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        elif isinstance(node, ast.Name) and node.id in variables:
            return variables[node.id]
        elif isinstance(node, ast.BinOp) and type(node.op) in _SAFE_OPS:
            return _SAFE_OPS[type(node.op)](_eval(node.left), _eval(node.right))
        elif isinstance(node, ast.UnaryOp) and type(node.op) in _SAFE_OPS:
            return _SAFE_OPS[type(node.op)](_eval(node.operand))
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in _SAFE_MATH:
                args = [_eval(arg) for arg in node.args]
                return _SAFE_MATH[node.func.id](*args)
            raise ValueError(f"unsupported function: {ast.dump(node.func)}")
        raise ValueError(f"unsupported expression: {ast.dump(node)}")

    return _eval(tree)


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
            for lookup_type in ("fields", "tags"):
                for key, val in rule.get(lookup_type, {}).items():
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
        if value is not None and transforms:
            for transform in transforms:
                value = re.sub(transform["match"], transform["sub"], value)
        if value is not None and valtypef:
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
            if isinstance(lookup, dict) and "math" in lookup:
                v = self.eval_math_field(rule, msg, match, lookup)
            else:
                v = self.rule_value_lookup(rule, msg, match, lookup)
            if v is not None:
                r[k] = v
        return r

    def eval_math_field(
        self, rule: Rule, msg: Message, match: re.Match[str], field_def: dict[str, Any]
    ) -> float | int | None:
        """Evaluate a math expression field with multiple variable lookups."""
        variables: dict[str, float | int] = {}
        for var_name, var_lookup in field_def.get("vars", {}).items():
            val = self.rule_value_lookup(rule, msg, match, var_lookup)
            if val is None:
                log("math field: variable '{}' resolved to None, skipping", var_name)
                return None
            try:
                variables[var_name] = float(val)
            except (ValueError, TypeError):
                log("math field: variable '{}' value '{}' is not numeric", var_name, val)
                return None
        try:
            result = safe_eval_math(field_def["math"], variables)
        except Exception as exc:
            log("math field: expression '{}' failed: {}", field_def["math"], exc)
            return None
        valtypef = TYPE_MAP.get(field_def.get("type", ""), None)
        if valtypef:
            result = valtypef(result)
        return result

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
