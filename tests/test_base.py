import re
from argparse import Namespace
from unittest.mock import MagicMock

import pytest

from logflux.base import (
    LogFluxApplication,
    fmtarg,
    influxarg,
    safe_eval_math,
)

# -- Utility functions --------------------------------------------------------


class TestInfluxarg:
    def test_float(self):
        assert influxarg(3.14) == "3.14"

    def test_int(self):
        assert influxarg(42) == "42i"

    def test_string(self):
        assert influxarg("hello") == '"hello"'

    def test_string_with_quotes(self):
        assert influxarg('say "hi"') == '"say \\"hi\\""'


class TestFmtarg:
    def test_single(self):
        assert fmtarg({"cpu": 0.5}) == "cpu=0.5"

    def test_multiple(self):
        result = fmtarg({"value": 42, "msg": "ok"})
        assert "value=42i" in result
        assert 'msg="ok"' in result

    def test_empty(self):
        assert fmtarg({}) == ""


# -- Helper to build a testable app without hitting InfluxDB or files ---------


def make_app(rules=None, config_extra=None, debug=False, verbose=False, telegraf=False):
    """Create a LogFluxApplication subclass instance with setup() bypassed."""
    args = Namespace(config="dummy.yaml", debug=debug, verbose=verbose, telegraf=telegraf)

    class TestApp(LogFluxApplication):
        def setup(self):
            # skip file I/O and InfluxDB
            self.rules = rules or []
            self.config = config_extra or {}

        def make_point(self, rule, msg, match):
            fields = self.get_fields_tags("fields", rule, msg, match, default={"value": "MESSAGE"})
            tags = self.get_fields_tags("tags", rule, msg, match)
            m = {"measurement": rule["name"], "time": 0, "fields": fields}
            if tags:
                m["tags"] = tags
            return m

        def run(self):
            pass

    return TestApp(args)


# -- check_re ----------------------------------------------------------------


class TestCheckRe:
    def test_match(self):
        app = make_app()
        pattern = re.compile(r"error: (?P<code>\d+)")
        m = app.check_re({"MESSAGE": "error: 404"}, "MESSAGE", pattern)
        assert m is not None
        assert m.group("code") == "404"

    def test_no_match(self):
        app = make_app()
        pattern = re.compile(r"error:")
        m = app.check_re({"MESSAGE": "all good"}, "MESSAGE", pattern)
        assert m is None

    def test_missing_key(self):
        app = make_app()
        pattern = re.compile(r".*")
        m = app.check_re({}, "MESSAGE", pattern)
        assert m is None

    def test_bytes_value(self):
        app = make_app()
        pattern = re.compile(r"hello")
        m = app.check_re({"MESSAGE": b"hello world"}, "MESSAGE", pattern)
        assert m is not None

    def test_bytes_with_invalid_utf8(self):
        app = make_app()
        pattern = re.compile(r"hello")
        m = app.check_re({"MESSAGE": b"hello \xff\xfe"}, "MESSAGE", pattern)
        assert m is not None

    def test_strips_whitespace(self):
        app = make_app()
        pattern = re.compile(r"^hello$")
        m = app.check_re({"MESSAGE": "  hello  "}, "MESSAGE", pattern)
        assert m is not None


# -- Rule value lookups -------------------------------------------------------


class TestRuleValueLookup:
    def setup_method(self):
        self.app = make_app()
        self.rule = {
            "name": "test",
            "match": {"key": "MESSAGE", "regex": re.compile(r"val=(?P<val>\d+)")},
        }

    def test_direct_key(self):
        msg = {"MESSAGE": "val=42", "host": "web1"}
        match = re.match(self.rule["match"]["regex"], msg["MESSAGE"])
        assert self.app.rule_value_lookup(self.rule, msg, match, "host") == "web1"

    def test_regex_group_lookup(self):
        msg = {"MESSAGE": "val=42"}
        match = re.match(self.rule["match"]["regex"], msg["MESSAGE"])
        assert self.app.rule_value_lookup(self.rule, msg, match, "MESSAGE.val") == "42"

    def test_type_conversion(self):
        msg = {"MESSAGE": "val=42"}
        match = re.match(self.rule["match"]["regex"], msg["MESSAGE"])
        result = self.app.rule_value_lookup(self.rule, msg, match, {"lookup": "MESSAGE.val", "type": "int"})
        assert result == 42
        assert isinstance(result, int)

    def test_transform(self):
        msg = {"MESSAGE": "val=42", "host": "web-server-01.example.com"}
        match = re.match(self.rule["match"]["regex"], msg["MESSAGE"])
        lookup = {
            "lookup": "host",
            "transform": [
                {"match": re.compile(r"\.example\.com$"), "sub": ""},
            ],
        }
        result = self.app.rule_value_lookup(self.rule, msg, match, lookup)
        assert result == "web-server-01"

    def test_missing_key(self):
        msg = {"MESSAGE": "val=42"}
        match = re.match(self.rule["match"]["regex"], msg["MESSAGE"])
        result = self.app.rule_value_lookup(self.rule, msg, match, "nonexistent")
        assert result is None

    def test_wrong_rule_key_raises(self):
        msg = {"MESSAGE": "val=42"}
        match = re.match(self.rule["match"]["regex"], msg["MESSAGE"])
        with pytest.raises(Exception, match="invalid key"):
            self.app.rule_value_match_lookup(self.rule, match, "OTHER.val")


# -- compile_rules ------------------------------------------------------------


class TestCompileRules:
    def test_compiles_regex(self):
        app = make_app()
        app.rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": r"error (?P<code>\d+)"},
            },
        ]
        app.compile_rules()
        assert isinstance(app.rules[0]["match"]["regex"], re.Pattern)

    def test_compiles_tag_transforms(self):
        app = make_app()
        app.rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": r".*"},
                "tags": {
                    "host": {
                        "lookup": "hostname",
                        "transform": [{"match": r"\.example\.com$", "sub": ""}],
                    },
                },
            }
        ]
        app.compile_rules()
        assert isinstance(app.rules[0]["tags"]["host"]["transform"][0]["match"], re.Pattern)

    def test_compiles_field_transforms(self):
        app = make_app()
        app.rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": r".*"},
                "fields": {
                    "path": {
                        "lookup": "MESSAGE",
                        "transform": [{"match": r"\?.*", "sub": ""}],
                    },
                },
            }
        ]
        app.compile_rules()
        assert isinstance(app.rules[0]["fields"]["path"]["transform"][0]["match"], re.Pattern)


# -- parse_message / end-to-end point generation ------------------------------


class TestParseMessage:
    def test_matching_rule_produces_point(self):
        rules = [
            {
                "name": "http_errors",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"status=(?P<status>\d+)"),
                },
                "fields": {"status": {"lookup": "MESSAGE.status", "type": "int"}},
                "tags": {"host": "HOSTNAME"},
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "status=500", "HOSTNAME": "web1"}
        points = app.parse_message(msg)
        assert len(points) == 1
        assert points[0]["measurement"] == "http_errors"
        assert points[0]["fields"]["status"] == 500
        assert points[0]["tags"]["host"] == "web1"

    def test_non_matching_rule(self):
        rules = [
            {
                "name": "test",
                "match": {"key": "MESSAGE", "regex": re.compile(r"NEVER_MATCH")},
            }
        ]
        app = make_app(rules=rules)
        points = app.parse_message({"MESSAGE": "something else"})
        assert points == []

    def test_multiple_rules(self):
        rules = [
            {"name": "r1", "match": {"key": "MESSAGE", "regex": re.compile(r"alpha")}},
            {"name": "r2", "match": {"key": "MESSAGE", "regex": re.compile(r"alpha")}},
        ]
        app = make_app(rules=rules)
        points = app.parse_message({"MESSAGE": "alpha"})
        assert len(points) == 2


# -- send_points --------------------------------------------------------------


class TestSendPoints:
    def test_sends_to_client(self):
        app = make_app()
        app._LogFluxApplication__client = MagicMock()
        points = [{"measurement": "test", "time": 0, "fields": {"value": 1}}]
        app.send_points(points)
        app._LogFluxApplication__client.write_points.assert_called_once_with(points)

    def test_empty_points_not_sent(self):
        app = make_app()
        app._LogFluxApplication__client = MagicMock()
        app.send_points([])
        app._LogFluxApplication__client.write_points.assert_not_called()

    def test_verbose_prints(self, capsys):
        app = make_app(verbose=True)
        app._LogFluxApplication__client = MagicMock()
        points = [{"measurement": "test", "time": 123, "fields": {"value": 1}}]
        app.send_points(points)
        captured = capsys.readouterr()
        assert "test" in captured.out
        assert "value=1i" in captured.out


# -- safe_eval_math -----------------------------------------------------------


class TestSafeEvalMath:
    def test_basic_arithmetic(self):
        assert safe_eval_math("a + b", {"a": 1, "b": 2}) == 3
        assert safe_eval_math("a - b", {"a": 10, "b": 3}) == 7
        assert safe_eval_math("a * b", {"a": 4, "b": 5}) == 20
        assert safe_eval_math("a / b", {"a": 10, "b": 4}) == 2.5

    def test_floor_division_and_modulo(self):
        assert safe_eval_math("a // b", {"a": 10, "b": 3}) == 3
        assert safe_eval_math("a % b", {"a": 10, "b": 3}) == 1

    def test_power(self):
        assert safe_eval_math("a ** b", {"a": 2, "b": 10}) == 1024

    def test_unary_negation(self):
        assert safe_eval_math("-a", {"a": 5}) == -5

    def test_constants(self):
        assert safe_eval_math("a + 1", {"a": 5}) == 6
        assert safe_eval_math("a * 2.5", {"a": 4}) == 10.0

    def test_math_functions(self):
        assert safe_eval_math("sqrt(a)", {"a": 16}) == 4.0
        assert safe_eval_math("log10(a)", {"a": 100}) == 2.0
        assert safe_eval_math("ceil(a)", {"a": 1.2}) == 2
        assert safe_eval_math("floor(a)", {"a": 1.8}) == 1
        assert safe_eval_math("abs(a)", {"a": -5}) == 5

    def test_complex_expression(self):
        result = safe_eval_math("(a + b) / c", {"a": 10, "b": 20, "c": 5})
        assert result == 6.0

    def test_rejects_builtins(self):
        with pytest.raises(ValueError, match="unsupported"):
            safe_eval_math("__import__('os')", {})

    def test_rejects_unknown_function(self):
        with pytest.raises(ValueError, match="unsupported function"):
            safe_eval_math("open('foo')", {})

    def test_rejects_unknown_variable(self):
        with pytest.raises(ValueError, match="unsupported"):
            safe_eval_math("x + 1", {})


# -- Math fields --------------------------------------------------------------


class TestMathFields:
    def test_basic_math_field(self):
        rules = [
            {
                "name": "rate",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"bytes=(?P<bytes>\d+) seconds=(?P<seconds>\d+)"),
                },
                "fields": {
                    "rate": {
                        "math": "bytes / seconds",
                        "vars": {
                            "bytes": {"lookup": "MESSAGE.bytes", "type": "float"},
                            "seconds": {"lookup": "MESSAGE.seconds", "type": "float"},
                        },
                    },
                },
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "bytes=1000 seconds=5"}
        points = app.parse_message(msg)
        assert len(points) == 1
        assert points[0]["fields"]["rate"] == 200.0

    def test_math_field_with_type(self):
        rules = [
            {
                "name": "test",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"a=(?P<a>\d+) b=(?P<b>\d+)"),
                },
                "fields": {
                    "result": {
                        "math": "a + b",
                        "vars": {
                            "a": {"lookup": "MESSAGE.a", "type": "int"},
                            "b": {"lookup": "MESSAGE.b", "type": "int"},
                        },
                        "type": "int",
                    },
                },
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "a=3 b=7"}
        points = app.parse_message(msg)
        assert points[0]["fields"]["result"] == 10
        assert isinstance(points[0]["fields"]["result"], int)

    def test_math_field_with_math_function(self):
        rules = [
            {
                "name": "test",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"val=(?P<val>\d+)"),
                },
                "fields": {
                    "log_val": {
                        "math": "log10(val)",
                        "vars": {
                            "val": {"lookup": "MESSAGE.val", "type": "float"},
                        },
                    },
                },
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "val=1000"}
        points = app.parse_message(msg)
        assert points[0]["fields"]["log_val"] == pytest.approx(3.0)

    def test_math_field_missing_var_returns_none(self):
        rules = [
            {
                "name": "test",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"a=(?P<a>\d+)"),
                },
                "fields": {
                    "result": {
                        "math": "a + b",
                        "vars": {
                            "a": "MESSAGE.a",
                            "b": "MESSAGE.b",  # b not captured
                        },
                    },
                },
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "a=5"}
        match = re.match(rules[0]["match"]["regex"], msg["MESSAGE"])
        result = app.eval_math_field(rules[0], msg, match, rules[0]["fields"]["result"])
        assert result is None

    def test_math_field_mixed_with_regular_fields(self):
        rules = [
            {
                "name": "test",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"a=(?P<a>\d+) b=(?P<b>\d+)"),
                },
                "fields": {
                    "sum": {
                        "math": "a + b",
                        "vars": {
                            "a": {"lookup": "MESSAGE.a", "type": "float"},
                            "b": {"lookup": "MESSAGE.b", "type": "float"},
                        },
                    },
                    "raw_a": {"lookup": "MESSAGE.a", "type": "int"},
                },
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "a=3 b=7"}
        points = app.parse_message(msg)
        assert points[0]["fields"]["sum"] == 10.0
        assert points[0]["fields"]["raw_a"] == 3

    def test_math_field_with_constant(self):
        rules = [
            {
                "name": "test",
                "match": {
                    "key": "MESSAGE",
                    "regex": re.compile(r"bytes=(?P<bytes>\d+)"),
                },
                "fields": {
                    "kb": {
                        "math": "bytes / 1024",
                        "vars": {
                            "bytes": {"lookup": "MESSAGE.bytes", "type": "float"},
                        },
                    },
                },
            }
        ]
        app = make_app(rules=rules)
        msg = {"MESSAGE": "bytes=2048"}
        points = app.parse_message(msg)
        assert points[0]["fields"]["kb"] == 2.0
