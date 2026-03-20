#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
import socketserver
from errno import ENOENT
from os import getpid, unlink
from threading import current_thread
from typing import Any, Callable, Optional

from .base import LogFluxApplication, Message, Point, Rule, log

SOCK = "/run/logflux.sock"


class MessageHandler(socketserver.BaseRequestHandler):
    server: LogFluxServer

    def handle(self) -> None:
        self.server.app.handle(self)


class LogFluxServer(socketserver.UnixDatagramServer):
    app: RsyslogApplication

    def log(self, msg: str, *args: Any, **kwargs: Any) -> None:
        log(msg, *args, **kwargs)


class ForkingServer(socketserver.ForkingMixIn, LogFluxServer):
    def log(self, msg: str, *args: Any, **kwargs: Any) -> None:
        log("[pid {}] {}".format(getpid(), msg), *args, **kwargs)


class ThreadingServer(socketserver.ThreadingMixIn, LogFluxServer):
    def log(self, msg: str, *args: Any, **kwargs: Any) -> None:
        log("[tid {}] {}".format(current_thread().ident, msg), *args, **kwargs)


SERVER_CLASS_MAP: dict[str, type[LogFluxServer]] = {
    "forking": ForkingServer,
    "threading": ThreadingServer,
}


class RsyslogApplication(LogFluxApplication):
    def __init__(self, args: argparse.Namespace) -> None:
        self.server: Optional[LogFluxServer] = None
        self.message_id: int = 0
        self.message_loader: Optional[Callable[[bytes], Message]] = None
        super().__init__(args)

    @property
    def socket(self) -> str:
        rv: str = self.config.get("socket", SOCK)
        return rv

    def read_config(self) -> None:
        super().read_config()
        if self.config.get("message_format") == "json":
            self.message_loader = self.load_message_json
        elif self.config.get("message_format") == "legacy":
            self.message_loader = self.load_message_legacy

    def log_msg(self, msg: str, *args: Any, **kwargs: Any) -> None:
        assert self.server is not None
        self.server.log("[msg {}]: {}".format(self.message_id, msg), *args, **kwargs)

    def load_message_json(self, raw: bytes) -> Message:
        result: Message = json.loads(raw.decode("utf-8").strip())
        return result

    def load_message_legacy(self, raw: bytes) -> Message:
        r: dict[str, str] = {}
        lines = iter(raw.decode("utf-8").splitlines())
        for line in lines:
            if line:
                k, v = line.split(": ", 1)
                r[k] = v
            else:
                break
        r["message"] = "\n".join(lines)
        return r

    def load_message(self, raw: bytes) -> Message:
        if not self.message_loader:
            try:
                self.load_message_json(raw)
                self.message_loader = self.load_message_json
                log("first message appears to be JSON format, setting loader to JSON")
            except ValueError:
                log("first message does not appear to be JSON format, setting loader to legacy")
                self.message_loader = self.load_message_legacy
        return self.message_loader(raw)

    def make_point(self, rule: Rule, msg: Message, match: re.Match[str]) -> Point:
        measurement = rule["name"]
        time = msg["@timestamp"]
        fields = self.get_fields_tags("fields", rule, msg, match, default={"value": "message"})
        assert fields, "Unable to populate field values"
        tags = self.get_fields_tags("tags", rule, msg, match)
        m: Point = {
            "measurement": measurement,
            "time": time,
            "fields": fields,
        }
        if tags:
            m["tags"] = tags
        return m

    def handle(self, handler: MessageHandler) -> None:
        self.message_id += 1
        raw = handler.request[0]
        self.debug("received message: {}", raw)
        try:
            msg = self.load_message(raw)
            if not msg:
                return
            points = self.parse_message(msg)
            self.send_points(points)
        except Exception:
            self.log_msg("Caught exception handling message", exception=True)

    def run(self) -> None:
        try:
            unlink(self.socket)
        except (IOError, OSError) as exc:
            if exc.errno != ENOENT:
                raise
        log("binding socket {}", self.socket)
        if self.config.get("server_type"):
            self.server = SERVER_CLASS_MAP[self.config["server_type"]](self.socket, MessageHandler)
        else:
            self.server = LogFluxServer(self.socket, MessageHandler)
        self.server.app = self
        try:
            self.server.serve_forever()
        finally:
            unlink(self.socket)
