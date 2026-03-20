#!/usr/bin/env python
import json
import socketserver
from errno import ENOENT
from os import getpid, unlink
from threading import current_thread

from .base import LogFluxApplication, log

SOCK = "/run/logflux.sock"


class MessageHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.app.handle(self)


class ForkingServer(socketserver.ForkingMixIn, socketserver.UnixDatagramServer):
    def log(self, msg, *args, **kwargs):
        log("[pid {}] {}".format(getpid(), msg), *args, **kwargs)


class ThreadingServer(socketserver.ThreadingMixIn, socketserver.UnixDatagramServer):
    def log(self, msg, *args, **kwargs):
        log("[tid {}] {}".format(current_thread().ident, msg), *args, **kwargs)


class Server(socketserver.UnixDatagramServer):
    def log(self, msg, *args, **kwargs):
        log(msg, *args, **kwargs)


SERVER_CLASS_MAP = {
    "forking": ForkingServer,
    "threading": ThreadingServer,
}


class RsyslogApplication(LogFluxApplication):
    def __init__(self, args):
        self.server = None
        self.message_id = 0
        self.message_loader = None
        super().__init__(args)

    @property
    def socket(self):
        return self.config.get("socket", SOCK)

    def read_config(self):
        super().read_config()
        if self.config.get("message_format") == "json":
            self.message_loader = self.load_message_json
        elif self.config.get("message_format") == "legacy":
            self.message_loader = self.load_message_legacy

    def log_msg(self, msg, *args, **kwargs):
        self.server.log("[msg {}]: {}".format(self.message_id, msg), *args, **kwargs)

    def load_message_json(self, raw):
        return json.loads(raw.decode("utf-8").strip())

    def load_message_legacy(self, raw):
        r = {}
        lines = iter(raw.decode("utf-8").splitlines())
        for line in lines:
            if line:
                k, v = line.split(": ", 1)
                r[k] = v
            else:
                break
        r["message"] = "\n".join(lines)
        return r

    def load_message(self, raw):
        if not self.message_loader:
            try:
                self.load_message_json(raw)
                self.message_loader = self.load_message_json
                log("first message appears to be JSON format, setting loader to JSON")
            except ValueError:
                log(
                    "first message does not appear to be JSON format, setting loader to legacy"
                )
                self.message_loader = self.load_message_legacy
        return self.message_loader(raw)

    def make_point(self, rule, msg, match):
        measurement = rule["name"]
        time = msg["@timestamp"]
        fields = self.get_fields_tags(
            "fields", rule, msg, match, default={"value": "message"}
        )
        assert fields, "Unable to populate field values"
        tags = self.get_fields_tags("tags", rule, msg, match)
        m = {
            "measurement": measurement,
            "time": time,
            "fields": fields,
        }
        if tags:
            m["tags"] = tags
        return m

    def handle(self, handler):
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

    def run(self):
        try:
            unlink(self.socket)
        except (IOError, OSError) as exc:
            if exc.errno != ENOENT:
                raise
        log("binding socket {}", self.socket)
        if self.config.get("server_type"):
            self.server = SERVER_CLASS_MAP[self.config["server_type"]](
                self.socket, MessageHandler
            )
        else:
            self.server = Server(self.socket, MessageHandler)
        self.server.app = self
        try:
            self.server.serve_forever()
        finally:
            unlink(self.socket)
