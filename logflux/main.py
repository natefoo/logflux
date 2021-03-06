#!/usr/bin/env python
from __future__ import print_function

import argparse
import json
import re
import sys
import traceback
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
from errno import ENOENT
from os import (
    getpid,
    unlink
)
from threading import current_thread

import yaml
from influxdb import InfluxDBClient


CONFIG_DEFAULT = 'logflux.yaml'
SOCK = '/run/logflux.sock'
DATABASE = 'logflux'
TYPE_MAP = {
    'int': int,
    'float': float,
}


class MessageHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.app.handle(self)


class ForkingServer(socketserver.ForkingMixIn, socketserver.UnixDatagramServer):
    def log(self, msg, *args, **kwargs):
        log('[pid {}] {}'.format(getpid(), msg), *args, **kwargs)


class ThreadingServer(socketserver.ThreadingMixIn, socketserver.UnixDatagramServer):
    def log(self, msg, *args, **kwargs):
        log('[tid {}] {}'.format(current_thread().ident, msg), *args, **kwargs)


class Server(socketserver.UnixDatagramServer):
    def log(self, msg, *args, **kwargs):
        log(msg, *args, **kwargs)


SERVER_CLASS_MAP = {
    'forking': ForkingServer,
    'threading': ThreadingServer,
}


class LogFluxApplication(object):
    def __init__(self):
        self.args = None
        self.config = None
        self.rules = []
        self.__client = None
        self.server = None
        self.message_id = 0
        self.message_loader = None
        self.setup()

    @property
    def socket(self):
        return self.config.get('socket', SOCK)

    @property
    def influx_config(self):
        return self.config.get('influx', {})

    @property
    def database(self):
        return self.config.get('database', DATABASE)

    @property
    def client(self):
        if self.__client is None:
            self.__client = InfluxDBClient(**self.influx_config)
        return self.__client

    def debug(self, msg, *args, **kwargs):
        if self.args.debug:
            self.log(msg, *args, **kwargs)

    def log(self, msg, *args, **kwargs):
        self.server.log('[msg {}]: {}'.format(self.message_id, msg), *args, **kwargs)

    def setup(self):
        self.parse_arguments()
        self.read_config()
        self.compile_rules()
        self.setup_influx()

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='Feed syslog messages to InfluxDB')
        parser.add_argument('--config', '-c', default=CONFIG_DEFAULT)
        parser.add_argument('--debug', '-d', action='store_true', default=False)
        self.args = parser.parse_args()

    def read_config(self):
        log('reading config from: {}', self.args.config)
        with open(self.args.config) as config_fh:
            self.config = yaml.safe_load(config_fh)
        self.rules = self.config.get('rules', [])
        if self.config.get('message_format') == 'json':
            self.message_loader = self.load_message_json
        elif self.config.get('message_format') == 'legacy':
            self.message_loader = self.load_message_legacy

    def compile_rules(self):
        log('compiling rule regular expressions...')
        for rule in self.rules:
            key = rule['match']['key']
            pattern = rule['match']['regex']
            log("{}: '{}' rexep: {}", rule['name'], key, pattern)
            rule['match']['regex'] = re.compile(pattern)
        log('done')

    def setup_influx(self):
        # TODO: support per-rule DB
        self.client.create_database(self.database)
        self.client.switch_database(self.database)

    def load_message_json(self, raw):
        return json.loads(raw.decode('utf-8').strip())

    def load_message_legacy(self, raw):
        r = {}
        lines = iter(raw.decode('utf-8').splitlines())
        for line in lines:
            if line:
                k, v = line.split(': ', 1)
                r[k] = v
            else:
                break
        r['message'] = '\n'.join(lines)
        return r

    def load_message(self, raw):
        if not self.message_loader:
            try:
                self.load_message_json(raw)
                self.message_loader = self.load_message_json
                log('first message appears to be JSON format, setting loader to JSON')
            except ValueError as exc:
                log('first message does not appear to be JSON format, setting loader to legacy')
                self.message_loader = self.load_message_legacy
        return self.message_loader(raw)

    def check_re(self, msg, key, pattern):
        try:
            return re.match(pattern, msg[key].strip())
        except KeyError:
            self.log("expected key '{}' not in message", key)

    def rule_value_match_lookup(self, rule, match, lookup):
        rule_key = rule['match']['key']
        key, matchkey = lookup.split('.', 1)
        if key != rule_key:
            raise NotImplemented("invalid key, fields/tags using regex lookup cannot be performed on message parts "
                                 "other than the '{}' key: {}".format(rule_key, key))
        return match.groupdict()[matchkey]


    def rule_value_lookup(self, rule, msg, match, lookup):
        value = None
        valtypef = None
        if isinstance(lookup, dict):
            if 'type' in lookup:
                valtypef = TYPE_MAP[lookup['type']]
            lookup = lookup['lookup']
        try:
            if '.' in lookup:
                value = self.rule_value_match_lookup(rule, match, lookup)
            else:
                value = msg[lookup]
        except KeyError:
            self.log("error: invalid field/tag reference: {}; matches were:", lookup)
            for k, v in match.groupdict().items():
                self.log("  {}: {}", k, v)
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
        measurement = rule['name']
        time = msg['@timestamp']
        fields = self.get_fields_tags('fields', rule, msg, match, default={'value': 'message'})
        assert fields, "Unable to populate field values"
        tags = self.get_fields_tags('tags', rule, msg, match)
        m = {
            'measurement': measurement,
            'time': time,
            'fields': fields,
        }
        if tags:
            m['tags'] = tags
        return m

    def parse_message(self, msg):
        points = []
        for rule in self.rules:
            key = rule['match']['key']
            pattern = rule['match']['regex']
            m = self.check_re(msg, key, pattern)
            if m:
                try:
                    points.append(self.make_point(rule, msg, m))
                except Exception as exc:
                    self.log('Failed to generate point: {}', str(exc))
        return points

    def send_points(self, points):
        for point in points:
            tags = ''
            if point.get('tags'):
                tags = ',' + fmtarg(point.get('tags', {}))
            self.log('{measurement}{tags} {fields} {timestamp}'.format(
                measurement=point['measurement'],
                tags=tags,
                fields=fmtarg(point['fields']),
                timestamp=point['time']))
        if points:
            self.client.write_points(points)

    def handle(self, handler):
        self.message_id += 1
        points = []
        raw = handler.request[0]
        self.debug('received message: {}', raw)
        try:
            msg = self.load_message(raw)
            if not msg:
                return
            points = self.parse_message(msg)
            self.send_points(points)
        except Exception:
            self.log("Caught exception handling message", exception=True)

    def serve(self):
        try:
            unlink(self.socket)
        except (IOError, OSError) as exc:
            if exc.errno != ENOENT:
                raise
        log('binding socket {}', self.socket)
        if self.config.get('server_type'):
            self.server = SERVER_CLASS_MAP[self.config['server_type']](self.socket, MessageHandler)
        else:
            self.server = Server(self.socket, MessageHandler)
        self.server.app = self
        try:
            self.server.serve_forever()
        finally:
            unlink(self.socket)


def log(msg, *args, **kwargs):
    exception = kwargs.pop('exception', False)
    if exception and sys.exc_info()[0] is not None:
        print(traceback.format_exc(), end='', file=sys.stderr)
    print(msg.format(*args, **kwargs), file=sys.stderr)


def influxarg(v):
    if isinstance(v, float):
        return str(v)
    elif isinstance(v, int):
        return str(v) + 'i'
    else:
        return '"' + str(v).replace('"', '\\"') + '"'


def fmtarg(d):
    return ','.join(['{}={}'.format(k, influxarg(v)) for k, v in d.items()])


def main():
    app = LogFluxApplication()
    app.serve()


if __name__ == '__main__':
    main()
