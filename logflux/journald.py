#!/usr/bin/env python
import argparse
import datetime
import os.path
import re
import sys
import traceback
from time import sleep
from datetime import timezone

import tzlocal
import yaml
from influxdb import InfluxDBClient
from systemd import journal


CONFIG_DEFAULT = 'logflux.yaml'
LAST_TIMESTAMP_FILE = '.last_timestamp'
DATABASE = 'logflux'
TYPE_MAP = {
    'int': int,
    'float': float,
}
VERBOSE = False


class LogFluxApplication(object):
    def __init__(self):
        self.args = None
        self.config = None
        self.rules = []
        self.field_matches = {}
        self.__client = None
        self.server = None
        self.message_id = 0
        self.message_loader = None
        self.setup()

    @property
    def influx_config(self):
        return self.config.get('influx', {})

    @property
    def database(self):
        return self.config.get('database', DATABASE)

    @property
    def last_timestamp_file(self):
        return self.config.get("last_timestamp_file", LAST_TIMESTAMP_FILE)

    @property
    def client(self):
        if self.__client is None:
            self.__client = InfluxDBClient(**self.influx_config)
        return self.__client

    def debug(self, msg, *args, **kwargs):
        if self.args.debug:
            log(msg, *args, **kwargs)

    def setup(self):
        self.parse_arguments()
        self.read_config()
        self.compile_rules()
        if not self.args.telegraf:
            self.setup_influx()

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='Feed syslog messages to InfluxDB')
        parser.add_argument('--config', '-c', default=CONFIG_DEFAULT)
        parser.add_argument('--debug', '-d', action='store_true', default=False)
        parser.add_argument('--telegraf', '-t', action='store_true', default=False,
            help="Telegraf mode, run once and exit, store timestamp of last run")
        parser.add_argument('--verbose', '-v', action='store_true', default=False)
        self.args = parser.parse_args()
        if self.args.verbose:
            global VERBOSE
            VERBOSE = True

    def read_config(self):
        log('reading config from: {}', self.args.config)
        with open(self.args.config) as config_fh:
            self.config = yaml.safe_load(config_fh)
        self.filters = self.config.get("filters", [])
        self.rules = self.config.get('rules', [])

    def compile_rules(self):
        log('compiling rule regular expressions...')
        pattern_matches = []
        for rule in self.rules:
            key = rule['match']['key']
            pattern = rule['match']['regex']
            log("{}: '{}' regexp: {}", rule['name'], key, pattern)
            rule['match']['regex'] = re.compile(pattern)
        log('done')

    def setup_influx(self):
        # TODO: support per-rule DB
        self.client.create_database(self.database)
        self.client.switch_database(self.database)

    def check_re(self, msg, key, pattern):
        try:
            return re.match(pattern, msg[key].strip())
        except KeyError:
            log("expected key '{}' not in message", key)

    def rule_value_match_lookup(self, rule, match, lookup):
        rule_key = rule['match']['key']
        key, matchkey = lookup.split('.', 1)
        if key != rule_key:
            raise Exception("invalid key, fields/tags using regex lookup cannot be performed on message parts "
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
            log("error: invalid field/tag reference: {}; matches were:", lookup)
            for k, v in match.groupdict().items():
                log("  {}: {}", k, v)
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
        stamp = msg["__REALTIME_TIMESTAMP"].replace(tzinfo=tzlocal.get_localzone())
        fields = self.get_fields_tags('fields', rule, msg, match, default={'value': 'MESSAGE'})
        assert fields, "Unable to populate field values"
        tags = self.get_fields_tags('tags', rule, msg, match)
        m = {
            'measurement': measurement,
            'time': int(stamp.timestamp() * 1e9),
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
                    log('Failed to generate point: {}', str(exc))
        return points

    def send_points(self, points):
        for point in points:
            tags = ''
            if point.get('tags'):
                tags = ',' + fmtarg(point.get('tags', {}))
            if self.args.telegraf or self.args.verbose:
                print('{measurement}{tags} {fields} {timestamp}'.format(
                    measurement=point['measurement'],
                    tags=tags,
                    fields=fmtarg(point['fields']),
                    timestamp=point['time']))
        if points and not self.args.telegraf:
            self.client.write_points(points)

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


def log(msg, *args, **kwargs):
    exception = kwargs.pop('exception', False)
    if exception and sys.exc_info()[0] is not None:
        print(traceback.format_exc(), end='', file=sys.stderr)
    if VERBOSE:
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
    app.run()


if __name__ == '__main__':
    main()
