#!/usr/bin/env python
from __future__ import annotations

import argparse
from importlib import import_module

from .base import CONFIG_DEFAULT

SOURCE_MAP: dict[str, str] = {
    "rsyslog": "logflux.rsyslog:RsyslogApplication",
    "journald": "logflux.journald:JournaldApplication",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Feed log messages to InfluxDB")
    parser.add_argument(
        "--source",
        "-s",
        choices=SOURCE_MAP.keys(),
        default="rsyslog",
        help="Log source type (default: rsyslog)",
    )
    parser.add_argument("--config", "-c", default=CONFIG_DEFAULT)
    parser.add_argument("--debug", "-d", action="store_true", default=False)
    parser.add_argument("--verbose", "-v", action="store_true", default=False)
    parser.add_argument(
        "--telegraf",
        "-t",
        action="store_true",
        default=False,
        help="Telegraf mode: run once and exit (journald only)",
    )
    args = parser.parse_args()

    if args.verbose:
        import logflux.base

        logflux.base.VERBOSE = True

    # Lazy import to avoid requiring systemd on rsyslog-only installs
    module_path, class_name = SOURCE_MAP[args.source].rsplit(":", 1)
    module = import_module(module_path)
    app_class = getattr(module, class_name)

    app = app_class(args)
    app.run()


if __name__ == "__main__":
    main()
