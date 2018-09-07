#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


with open('requirements.txt') as fh:
    requirements = fh.readlines()


setup(
    name = 'logflux',
    version = '0.2',
    packages = find_packages(),
    description = '(r)syslog-to-influxdb translator',
    long_description = 'Read messages from rsyslog via a socket and send regex-parsed values to InfluxDB',
    url = 'https://github.com/natefoo/logflux',
    author = 'Nate Coraor',
    author_email = 'nate@bx.psu.edu',
    license = 'MIT',
    keywords = 'syslog rsyslog influx influxdb grafana',
    install_requires = requirements,
    entry_points = {
        'console_scripts': [
            'logflux = logflux.main:main'
        ]
    },
    classifiers = [
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
    zip_safe = True
)
