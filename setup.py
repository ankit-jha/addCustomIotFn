#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='customagg',
    version='0.0.1',
    packages=find_packages(),
    extras_require = {
        'kafka':  ['confluent-kafka==0.11.5']
    }
)
