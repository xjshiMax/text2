#!/usr/bin/env python
# coding=utf-8
from setuptools import setup
setup(
    name="GE28",
    version=1.0,
    author="xj&&xh",
    author_email="coremail.cn",
  #
    install_requires = [
        'Dbutils>=1.3',
        'elasticsearch5>=5.5.5',
        'MySQL-python>=1.2'
                    ],
)