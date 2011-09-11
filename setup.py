#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path
ROOT_DIR = path.abspath(path.dirname(__file__))

def get_deps():
    f = open(path.join(ROOT_DIR, "requirements.pip"), 'r')
    return [l[:-1] for l in f.readlines()]

setup(
    name='leaderboard',
    version='2.0.0',
    author='Votizen',
    author_email='team@votizen.com',
    url='http://github.com/votizen/leaderboard',
    description = 'Redis-backed leaderboard for Votizen',
    packages=find_packages(),
    zip_safe=False,
    install_requires=get_deps(),
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
