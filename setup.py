#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-kinesis",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_kinesis"],
    install_requires=[
        "singer-python==5.0.12",
        "boto3==1.9.36"
    ],
    entry_points="""
    [console_scripts]
    target-kinesis=target_kinesis:main
    """,
    packages=["target_kinesis"],
    package_data = {},
    include_package_data=True,
)
