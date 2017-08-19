#!/usr/bin/env python
#
# Copyright (c) 2017 luyi@neucloud.cn
#

from setuptools import setup, find_packages

setup_options = dict(
  name='webhdfs-client',
  version='0.1.0',
  description='A WebHDFS Client',
  author='luff',
  url='https://github.com/luff/webhdfs-client',
  install_requires=[
    'click',
    'requests>=2.12.0'
  ],
  packages=find_packages(),
  entry_points={
    'console_scripts': [
      # command=package.module:function
      'whdfsc=webhdfs.cli:hdfs_cli'
    ]
  }
)

setup(**setup_options)

