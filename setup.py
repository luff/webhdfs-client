#!/usr/bin/env python
#
# Copyright (c) 2019 Lu.Yi
#

import setuptools

with open("README.md", "r") as fh:
  long_description = fh.read()

setuptools.setup(
  name="webhdfs-client",
  version="0.1.0",
  author="Lu.Yi",
  author_email="luyiff@gmail.com",
  description="A WebHDFS Client",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/luff/webhdfs-client",
  install_requires=[
    "click",
    "requests>=2.12.0"
  ],
  packages=setuptools.find_packages(),
  entry_points={
    "console_scripts": [
      # command=package.module:function
      "whdfsc=webhdfs.cli:hdfs_cli"
    ]
  },
  classifiers=[
    "License :: OSI Approved :: MIT License",
    "Operating System :: POSIX",
  ],
)

