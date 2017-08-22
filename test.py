#!/usr/bin/env python
#
# Copyright (c) 2017 luyi@neucloud.cn
#

import json
import os
import sys

from webhdfs.webhdfs import WebHDFS


def main():
  with open(os.path.expanduser('~') + '/.whdfsc.json', 'r') as f:
    test_config = json.load(f)
  hdfs = WebHDFS(**test_config)

  print " > echo -n '1234567890' > test.txt"
  hdfs.create('test.txt', lsrc=__file__, overwrite=True)

  print " > echo -n 'abcdefg' >> test.txt"
  hdfs.append('test.txt', data='abcdefg\n')

  print " > ls test.txt"
  print hdfs.list_status('test.txt')

  print " > mkdir example"
  print hdfs.mkdirs('example')

  print " > ls example"
  print hdfs.list_status('example')

  print " > mv test.txt example/test.txt"
  print hdfs.rename('test.txt', 'example/test.txt')

  print " > ls example"
  print hdfs.list_status('example')

  print " > cat example/test.txt"
  print hdfs.open('example/test.txt')

  print " > rm -r example"
  print hdfs.delete('example', recursive=True)


if __name__ == "__main__":
  main()

