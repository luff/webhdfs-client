#!/usr/bin/env python
#
# Copyright (c) 2017 luyi@neucloud.cn
#

import os
import sys
import json
import requests
import urllib3



class WebHDFS(object):

  def __init__(self, rest_api, username='', password='', insecure=False):
    self.rest_api = rest_api
    self._s = requests.session()
    if insecure:
      urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    self._verify = not insecure
    if username and password:
      self._s.auth = (username, password)
    self.home = self.get_home_dir()

  def _get_path(self, path=None):
    if not path:
      path = self.home
    elif not path.startswith('/'):
      path = '{}/{}'.format(self.home, path)
    return os.path.abspath(path)

  def _get_url(self, path=None):
    return self.rest_api + self._get_path(path)

  def _process_response(self, response):
    code = response.status_code
    if code < 400:
      return
    cl = response.headers.get('Content-Length')
    ct = response.headers.get('content-type')
    if cl and cl == '0':
      response.raise_for_status()
    if ct and ct.startswith('application/json'):
      e = json.dumps(response.json(), indent=2)
    else:
      e = response.content
    raise Exception(
      'Server return code {}, response message:\n{}'
      .format(code, e)
    )

  def get_home_dir(self):
    p = {
      'op': 'gethomedirectory'
    }
    r = self._s.get(
        self._get_url('/'), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('Path')

  def get_content_summary(self, path=None):
    p = {
      'op': 'getcontentsummary'
    }
    r = self._s.get(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('ContentSummary')

  def get_file_status(self, path):
    p = {
      'op': 'getfilestatus'
    }
    r = self._s.get(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('FileStatus')

  def list_status(self, path=None):
    p = {
      'op': 'liststatus'
    }
    r = self._s.get(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('FileStatuses').get('FileStatus')

  def open(self, path):
    p = {
      'op': 'open'
    }
    r = self._s.get(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.text

  def get(self, path, ldst):
    p = {
      'op': 'open'
    }
    r = self._s.get(
        self._get_url(path), params=p, verify=self._verify, stream=True)
    with open(ldst, 'wb') as f:
      for chunk in r.iter_content(chunk_size=2**20): 
        if chunk: # filter out keep-alive new chunks
          f.write(chunk)
    self._process_response(r)

  def concat(self, path, srcs=[]):
    if not srcs:
      return
    srcs = [ self._get_path(src) for src in srcs ]
    p = {
      'op':'concat',
      'sources': ','.join(srcs)
    }
    r = self._s.post(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)

  def append(self, path, data=None):
    p = {
      'op':'append'
    }
    r = self._s.post(
        self._get_url(path), params=p, data=data, verify=self._verify)
    self._process_response(r)

  def set_owner(self, path, owner='', group=''):
    p = {
      'op': 'setowner',
      'owner': owner,
      'group': group
    }
    r = self._s.put(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)

  def set_permission(self, path, permission='700'):
    p = {
      'op': 'setpermission',
      'permission': permission
    }
    r = self._s.put(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)

  def set_times(self, path, modificationtime=-1, accesstime=-1):
    p = {
      'op': 'settimes',
      'modificationtime': modificationtime,
      'accesstime': accesstime
    }
    r = self._s.put(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)

  def create(self, path, data=None, permission='700', overwrite=False):
    p = {
      'op':'create',
      'permission': permission,
      'overwrite': overwrite
    }
    r = self._s.put(
        self._get_url(path), params=p, data=data, verify=self._verify)
    self._process_response(r)

  def put(self, path, lsrc, permission='700', overwrite=False):
    p = {
      'op':'create',
      'permission': permission,
      'overwrite': overwrite
    }
    with open(lsrc, 'rb') as f:
      r = self._s.put(
          self._get_url(path), params=p, data=f, verify=self._verify)
      self._process_response(r)

  def create_symlink(self, path, dest, createParent=False):
    p = {
      'op':'createsymlink',
      'destination': self._get_path(dest),
      'createParent': createParent
    }
    r = self._s.put(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)

  def mkdirs(self, path, permission='700'):
    p = {
      'op':'mkdirs',
      'permission': permission
    }
    r = self._s.put(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('boolean')

  def rename(self, path, dest):
    p = {
      'op':'rename',
      'destination': self._get_path(dest)
    }
    r = self._s.put(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('boolean')

  def delete(self, path, recursive=False):
    p = {
      'op':'delete',
      'recursive': recursive
    }
    r = self._s.delete(
        self._get_url(path), params=p, verify=self._verify)
    self._process_response(r)
    return r.json().get('boolean')



def main():
  # Example
  with open(sys.path[0] + '/config.json', 'r') as f:
    cfg = json.load(f)

  hdfs = WebHDFS(**cfg)

  print " > echo -n '1234567890' > test.txt"
  hdfs.create('test.txt', data='1234567890', overwrite=True)

  print " > echo -n 'abcdefg' >> test.txt"
  hdfs.append('test.txt', data='abcdefg')

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

