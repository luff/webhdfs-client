#!/usr/bin/env python
#
# Copyright (c) 2019 Lu.Yi
#

import click
import fnmatch
import json
import os
import re
import time

from .webhdfs import WebHDFS


def _glob(path):
  result = []
  magic_check = re.compile("([*?[])")

  def recursive(levels):
    lvls = levels[:]
    base = "/"
    for i, pattern in enumerate(lvls):
      if magic_check.search(pattern):
        ls = []
        try:
          ls = hdfs.list_status(base)
        except Exception as e:
          click.echo('failed list status {}'.format(base))
        ps = [ p['pathSuffix'] for p in ls ]
        for p in fnmatch.filter(ps, pattern):
          lvls[i] = p
          recursive(lvls)
        return
      else:
        base = os.path.join(base, pattern)
    try:
      hdfs.get_file_status(base)
      result.append(base)
    except Exception as e:
      return

  recursive(path.split("/")[1:])
  return result or [path]


def paths(path, magic=True):
  result = []
  magic_check = re.compile("([*?[])")

  for p in path:
    if p.startswith("~/") or p == "~":
      p = re.sub("^~", hdfs.home, p)
    elif p[0] != "/":
      p = os.path.join(hdfs.home, p)
    if p != "/":
      p = p.rstrip("/")
    if magic_check.search(p):
      result.extend(_glob(p))
    else:
      result.append(p)

  return result


@click.group()
def hdfs_cli():
  with open(os.path.expanduser('~') + '/.whdfsc.json', 'r') as f:
    cfg = json.load(f)
  global hdfs
  hdfs = WebHDFS(**cfg)


@hdfs_cli.command()
def home():
  """ get home dir
  """
  click.echo(hdfs.home)


@hdfs_cli.command()
@click.argument('target', nargs=-1, required=True)
def stat(target):
  """ get files/dirs status
  """
  for p in paths(target):
    try:
      r = hdfs.get_file_status(p)
      click.echo(json.dumps(r, indent=2))
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.argument('target', nargs=-1)
def ls(target):
  """ list files/dirs status
  """
  def get_bits(file_type, permission, acl=False):
    bt = file_type[0].lower().replace('f', '-')
    bp_all = 'rwxrwxrwx'
    bp = ''
    # convert oct string
    pm = int(permission, 8)
    for i in range(8, -1, -1):
      bp += '-' if (pm & 2**i == 0) else bp_all[8-i]
    if (pm & 2**9 != 0):
      # sticky bit
      bp = bp[:-1] + ('t' if bp[-1] == 'x' else 'T')
    ba = '+' if acl else ' '
    return bt + bp + ba

  if not target:
    target = ['~']
  for p in paths(target):
    click.secho('{}:'.format(p), reverse=True)
    try:
      res = hdfs.list_status(p)
      if not res:
        click.echo('(empty)')
        continue
      fmt = '{{}} {{:{}}} {{:{}}} {{:>{}}} {{}} {{}}'.format(
        max([len(r['owner']) for r in res]),
        max([len(r['group']) for r in res]),
        max([len(str(r['length'])) for r in res])
      )
      for r in res:
        b = get_bits(r['type'], r['permission'], r.get('aclBit', False))
        t = time.strftime(
          '%F %H:%M',
          time.localtime(r['modificationTime']/1000)
        )
        if not r['pathSuffix'] or p in ('/', ''):
          f = p + r['pathSuffix']
        else:
          f = p + '/' + r['pathSuffix']
        click.echo(
          fmt.format(b, r['owner'], r['group'], r['length'], t, f)
        )
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.argument('source', nargs=-1, required=True)
@click.argument('target', nargs=1)
def mv(source, target):
  """ move(rename) files/dirs
  """
  for s in paths(source):
    dtarget = '{}/{}'.format(target.rstrip('/'), os.path.basename(s))
    try:
      if not hdfs.rename(s, target) and not hdfs.rename(s, dtarget):
        click.echo('cannot move {} to {}'.format(s, target))
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.option('-p', '--permission', default='700')
@click.option('-f', '--force', is_flag=True)
@click.argument('source', nargs=1)
@click.argument('target', nargs=1)
def put(source, target, permission, force):
  """ copy from local
  """
  if not os.path.isfile(source):
    click.echo('no such file: {}'.format(source))
  try:
    hdfs.create(target, source, permission, force)
  except Exception as e:
    click.echo(e)


@hdfs_cli.command()
@click.option('-f', '--force', is_flag=True)
@click.argument('source', nargs=1)
@click.argument('target', nargs=1)
def get(source, target, force):
  """ copy to local
  """
  if os.path.isfile(target) and not force:
    click.echo('file already exists: {}'.format(target))
  try:
    hdfs.open(source, target)
  except Exception as e:
    click.echo(e)


@hdfs_cli.command()
@click.argument('target', nargs=-1, required=True)
def cat(target):
  """ output file content
  """
  for f in paths(target):
    try:
      hdfs.open(f)
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.option('-p', '--permission', default='700')
@click.argument('target', nargs=-1, required=True)
def mkdir(target, permission):
  """ make dirs
  """
  for d in paths(target):
    try:
      if not hdfs.mkdirs(d, permission):
        click.echo('cannot make dir {}'.format(d))
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.option('-r', '--recursive', is_flag=True)
@click.argument('target', nargs=-1, required=True)
def rm(target, recursive):
  """ delete files/dirs
  """
  for p in paths(target):
    try:
      if not hdfs.delete(p, recursive):
        click.echo('cannot delete {}'.format(p))
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.option('-o', '--owner', default='')
@click.option('-g', '--group', default='')
@click.argument('target', nargs=-1, required=True)
def chown(target, owner, group):
  """ set owner of files/dirs
  """
  for p in paths(target):
    try:
      hdfs.setowner(p, owner, group)
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.option('-p', '--permission', default='700')
@click.argument('target', nargs=-1, required=True)
def chmod(target, permission):
  """ set permission of files/dirs
  """
  for p in paths(target):
    try:
      hdfs.set_permission(p, permission)
    except Exception as e:
      click.echo(e)


@hdfs_cli.command()
@click.argument('target', nargs=-1)
def summary(target):
  """ get content summary of a dir
  """
  if not target:
    target = ['~']
  for p in paths(target):
    click.secho('{}:'.format(p), reverse=True)
    try:
      r = hdfs.get_content_summary(p)
      click.echo(json.dumps(r, indent=2))
    except Exception as e:
      click.echo(e)


if __name__ == "__main__":
  hdfs_cli()

