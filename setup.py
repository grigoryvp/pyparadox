#!/usr/bin/env python
# coding:utf-8 vi:et:ts=2

# pyparadox distribute install.
# Copyright 2013 Grigory Petrov
# See LICENSE for details.

import os
import setuptools
import subprocess

from pyparadox.info import NAME_SHORT, DESCR, VER_MAJOR, VER_MINOR

##  Get version from VCS.
VER_BUILD = 0
try :
  ##  If this file exist, package is installed from pypi and this file is
  ##  executed with 'egg_info' command-line argument.
  with open( 'PKG-INFO' ) as oFile :
    import rfc822
    import re
    sVer = rfc822.Message( oFile ).get( 'version' )
    if sVer :
      oMatch = re.match( r'\d+\.\d+\.(\d+)', sVer.strip() )
      if oMatch :
        VER_BUILD = int( oMatch.group( 1 ) )
except IOError :
  DIR_THIS = os.path.dirname( os.path.abspath( __file__ ) )
  sId = subprocess.check_output( [ 'hg', '-R', DIR_THIS, 'id', '-n' ] )
  VER_BUILD = int( sId.strip( '+\n' ) )

VER_TXT = ".".join( map( str, [ VER_MAJOR, VER_MINOR, VER_BUILD ] ) )

setuptools.setup(
  name         = NAME_SHORT,
  version      = VER_TXT,
  description  = DESCR,
  author       = "Grigory Petrov",
  author_email = "grigory.v.p@gmail.com",
  url          = "http://bitbucket.org/eyeofhell/{0}".format( NAME_SHORT ),
  license      = 'GPLv3',
  packages     = [ NAME_SHORT ],
  zip_safe     = True,
  install_requires = [],
  entry_points = {
    'console_scripts' : [
      '{0} = {0}:main'.format( NAME_SHORT ),
    ],
  },
  ##  http://pypi.python.org/pypi?:action=list_classifiers
  classifiers  = [
    ('Development Status :: 1 - Planning'),
    ('Environment :: Console'),
    ('Intended Audience :: Developers'),
    ('License :: OSI Approved :: GNU General Public License v3 (GPLv3)'),
    ('Natural Language :: English'),
    ('Operating System :: OS Independent'),
    ('Programming Language :: Python :: 2.7'),
    ('Topic :: Software Development :: Libraries'),
  ]
)

