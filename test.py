#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pyparadox test.
# Copyright 2013 Grigory Petrov
# See LICENSE for details.

import pyparadox


oDb = pyparadox.open( "test.db" )
print( "record size: {}".format( oDb.recordSize ) )
print( "header size: {}".format( oDb.headerSize ) )
print( "file type: {}".format( oDb.fileType ) )
ABOUT = { 1: "64m", 2: "128M", 3: "192M", 4: "256M" }
print( "max table size: {}".format( ABOUT[ oDb.maxTableSize ] ) )
print( "number of records: {}".format( oDb.recordsCount ) )
print( "Sort order: {:x}".format( oDb.sortOrder ) )
print( "write protection: {}".format( oDb.writeProtected ) )
print( "Common version: {:x}".format( oDb.versionCommon ) )
print( "Next auto increment: {}".format( oDb.nextAutoInc ) )
print( "Data version: {:04x}".format( oDb.versionData ) )
print( "Codepage: {:04x}".format( oDb.codepage ) )
print( "Table name: {}".format( oDb.tableName ) )
print( "Sort order text: {}".format( oDb.sortOrderTxt ) )
print( "Fields: " )
for oField in oDb.fields:
  print( "  {} ({})".format( oField.name, oField.typeAsTxt() ) )
print( "Records: " )
for oRecord in oDb.records:
  print( u"  {}".format( oRecord ).encode( "utf-8" ) )

