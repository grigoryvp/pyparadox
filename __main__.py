#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pyparadox
# Copyright 2011 Grigory Petrov
# See LICENSE for details.

# Test.

import pyparadox

oDb = pyparadox.open( "test.db" )
print( "record size: {}".format( oDb.record_size ) )
print( "header size: {}".format( oDb.header_size ) )
print( "file type: {}".format( oDb.file_type ) )
ABOUT = { 1 : "64m", 2 : "128M", 3 : "192M", 4 : "256M" }
print( "max table size: {}".format( ABOUT[ oDb.max_table_size ] ) )
print( "number of records: {}".format( oDb.records_count ) )
print( "Sort order: {:x}".format( oDb.sort_order ) )
print( "write protection: {}".format( oDb.write_protected ) )
print( "Common version: {:x}".format( oDb.version_common ) )
print( "Next auto increment: {}".format( oDb.next_auto_inc ) )
print( "Data version: {:04x}".format( oDb.version_data ) )
print( "Codepage: {:04x}".format( oDb.codepage ) )
print( "Table name: {}".format( oDb.table_name ) )
print( "Sort order text: {}".format( oDb.sort_order_txt ) )
print( "Fields: " )
for oField in oDb.fields :
  print( "  {} ({})".format( oField.name, oField.TypeAsTxt() ) )
print( "Records: " )
for oRecord in oDb.records :
  print( u"  {}".format( oRecord ).encode( "utf-8" ) )

