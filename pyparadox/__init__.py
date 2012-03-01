#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pyparadox
# Copyright 2011 Grigory Petrov
# See LICENSE for details.

# Main library code.

import __builtin__, struct
import threading
from datetime import date, time, datetime

MSG_ERR_FILE = "File is not a paradox data file"
MSG_ERR_ENCRYPTION = "Encrypted files are not supported"
MSG_ERR_FIELD_TYPE = "Unsupported field type 0x{:02x}"
MSG_ERR_INCREMENTAL = "No autoincrement field for incremental load"

class Shutdown( Exception ) : pass

class CDatabase( object ) :
  def __init__( self ) :
    self.record_size = None
    self.header_size = None
    ##  |0|: indexed data file, |2|: nont-indexed data file.
    self.file_type   = None
    ##  Block size in 1k chunks. Table contains max 0xFFFF blocks, so if this
    ##  field is 1 max table size is 64mmb, if this field is 2 max table size
    ##  is 128mb etc. Max 32 (2Gb table).
    self.max_table_size = None
    self.records_count = None
    self.fields_count = None
    self.sort_order = None
    ## |0|: no write protection, |1|: write protected.
    self.write_protected = None
    self.version_common = None
    self.next_auto_inc = None
    self.version_data = None
    ##  Codepage as for |DOS| interrupt 0x21 function 0x66.
    self.codepage = None
    self.table_name = None
    ##  ASCII string representing sort order.
    self.sort_order_txt = None
    ##  List of |CField|'s.
    self.fields = []
    ##  List of |CRecord|'s.
    self.records = []

class CField( object ) :
  ALPHA         = 0x01
  DATE          = 0x02
  INT16         = 0x03
  INT32         = 0x04
  INT64         = 0x06
  LOGICAL       = 0x09
  MEMO_BLOB     = 0x0C
  BLOB          = 0x0D
  GRAPHICS_BLOB = 0x10
  TIME          = 0x14
  TIMESTAMP     = 0x15
  AUTOINCREMENT = 0x16
  BYTES         = 0x18
  ABOUT_TYPES = {
    ALPHA :         "text",
    DATE :          "date",
    INT16 :         "int16",
    INT32 :         "int32",
    INT64 :         "int64",
    LOGICAL :       "bool",
    MEMO_BLOB :     "mblob",
    BLOB :          "blob",
    GRAPHICS_BLOB : "gblob",
    TIME :          "time",
    TIMESTAMP :     "datetime",
    AUTOINCREMENT : "autoincrement",
    BYTES :         "bytes" }
  def __init__( self ) :
    self.type = None
    self.size = None
    self.name = None
  def TypeAsTxt( self ) :
    return CField.ABOUT_TYPES[ self.type ]

class CRecord( object ) :
  def __init__( self ) :
    self.fields = []
  def __str__( self ) :
    lTxt = []
    for uField in self.fields :
      if str == type( uField ) :
        lTxt.append( u'"{}"'.format( uField.decode( 'cp1251' ) ) )
      elif bool == type( uField ) :
        lTxt.append( "true" if uField else "false" )
      else :
        lTxt.append( str( uField ) )
    return " ".join( lTxt )

class CReader( object ) :
  def __init__( self, i_sData ) :
    self.m_sData = i_sData
    self.m_nOffset = 0
    self.m_lOffsets = []
  def Read( self, i_sFormat, dontmove = False ) :
    ABOUT = { '!' : 0, '<' : 0, 'B' : 1, 'h' : 2, 'H' : 2, 'I' : 4, 'f' : 4 }
    nLen = reduce( lambda x, y : x + y, [ ABOUT[ x ] for x in i_sFormat ] )
    sSplice = self.m_sData[ self.m_nOffset : self.m_nOffset + nLen ]
    gItems = struct.unpack( i_sFormat, sSplice )
    if not dontmove :
      self.m_nOffset += nLen
    return gItems if len( gItems ) > 1 else gItems[ 0 ]
  def ReadArray( self, i_nLen ) :
    sSplice = self.m_sData[ self.m_nOffset : self.m_nOffset + i_nLen ]
    self.m_nOffset += i_nLen
    return sSplice
  def Push( self, i_nNewOffset ) :
    self.m_lOffsets.append( self.m_nOffset )
    self.m_nOffset = i_nNewOffset
  def Pop( self ) :
    self.m_nOffset = self.m_lOffsets.pop()
  def Offset( self ) :
    return self.m_nOffset
  def Size( self ) :
    return len( self.m_sData )

class CReaderParadox( CReader ) :
  def ReadStr( self ) :
    sStr = ""
    while True :
      nChar = self.Read( '<B' )
      if 0 == nChar :
        break
      sStr += chr( nChar )
    ##  ASCII.
    return sStr
  def ReadNumber( self, i_sFormat ) :
    ABOUT_SIZE = { 'B' : 1, 'h' : 2, 'I' : 4, 'i' : 4, 'Q' : 8, 'd' : 8 }
    sData = self.ReadArray( ABOUT_SIZE[ i_sFormat ] )
    ##  High bit is set for positive numbers.
    if ord( sData[ 0 ] ) & 0x80 :
      sData = chr( ord( sData[ 0 ] ) & (~ 0x80) ) + sData[ 1 : ]
      return struct.unpack( '!{}'.format( i_sFormat ), sData )[ 0 ]
    return - struct.unpack( '!{}'.format( i_sFormat ), sData )[ 0 ]
  def ReadField( self, i_oField ) :
    ABOUT = {
      CField.ALPHA :         self.ReadFieldAlpha,
      CField.DATE :          self.ReadFieldDate,
      CField.INT16 :         self.ReadFieldInt16,
      CField.INT32 :         self.ReadFieldInt32,
      CField.INT64 :         self.ReadFieldInt64,
      CField.LOGICAL :       self.ReadFieldLogical,
      CField.MEMO_BLOB :     self.ReadFieldMemoBlob,
      CField.BLOB :          self.ReadFieldBlob,
      CField.GRAPHICS_BLOB : self.ReadFieldGraphicsBlob,
      CField.TIME :          self.ReadFieldTime,
      CField.TIMESTAMP :     self.ReadFieldTimestamp,
      CField.AUTOINCREMENT : self.ReadFieldAutoincrement,
      CField.BYTES :         self.ReadFieldBytes }
    if i_oField.type not in ABOUT :
      raise Exception( MSG_ERR_FIELD_TYPE.format( i_oField.type ) )
    return ABOUT[ i_oField.type ]( i_oField )
  def ReadFieldAlpha( self, i_oField ) :
    ##  Zero-padded text.
    return self.ReadArray( i_oField.size ).replace( '\0', '' )
  def ReadFieldDate( self, i_oField ) :
    nTime = (self.ReadNumber( 'I' ) - 719163) * 86400 
    ##  Number of days since 01.01.0001
    try :
      return date.fromtimestamp( nTime )
    except ValueError :
      return date.max if nTime > 0 else date.min
  def ReadFieldInt16( self, i_oField ) :
    return self.ReadNumber( 'h' )
  def ReadFieldInt32( self, i_oField ) :
    return self.ReadNumber( 'i' )
  def ReadFieldInt64( self, i_oField ) :
    return self.ReadNumber( 'Q' )
  def ReadFieldLogical( self, i_oField ) :
    return self.ReadNumber( 'B' ) != 0
  def ReadFieldMemoBlob( self, i_oField ) :
    ##! Not implemented.
    self.ReadArray( i_oField.size )
    return ''
  def ReadFieldBlob( self, i_oField ) :
    ##! Not implemented.
    self.ReadArray( i_oField.size )
    return ''
  def ReadFieldGraphicsBlob( self, i_oField ) :
    ##! Not implemented.
    self.ReadArray( i_oField.size )
    return ''
  def ReadFieldTime( self, i_oField ) :
    nTime = self.ReadNumber( 'I' )
    ##  Number of milliseconds since midnight, which is |0|.
    nHour = nTime / 3600000
    nMinute = nTime / 60000 - nHour * 60
    nSecond = nTime / 1000 - nHour * 3600 - nMinute * 60
    return time( nHour, nMinute, nSecond )
  def ReadFieldTimestamp( self, i_oField ) :
    ##  Number of milliseconds since 02.01.0001
    nTime = (self.ReadNumber( 'd' ) / 1000) - (719163 * 86400)
    try :
      return datetime.fromtimestamp( nTime )
    except ValueError :
      return datetime.max if nTime > 0 else datetime.min
  def ReadFieldAutoincrement( self, i_oField ) :
    return self.ReadNumber( 'I' )
  def ReadFieldBytes( self, i_oField ) :
    ##! Not implemented.
    self.ReadArray( i_oField.size )
    return ''

##i {start} If not |None|, defines first autoincrement index to load.
def open( fp, mode = 'r', start = None, shutdown = None ) :
  assert 'r' == mode
  with __builtin__.open( fp, mode ) as oFile :
    oReader = CReaderParadox( oFile.read() )
  oDb = CDatabase()

  ##  Common header.
  oDb.record_size = oReader.Read( '<H' )
  oDb.header_size = oReader.Read( '<H' )
  oDb.file_type = oReader.Read( '<B' )
  if oDb.file_type not in [ 0, 2 ] :
    raise Exception( MSG_ERR_FILE )
  oDb.max_table_size = oReader.Read( '<B' )
  if oDb.max_table_size not in range( 1, 32 + 1 ) :
    raise Exception( MSG_ERR_FILE )
  oDb.records_count = oReader.Read( '<I' )
  oReader.Read( '<H' ) # Next block.
  oReader.Read( '<H' ) # File blocks.
  oReader.Read( '<H' ) # First block.
  oReader.Read( '<H' ) # Last block.
  oReader.Read( '<H' ) # Unknown.
  oReader.Read( '<B' ) # Rebuild flag.
  oReader.Read( '<B' ) # Index field number.
  oReader.Read( '<I' ) # Primary index pointer.
  oReader.Read( '<I' ) # Unknown.
  oReader.ReadArray( 3 ) # Unknown.
  oDb.fields_count = oReader.Read( '<H' )
  oReader.Read( '<H' ) # Primary key fields.
  oReader.Read( '<I' ) # Encryption.
  oDb.sort_order = oReader.Read( '<B' )
  oReader.Read( '<B' ) # Rebuild flag.
  oReader.Read( '<H' ) # Unknown.
  oReader.Read( '<B' ) # Change count.
  oReader.Read( '<B' ) # Unknown.
  oReader.Read( '<B' ) # Unknown.
  oReader.Read( '<I' ) # ** table name.
  oReader.Read( '<I' ) # * list of field identifiers.
  ABOUT = { 0 : False, 1 : True }
  nData = oReader.Read( '<B' )
  if nData not in ABOUT :
    raise Exception( MSG_ERR_FILE )
  oDb.write_protected = ABOUT[ nData ]
  oDb.version_common = oReader.Read( '<B' )
  oReader.Read( '<H' ) # Unknown.
  oReader.Read( '<B' ) # Unknown.
  nAuxiliaryPassCount = oReader.Read( '<B' )
  if 0 != nAuxiliaryPassCount :
    raise Exception( MSG_ERR_ENCRYPTION )
  oReader.Read( '<H' ) # Unknown.
  nCryptInfoFieldPtr = oReader.Read( '<I' )
  if 0 != nCryptInfoFieldPtr :
    raise Exception( MSG_ERR_ENCRYPTION )
  oReader.Read( '<I' ) # * crypt info field end.
  oReader.Read( '<B' ) # Unknown.
  oDb.next_auto_inc = oReader.Read( '<I' )
  oReader.Read( '<H' ) # Unknown.
  oReader.Read( '<B' ) # Index update flag.
  oReader.ReadArray( 5 ) # Unknown.
  oReader.Read( '<B' ) # Unknown.
  oReader.Read( '<H' ) # Unknown.

  ##  4+ data file header (and pyparadox reads only data files).
  oDb.version_data = oReader.Read( '<H' )
  nData = oReader.Read( '<H' )
  if nData != oDb.version_data :
    raise Exception( MSG_ERR_FILE )
  oReader.Read( '<I' ) # Unknown.
  oReader.Read( '<I' ) # Unknown.
  oReader.Read( '<H' ) # Unknown.
  oReader.Read( '<H' ) # Unknown.
  oReader.Read( '<H' ) # Unknown.
  oDb.codepage = oReader.Read( '<H' )
  oReader.Read( '<I' ) # Unknown.
  oReader.Read( '<H' ) # Unknown.
  oReader.ReadArray( 6 ) # Unknown.

  ##  Fields
  for i in range( oDb.fields_count ) :
    oField = CField()
    oField.type = oReader.Read( '<B' )
    oField.size = oReader.Read( '<B' )
    oDb.fields.append( oField )

  oReader.Read( '<I' ) # Table name pointer.
  oReader.ReadArray( oDb.fields_count * 4 ) # Field name pointers.

  ##  Table name as original file name with extension. Padded with zeroes.
  sTableName = ""
  while True :
    nChar = oReader.Read( '<B', dontmove = True )
    if 0 == nChar :
      break
    sTableName += chr( oReader.Read( '<B' ) )
  while True :
    nChar = oReader.Read( '<B', dontmove = True )
    if 0 != nChar :
      break
    oReader.Read( '<B' )
  oDb.table_name = sTableName

  ##  Field names.
  for oField in oDb.fields :
    oField.name = oReader.ReadStr()
  if len( oDb.fields ) != oDb.fields_count :
    raise Exception( MSG_ERR_FILE )

  oReader.ReadArray( oDb.fields_count * 2 ) # Field numbers.
  oDb.sort_order_txt = oReader.ReadStr()

  ##  Data blocks starts at |header_size| offset.
  oReader.Push( oDb.header_size )

  if start != None and oDb.fields[ 0 ].type != CField.AUTOINCREMENT :
    raise Exception( MSG_ERR_INCREMENTAL )

  ##  Records.
  nRemaining = oReader.Size() - oReader.Offset()
  nBlockSize = oDb.max_table_size * 1024
  nBlocks = nRemaining // nBlockSize
  nOffsetStart = oReader.Offset()
  if 0 != nRemaining % nBlockSize :
    raise Exception( MSG_ERR_FILE )
  ##  Read blocks from end so we can pick new autoincrement fields fast.
  for nBlock in range( nBlocks - 1, -1, -1 ) :
    oReader.Push( nOffsetStart + nBlock * nBlockSize )
    oReader.Read( '<H' ) # Unknown.
    oReader.Read( '<H' ) # Block number.
    ##  Amount of data in additional to one record.
    nAddDataSize = oReader.Read( '<h' )
    ##  Negative if block don't have records.
    if nAddDataSize >= 0 :
      nRecords = nAddDataSize / oDb.record_size + 1
      ##  Read records in block from end so we pick newest first.
      for nRecord in range( nRecords - 1, -1, -1 ) :
        oReader.Push( oReader.Offset() + nRecord * oDb.record_size )
        oRecord = CRecord()
        for i, oField in enumerate( oDb.fields ) :
          ##  Converting big database from start may take long time, external
          ##  shutdown can abort this process.
          if shutdown.__class__ is threading.Event and shutdown.is_set() :
            raise Shutdown()
          uVal = oReader.ReadField( oField )
          ##  Incremental mode, first field is autoincrement.
          if start != None and 0 == i :
            ##  All done while reading from the end?
            if uVal < start :
              return oDb
          oRecord.fields.append( uVal )
        oDb.records.insert( 0, oRecord )
        oReader.Pop()
    oReader.Pop()
  if len( oDb.records ) != oDb.records_count :
    raise Exception( MSG_ERR_FILE )

  return oDb

