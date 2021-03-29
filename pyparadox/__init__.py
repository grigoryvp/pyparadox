#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pyparadox main library code.
# Copyright 2013 Grigory Petrov
# See LICENSE for details.


try:
    import __builtin__
    byte_to_int = ord
    int_to_byte = chr
    empty_bytes = ""
    zero_byte = "\0"
except ImportError:
    import builtins
    __builtin__ = builtins
    byte_to_int = int
    int_to_byte = lambda v: bytes([v])
    empty_bytes = b""
    zero_byte = b"\0"
import struct
from datetime import date, time, datetime
from functools import reduce


MSG_ERR_FILE = "File \"{0}\" is not a paradox data file"
MSG_ERR_ENCRYPTION = "Encrypted files are not supported"
MSG_ERR_FIELD_TYPE = "Unsupported field type 0x{:02x}"
MSG_ERR_INCREMENTAL = "No autoincrement field for incremental load"


class Shutdown( Exception ): pass


##  Expected error.
class Error( Exception, object ):


  def __init__( self, s_msg = None ):
    super( Error, self ).__init__( s_msg )


class CDatabase( object ):


  def __init__( self ):

    self.recordSize = None
    self.headerSize = None
    ##  |0|: indexed data file, |2|: nont-indexed data file.
    self.fileType   = None
    ##  Block size in 1k chunks. Table contains max 0xFFFF blocks, so if this
    ##  field is 1 max table size is 64mmb, if this field is 2 max table size
    ##  is 128mb etc. Max 32 (2Gb table).
    self.maxTableSize = None
    self.recordsCount = None
    self.fieldsCount = None
    self.sortOrder = None
    ## |0|: no write protection, |1|: write protected.
    self.writeProtected = None
    self.versionCommon = None
    self.nextAutoInc = None
    self.versionData = None
    ##  Codepage as for |DOS| interrupt 0x21 function 0x66.
    self.codepage = None
    self.tableName = None
    ##  ASCII string representing sort order.
    self.sortOrderTxt = None
    ##  List of |CField|'s.
    self.fields = []
    ##  List of |CRecord|'s.
    self.records = []


class CField( object ):


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
    ALPHA:
      { 'name'   : "text",
        'sqlite' : 'TEXT' },
    DATE:
      { 'name'   : "date",
        'sqlite' : 'TEXT' },
    INT16:
      { 'name'   : "int16",
        'sqlite' : 'INTEGER' },
    INT32:
      { 'name'   : "int32",
        'sqlite' : 'INTEGER' },
    INT64:
      { 'name'   : "int64",
        'sqlite' : 'INTEGER' },
    LOGICAL:
      { 'name'   : "bool",
        'sqlite' : 'INTEGER' },
    MEMO_BLOB:
      { 'name'   : "mblob",
        'sqlite' : 'BLOB' },
    BLOB:
      { 'name'   : "blob",
        'sqlite' : 'BLOB' },
    GRAPHICS_BLOB:
      { 'name'   : "gblob",
        'sqlite' : 'BLOB' },
    TIME:
      { 'name'   : "time",
        'sqlite' : 'TEXT' },
    TIMESTAMP:
      { 'name'   : "datetime",
        'sqlite' : 'TEXT' },
    AUTOINCREMENT:
      { 'name'   : "autoincrement",
        'sqlite' : 'INTEGER PRIMARY KEY' },
    BYTES:
      { 'name'   : "bytes",
        'sqlite' : 'BLOB'  }
  }


  def __init__( self ):
    self.type = None
    self.size = None
    self.name = None


  def typeAsTxt( self ):
    return CField.ABOUT_TYPES[ self.type ][ 'name' ]


  def isAutoincrement( self ):
    return self.type == CField.AUTOINCREMENT


  def toSqliteType( self ):
    return CField.ABOUT_TYPES[ self.type ][ 'sqlite' ]


class CRecord( object ):


  def __init__( self ):
    self.fields = []


  def __str__( self ):
    lTxt = []
    for uField in self.fields:
      if str == type( uField ):
        lTxt.append( u'"{}"'.format( uField.decode( 'cp1251' ) ) )
      elif bool == type( uField ):
        lTxt.append( "true" if uField else "false" )
      else:
        lTxt.append( str( uField ) )
    return " ".join( lTxt )


class CReader( object ):


  def __init__( self, s_data ):
    self._data_s = s_data
    self._offset_n = 0
    self._offsets_l = []


  def read( self, s_format, f_dontmove = False ):
    ABOUT = { '!': 0, '<': 0, 'B': 1, 'h': 2, 'H': 2, 'I': 4, 'f': 4 }
    nLen = reduce( lambda x, y: x + y, [ ABOUT[ x ] for x in s_format ] )
    sSplice = self._data_s[ self._offset_n : self._offset_n + nLen ]
    if len( sSplice ) < nLen:
      raise Error()
    gItems = struct.unpack( s_format, sSplice )
    if not f_dontmove:
      self._offset_n += nLen
    return gItems if len( gItems ) > 1 else gItems[ 0 ]


  def readArray( self, n_len ):
    sSplice = self._data_s[ self._offset_n : self._offset_n + n_len ]
    self._offset_n += n_len
    return sSplice


  def push( self, n_newOffset ):
    self._offsets_l.append( self._offset_n )
    self._offset_n = n_newOffset


  def pop( self ):
    self._offset_n = self._offsets_l.pop()


  def offset( self ):
    return self._offset_n


  def size( self ):
    return len( self._data_s )


class CReaderParadox( CReader ):


  def readStr( self ):
    sStr = empty_bytes
    while True:
      nChar = self.read( '<B' )
      if 0 == nChar:
        break
      sStr += int_to_byte( nChar )
    ##  ASCII.
    return sStr


  def readNumber( self, s_format ):
    ABOUT_SIZE = { 'B': 1, 'h': 2, 'I': 4, 'i': 4, 'Q': 8, 'd': 8 }
    size = ABOUT_SIZE[ s_format ]
    sData = self.readArray( size )

    ##  High bit is set for positive numbers.
    if byte_to_int( sData[ 0 ] ) & 0x80:
      sData = int_to_byte(byte_to_int(sData[0]) & (~ 0x80)) + sData[1:]
      return struct.unpack( '!{}'.format( s_format ), sData )[ 0 ]
    return - struct.unpack( '!{}'.format( s_format ), sData )[ 0 ]


  def readField( self, o_field ):
    ABOUT = {
      CField.ALPHA:         self.readFieldAlpha,
      CField.DATE:          self.readFieldDate,
      CField.INT16:         self.readFieldInt16,
      CField.INT32:         self.readFieldInt32,
      CField.INT64:         self.readFieldInt64,
      CField.LOGICAL:       self.readFieldLogical,
      CField.MEMO_BLOB:     self.readFieldMemoBlob,
      CField.BLOB:          self.readFieldBlob,
      CField.GRAPHICS_BLOB: self.readFieldGraphicsBlob,
      CField.TIME:          self.readFieldTime,
      CField.TIMESTAMP:     self.readFieldTimestamp,
      CField.AUTOINCREMENT: self.readFieldAutoincrement,
      CField.BYTES:         self.readFieldBytes }
    if o_field.type not in ABOUT:
      raise Error( MSG_ERR_FIELD_TYPE.format( o_field.type ) )
    return ABOUT[ o_field.type ]( o_field )


  def readFieldAlpha( self, o_field ):
    ##  Zero-padded text.
    return self.readArray( o_field.size ).replace( zero_byte, empty_bytes )


  def readFieldDate( self, o_field ):
    nTime = (self.readNumber( 'I' ) - 719163) * 86400 
    ##  Number of days since 01.01.0001
    try:
      return date.fromtimestamp( nTime )
    except ValueError:
      return date.max if nTime > 0 else date.min


  def readFieldInt16( self, o_field ):
    return self.readNumber( 'h' )


  def readFieldInt32( self, o_field ):
    return self.readNumber( 'i' )


  def readFieldInt64( self, o_field ):
    return self.readNumber( 'Q' )


  def readFieldLogical( self, o_field ):
    return self.readNumber( 'B' ) != 0


  def readFieldMemoBlob( self, o_field ):
    ##! Not implemented.
    self.readArray( o_field.size )
    return ''


  def readFieldBlob( self, o_field ):
    ##! Not implemented.
    self.readArray( o_field.size )
    return ''


  def readFieldGraphicsBlob( self, o_field ):
    ##! Not implemented.
    self.readArray( o_field.size )
    return ''


  def readFieldTime( self, o_field ):
    nTime = self.readNumber( 'I' )
    ##  Number of milliseconds since midnight, which is |0|.
    nHour = nTime / 3600000
    nMinute = nTime / 60000 - nHour * 60
    nSecond = nTime / 1000 - nHour * 3600 - nMinute * 60
    return time( nHour, nMinute, nSecond )


  def readFieldTimestamp( self, o_field ):
    ##  Number of milliseconds since 02.01.0001
    nTime = (self.readNumber( 'd' ) / 1000) - (719163 * 86400)
    try:
      return datetime.fromtimestamp( nTime )
    except ValueError:
      return datetime.max if nTime > 0 else datetime.min


  def readFieldAutoincrement( self, o_field ):
    return self.readNumber( 'I' )


  def readFieldBytes( self, o_field ):
    ##! Not implemented.
    self.readArray( o_field.size )
    return ''


##i {start} If not |None|, defines first autoincrement index to load.
def open( fp, mode = 'rb', start = None, shutdown = None ):
  assert 'rb' == mode
  with __builtin__.open( fp, mode ) as oFile:
    oReader = CReaderParadox( oFile.read() )
  oDb = CDatabase()

  ##  Common header.
  oDb.recordSize = oReader.read( '<H' )
  oDb.headerSize = oReader.read( '<H' )
  oDb.fileType = oReader.read( '<B' )
  if oDb.fileType not in [ 0, 2 ]:
    raise Error( MSG_ERR_FILE.format( fp ) )
  oDb.maxTableSize = oReader.read( '<B' )
  if oDb.maxTableSize not in range( 1, 32 + 1 ):
    raise Error( MSG_ERR_FILE.format( fp ) )
  oDb.recordsCount = oReader.read( '<I' )
  oReader.read( '<H' ) # Next block.
  oReader.read( '<H' ) # File blocks.
  oReader.read( '<H' ) # First block.
  oReader.read( '<H' ) # Last block.
  oReader.read( '<H' ) # Unknown.
  oReader.read( '<B' ) # Rebuild flag.
  oReader.read( '<B' ) # Index field number.
  oReader.read( '<I' ) # Primary index pointer.
  oReader.read( '<I' ) # Unknown.
  oReader.readArray( 3 ) # Unknown.
  oDb.fieldsCount = oReader.read( '<H' )
  oReader.read( '<H' ) # Primary key fields.
  oReader.read( '<I' ) # Encryption.
  oDb.sortOrder = oReader.read( '<B' )
  oReader.read( '<B' ) # Rebuild flag.
  oReader.read( '<H' ) # Unknown.
  oReader.read( '<B' ) # Change count.
  oReader.read( '<B' ) # Unknown.
  oReader.read( '<B' ) # Unknown.
  oReader.read( '<I' ) # ** table name.
  oReader.read( '<I' ) # * list of field identifiers.
  ABOUT = { 0: False, 1: True }
  nData = oReader.read( '<B' )
  if nData not in ABOUT:
    raise Error( MSG_ERR_FILE.format( fp ) )
  oDb.writeProtected = ABOUT[ nData ]
  oDb.versionCommon = oReader.read( '<B' )
  oReader.read( '<H' ) # Unknown.
  oReader.read( '<B' ) # Unknown.
  nAuxiliaryPassCount = oReader.read( '<B' )
  if 0 != nAuxiliaryPassCount:
    raise Error( MSG_ERR_ENCRYPTION )
  oReader.read( '<H' ) # Unknown.
  nCryptInfoFieldPtr = oReader.read( '<I' )
  if 0 != nCryptInfoFieldPtr:
    raise Error( MSG_ERR_ENCRYPTION )
  oReader.read( '<I' ) # * crypt info field end.
  oReader.read( '<B' ) # Unknown.
  oDb.nextAutoInc = oReader.read( '<I' )
  oReader.read( '<H' ) # Unknown.
  oReader.read( '<B' ) # Index update flag.
  oReader.readArray( 5 ) # Unknown.
  oReader.read( '<B' ) # Unknown.
  oReader.read( '<H' ) # Unknown.

  ##  4+ data file header (and pyparadox reads only data files).
  oDb.versionData = oReader.read( '<H' )
  nData = oReader.read( '<H' )
  if nData != oDb.versionData:
    raise Error( MSG_ERR_FILE.format( fp ) )
  oReader.read( '<I' ) # Unknown.
  oReader.read( '<I' ) # Unknown.
  oReader.read( '<H' ) # Unknown.
  oReader.read( '<H' ) # Unknown.
  oReader.read( '<H' ) # Unknown.
  oDb.codepage = oReader.read( '<H' )
  oReader.read( '<I' ) # Unknown.
  oReader.read( '<H' ) # Unknown.
  oReader.readArray( 6 ) # Unknown.

  ##  Fields
  for i in range( oDb.fieldsCount ):
    oField = CField()
    oField.type = oReader.read( '<B' )
    oField.size = oReader.read( '<B' )
    oDb.fields.append( oField )

  oReader.read( '<I' ) # Table name pointer.
  oReader.readArray( oDb.fieldsCount * 4 ) # Field name pointers.

  ##  Table name as original file name with extension. Padded with zeroes.
  sTableName = empty_bytes
  while True:
    nChar = oReader.read( '<B', f_dontmove = True )
    if 0 == nChar:
      break
    sTableName += int_to_byte( oReader.read( '<B' ) )
  while True:
    nChar = oReader.read( '<B', f_dontmove = True )
    if 0 != nChar:
      break
    oReader.read( '<B' )
  oDb.tableName = sTableName

  ##  Field names.
  for oField in oDb.fields:
    oField.name = oReader.readStr()
  if len( oDb.fields ) != oDb.fieldsCount:
    raise Error( MSG_ERR_FILE.format( fp ) )

  oReader.readArray( oDb.fieldsCount * 2 ) # Field numbers.
  oDb.sortOrderTxt = oReader.readStr()

  ##  Data blocks starts at |header_size| offset.
  oReader.push( oDb.headerSize )

  if start != None and oDb.fields[ 0 ].type != CField.AUTOINCREMENT:
    raise Error( MSG_ERR_INCREMENTAL )

  ##  Records.
  nRemaining = oReader.size() - oReader.offset()
  nBlockSize = oDb.maxTableSize * 1024
  nBlocks = nRemaining // nBlockSize
  nOffsetStart = oReader.offset()
  if 0 != nRemaining % nBlockSize:
    raise Error( MSG_ERR_FILE.format( fp ) )
  ##  Read blocks from end so we can pick new autoincrement fields fast.
  for nBlock in range( nBlocks - 1, -1, -1 ):
    oReader.push( nOffsetStart + nBlock * nBlockSize )
    oReader.read( '<H' ) # Unknown.
    oReader.read( '<H' ) # Block number.
    ##  Amount of data in additional to one record.
    nAddDataSize = oReader.read( '<h' )
    ##  Negative if block don't have records.
    if nAddDataSize >= 0:
      nRecords = nAddDataSize / oDb.recordSize + 1
      ##  Read records in block from end so we pick newest first.
      for nRecord in range( int(nRecords) - 1, -1, -1 ):
        oReader.push( oReader.offset() + nRecord * oDb.recordSize )
        oRecord = CRecord()
        for i, oField in enumerate( oDb.fields ):
          ##  Converting big database from start may take long time, external
          ##  shutdown can abort this process.
          if hasattr( shutdown, 'is_set' ) and shutdown.is_set():
            raise Shutdown()
          uVal = oReader.readField( oField )
          ##  Incremental mode, first field is autoincrement.
          if start != None and 0 == i:
            ##  All done while reading from the end?
            if uVal < start:
              return oDb
          oRecord.fields.append( uVal )
        oDb.records.insert( 0, oRecord )
        oReader.pop()
    oReader.pop()
  if len( oDb.records ) != oDb.recordsCount:
    raise Error( MSG_ERR_FILE.format( fp ) )

  return oDb

