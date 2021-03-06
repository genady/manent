#
#    Copyright (C) 2008 Alex Gontmakher <gsasha@gmail.com>
#    License: see LICENSE.txt
#

import base64
import bz2
import Crypto.Cipher.ARC4
import logging
import os, os.path, shutil
import re
import cStringIO as StringIO
import sys
import traceback
import zlib

import manent.utils.Digest as Digest
import manent.utils.Format as Format
import Increment
import Reporting
import StreamAdapter
import manent.utils.BandwidthLimiter
import manent.utils.RemoteFSHandler as RemoteFSHandler
import manent.utils.FileIO as FileIO

#---------------------------------------------------
# Container file format:
#
# Each container consists of two kinds of data, the header and the body.
# The header contains its own header and blocks that encode metadata.
# One of the blocks is the BLOCK_TABLE that describes the blocks stored
# in the body.
#
# Header file format:
# 1. magic number "MNNT"
# 2. version number
# 3. header index.
# 4. header table length
# 5. digest of the header's header
# 6. header table entries in format:
#    7.1 entry size
#    7.2 entry code
#    7.3 entry digest
# 7. entries, encoded according to the header table
#
# Body file format:
# 1. entries, encoding according to the body table
#
# Table encoding
# Table contains three types of tags:
# 1. Compression control
#    COMPRESSION_START_<ALGORITHM>
#     The following entries are compressed by the given algorithm.
#      - size field gives the offset from which to read the compressed data
#      - digest is unused
#    COMPRESSION_END
#     The following entries are no longer compressed.
#      - size gives the total size of the *compressed* data
#      - digest is unused
#    Compression blocks should not be nested
# 2. Encryption control
#    We assume that the encryption does not change the size of the encrypted
#    data.
#    ENCRYPTION_START_<ALGORITHM>
#     The following entries are encrypted by the given algorithm.
#      - size field is the starting offset of the data
#      - digest field gives the seed used for the encryption.
#        Data for the digest should be taken from a random source.
#        For example, for arc4 encryption:
#        key = Digest(seed+password)
#        encryptor = ARC4(key)
#     The start tag should always be closed with an END tag.
#    ENCRYPTION_END
#     The following entries are no longer encrypted.
#      - size field is the size of the encrypted data (padding might have been
#        used for some encryption algorithms.
#      - digest field gives the hash of the plain data, used to verify that
#        the decryption was successful.
#    Compression blocks can be nested within encryption blocks.
#    The reverse is not a good idea (encrypted data would not compress).
# 3. Data blocks
#    Data blocks can have different codes, which are irrelevant here but have
#    a meaning for the application.
#     - size field is the starting offset of the data.
#       If the block is nested in a Compression block, then the offset is
#       within the uncompressed data (but of course! There is no meaning to
#       offset within the compressed data).
#     - digest field gives the digest of the data, for use by the application
#       and for verification
#---------------------------------------------------

MAGIC = "MNNT"
VERSION = 2

MAX_COMPRESSED_DATA = 256 * 1024

def compute_num_piggyback_headers(index):
  # Compute the number of piggybacking headers that can be
  # inserted in container of a given index.
  # For the sequence of number of allowed piggyback headers, see the test.
  if (index + 1) % 1024 == 0:
    return index % 1024
  SIZES = [1024, 256, 64, 16]
  for s in SIZES:
    if (index + 1) % s != 0 and (index + 1) % (s/4) == 0:
      return index % s
  return 0

def encode_piggyback_container_index(index):
  index_str = str(index)
  extra_chars = Digest.dataDigestSize() - len(index_str)
  index_str = index_str + (" " * extra_chars)
  return index_str
def decode_piggyback_container_index(digest):
  return int(digest)

#
# Codes for blocks stored in the body
#
CODE_DATA                  =  0
CODE_DATA_PACKER           =  1
CODE_DIR                   =  2
CODE_DIR_PACKER            =  3

#
# Codes for blocks stored in the header
#
CODE_CONTAINER_DESCRIPTOR  = 16
CODE_INCREMENT_DESCRIPTOR  = 17
CODE_BLOCK_TABLE           = 18

#
# Code for another header piggy-backed in the body
#
CODE_HEADER                = 19

CODE_CONTROL_START         = 48
#
# Codes for both kinds of blocks
#
CODE_COMPRESSION_END       = 48
CODE_COMPRESSION_BZ2       = 49
CODE_COMPRESSION_GZIP      = 50

CODE_ENCRYPTION_END        = 64
CODE_ENCRYPTION_ARC4       = 65

CODE_NAME_TABLE = {
  CODE_DATA:                 "DATA          ",
  CODE_DATA_PACKER:          "DATA_PACKER   ",
  CODE_DIR:                  "DIR           ",
  CODE_DIR_PACKER:           "DIR_PACKER    ",
  CODE_CONTAINER_DESCRIPTOR: "CONTAINER_DESC",
  CODE_INCREMENT_DESCRIPTOR: "INCREMENT_DESC",
  CODE_BLOCK_TABLE:          "BLOCK_TABLE   ",
  CODE_HEADER:               "HEADER        ",
  CODE_CONTROL_START:        "CONTROL_START ",
  CODE_COMPRESSION_END:      "COMPRESS_END  ",
  CODE_COMPRESSION_BZ2:      "COMPRESS_BZ2  ",
  CODE_COMPRESSION_GZIP:     "COMPRESS_GZIP ",
  CODE_ENCRYPTION_END:       "ENCRYPT_END   ",
  CODE_ENCRYPTION_ARC4:      "ENCRYPT_ARC4  "
}

def compute_packer_code(code):
  assert code < CODE_COMPRESSION_END
  if code % 2 == 0:
    return code+1
  else:
    return code

def is_packer_code(code):
  assert code < CODE_COMPRESSION_END
  return code%2==1

def is_user_code(code):
  return code < CODE_CONTROL_START

def code_name(code):
  return CODE_NAME_TABLE[code]

#-------------------------------------------------------------------
# Block serialization
#-------------------------------------------------------------------
def unserialize_blocks(file):
  blocks = []
  while True:
    digest = file.read(Digest.dataDigestSize())
    if digest == "":
      break
    size = Format.read_int(file)
    code = Format.read_int(file)
    blocks.append((digest, size, code))
  return blocks
def serialize_blocks(file, blocks):
  for (digest,size,code) in blocks:
    file.write(digest)
    Format.write_int(file,size)
    Format.write_int(file,code)

#-------------------------------------------------------------------
# Dump creation and reading utilities
#-------------------------------------------------------------------
class DataDumper:
  def __init__(self,file):
    self.file = file
    self.blocks = []
    self.pending_compression_start_block = None

    self.total_size = 0
    
    self.encryptor = None
    self.compressor = None
  def add_block(self, digest, code, data):
    if self.pending_compression_start_block is not None:
      self.blocks.append(self.pending_compression_start_block)
      self.pending_compression_start_block = None
    self.blocks.append((digest, len(data), code))
    if self.compressor is not None:
      data = self.__compress(data)
    if self.encryptor is not None:
      data = self.__encrypt(data)
    self.file.write(data)
    self.total_size += len(data)
  #
  # Encryption support
  #
  def start_encryption(self, algorithm_code, seed, password):
    """
    Encryption can be started only when compression is inactive
    """
    assert self.encryptor is None
    assert self.compressor is None
    
    self.blocks.append((seed, 0, algorithm_code))
    if algorithm_code == CODE_ENCRYPTION_ARC4:
      key = Digest.dataDigest(seed + password)
      self.encryptor = Crypto.Cipher.ARC4.new(key)
    self.encrypted_data_size = 0
    self.encrypted_data_digest = Digest.DataDigestAccumulator()
  def stop_encryption(self):
    assert self.encryptor is not None
    assert self.compressor is None

    self.blocks.append((self.encrypted_data_digest.digest(),
                        self.encrypted_data_size, CODE_ENCRYPTION_END))
    self.encryptor = None
  def __encrypt(self, data):
    self.encrypted_data_digest.update(data)
    self.encrypted_data_size += len(data)
    return self.encryptor.encrypt(data)
  #
  # Compression support
  #
  def start_compression(self, algorithm_code):
    """
    Compression can be started under encryption
    """
    assert self.compressor is None

    digest = Digest.dataDigest(str(len(self.blocks)))
    self.pending_compression_start_block = (digest, 0, algorithm_code)
    if algorithm_code == CODE_COMPRESSION_BZ2:
      self.compressor = bz2.BZ2Compressor(9)
    elif algorithm_code == CODE_COMPRESSION_GZIP:
      self.compressor = zlib.compressobj()
    else:
      raise Exception("Unsupported compression algorithm")
    self.compressor_algorithm = algorithm_code
    self.uncompressed_size = 0
    self.compressed_size = 0
  def stop_compression(self):
    if self.pending_compression_start_block is not None:
      # No block was added between start_compression and stop_compression;
      # in this case, the start block wasn't added, and the stop block shouldn't
      # be added either.
      self.compressor = None
      self.pending_compression_start_block = None
      return
    assert self.compressor is not None
    tail = self.compressor.flush()
    self.compressed_size += len(tail)
    
    if self.encryptor is not None:
      tail = self.__encrypt(tail)
    self.file.write(tail)
    self.total_size += len(tail)
    self.blocks.append((Digest.dataDigest(""), self.compressed_size,
                        CODE_COMPRESSION_END))
    self.compressor = None
  def __compress(self,data):
    self.uncompressed_size += len(data)
    compressed = self.compressor.compress(data)
    self.compressed_size += len(compressed)
    # The following should be necessary according to the documentation on zlib
    # module
    # However, I don't see that the compressor has unconsumed_tail attribute!
    #if self.compressor_algorithm == CODE_COMPRESSION_GZIP:
      #while self.compressor.unconsumed_tail != "":
        #print "Feeding unconsumed tail of length %d to the compressor" %\
        # len(self.compressor.unconsumed_tail)
        #compressed += self.compressor.compress(self.compressor.unconsumed_tail)
    return compressed
  #
  # Result
  #
  def get_blocks(self):
    return self.blocks

class DataDumpLoader:
  """The only mode of loading blocks from a container is through a listener.
  The listener can determine, given a digest and a code, whether a given block
  should be loaded. If the block is loaded, the listener returns it back to the
  listener through callback."""
  def __init__(self, file, blocks, password):
    self.file = file
    self.blocks = blocks
    self.password = password

    self.uncompressor = None
    self.decryptor = None
  def load_blocks(self, listener):
    total_offset = 0
    uncompressed_offset = 0
    skip_until = None
    for i in range(len(self.blocks)):
      (digest, size, code) = self.blocks[i]
      if code == CODE_ENCRYPTION_ARC4:
        # Since encryption blocks are not nested in anything,
        # we can't see start of encryption when skipping
        assert skip_until is None
        # find out if any of the blocks contained within
        # the section is actually needed
        requested = False
        for j in range(i + 1, len(self.blocks)):
          (s_digest, s_size, s_code) = self.blocks[j]
          if s_code == CODE_ENCRYPTION_END:
            break
          if (is_user_code(s_code) and
              listener.is_requested(s_digest, s_code)):
            requested = True
        else:
          raise Exception("Block table error: encryption start "
                          "without end")

        if not requested:
          skip_until = CODE_ENCRYPTION_END

        # We always perform decryption, even when it's needed
        # only for checking.
        key = Digest.dataDigest(digest + self.password)
        self.decryptor = Crypto.Cipher.ARC4.new(key)
        self.decryptor_data_digest = Digest.DataDigestAccumulator()
        self.decrypted_bytes = 0
        
      elif code == CODE_ENCRYPTION_END:
        # Encryption cannot be nested in compression
        assert skip_until != CODE_COMPRESSION_END
        if skip_until == CODE_ENCRYPTION_END:
          skipped = self.file.read(size)
          skipped = self.decryptor.decrypt(skipped)
          self.decrypted_bytes += len(skipped)
          self.decryptor_data_digest.update(skipped)
          skip_until = None
        assert self.decryptor_data_digest.digest() == digest
        self.decryptor = None
        self.decryptor_data_digest = None
      
      #
      # Process compression tags
      #
      elif code == CODE_COMPRESSION_BZ2:
        if skip_until is not None:
          assert skip_until == CODE_ENCRYPTION_END
          continue
        # find out if any of the blocks contained within
        # the section is actually needed
        requested = False
        for j in range(i+1,len(self.blocks)):
          s_digest,s_size,s_code = self.blocks[j]
          if s_code == CODE_COMPRESSION_END:
            self.uncompress_bytes = s_size
            break
          if (is_user_code(s_code) and
              listener.is_requested(s_digest, s_code)):
            requested = True
        else:
          raise Exception("Block table error: compression start "
                            "without end")

        if requested:
          self.uncompressor = bz2.BZ2Decompressor()
          self.uncompressed_buf = ""
        else:
          skip_until = CODE_COMPRESSION_END
      elif code == CODE_COMPRESSION_GZIP:
        if skip_until is not None:
          assert skip_until == CODE_ENCRYPTION_END
          continue
        # find out if any of the blocks contained within
        # the section is actually needed
        requested = False
        for j in range(i+1, len(self.blocks)):
          s_digest, s_size, s_code = self.blocks[j]
          if s_code == CODE_COMPRESSION_END:
            self.uncompress_bytes = s_size
            break
          if (is_user_code(s_code) and
              listener.is_requested(s_digest, s_code)):
            requested = True
        else:
          raise Exception("Block table error: compression start without end")

        if requested:
          self.uncompressor = zlib.decompressobj()
          self.uncompressed_buf = ""
        else:
          skip_until = CODE_COMPRESSION_END
      
      elif code == CODE_COMPRESSION_END:
        if skip_until == CODE_ENCRYPTION_END:
          assert self.uncompressor is None
          continue
        if skip_until == CODE_COMPRESSION_END:
          data = self.file.read(size)
          if self.decryptor is not None:
            data = self.decryptor.decrypt(data)
            self.decryptor_data_digest.update(data)
          skip_until = None
        else:
          if self.uncompress_bytes != 0:
            chunk = self.file.read(self.uncompress_bytes)
            if self.decryptor is not None:
              chunk = self.decryptor.decrypt(chunk)
              self.decrypted_bytes += len(chunk)
              self.decryptor_data_digest.update(chunk)
        self.uncompressor = None
        self.uncompressed_buf = ""
      #
      # Read normal data
      #
      else:
        if skip_until is not None:
          continue
        # If we're not skipping, we must also read, to preserve
        # consistency of the blocks
        # Uncompress data if necessary
        if self.uncompressor is not None:
          data = ""
          while len(data) < size:
            if len(self.uncompressed_buf) > 0:
              portion = min(size-len(data),
                            len(self.uncompressed_buf))
              data += self.uncompressed_buf[:portion]
              self.uncompressed_buf = self.uncompressed_buf[portion:]
            else:
              toread = min(8192, self.uncompress_bytes)
              self.uncompress_bytes -= toread
              logging.debug("Reading chunk to uncompress at offset %d",
                  self.file.tell())
              chunk = self.file.read(toread)
              logging.debug("Uncompressing chunk len=%s", len(chunk))
              if self.decryptor is not None:
                chunk = self.decryptor.decrypt(chunk)
                self.decryptor_data_digest.update(chunk)
                self.decrypted_bytes += len(data)
              if len(chunk) < toread:
                raise Exception(
                    "Cannot read data expected in the container")
              self.uncompressed_buf = self.uncompressor.decompress(chunk)
        else:
          data = self.file.read(size)
          if self.decryptor is not None:
            data = self.decryptor.decrypt(data)
            self.decryptor_data_digest.update(data)
            self.decrypted_bytes += len(data)

        if is_user_code(code) and listener.is_requested(digest, code):
          listener.loaded(digest, code, data)

class Container:
  """
  Represents one contiguous container that can be saved somewhere, i.e.,
  on an optical disk, in a mail system, over the network etc.

  Container consists in one of two states:
  1. Normal - in this state, blocks are added to the container
  2. Frozen - in this state, the container is completed. It can be written
              out, or its blocks can be read back.

  """
  def __init__(self, storage):
    # Configuration data
    self.storage = storage
    self.index = None
    self.sequence_id = None
    self.header_file = None
    self.body_file = None
    
    self.mode = None
    
    self.body_blocks = []

    self.report_manager = None
  def get_index(self):
    return self.index
  def get_sequence_id(self):
    return self.sequence_id
  def get_storage(self):
    return self.storage
  def set_report_manager(self, report_manager):
    self.report_manager = report_manager
  #
  # Dumping mode implementation
  #
  def start_dump(self, sequence_id, index):
    assert self.mode is None
    self.mode = "DUMP"
    self.sequence_id = sequence_id
    self.index = index
    self.header_file = self.storage.open_header_file(
      self.sequence_id, self.index)
    assert self.header_file.tell() == 0
    self.body_file = self.storage.open_body_file(
      self.sequence_id, self.index)
    assert self.body_file.tell() == 0
    self.piggyback_headers_num = 0
    self.piggyback_headers_size = 0
    self.max_num_piggyback_headers = compute_num_piggyback_headers(self.index)
    logging.debug("Container %d can add %d piggyback headers" %
        (self.index, self.max_num_piggyback_headers))

    self.body_dumper = DataDumper(self.body_file)
    self.header_dump_os = StringIO.StringIO()
    self.header_dumper = DataDumper(self.header_dump_os)

    if self.storage.get_encryption_key() != "":
      self.encryption_active = True
      self.body_dumper.start_encryption(
          CODE_ENCRYPTION_ARC4,
          os.urandom(Digest.dataDigestSize()),
          self.storage.get_encryption_key())
      self.header_dumper.start_encryption(
          CODE_ENCRYPTION_ARC4,
          os.urandom(Digest.dataDigestSize()),
          self.storage.get_encryption_key())
    else:
      self.encryption_active = False

    self.body_dumper.start_compression(CODE_COMPRESSION_BZ2)
    self.compression_active = True
    self.compressed_data = 0
  def enable_compression(self):
    if not self.compression_active:
      self.body_dumper.start_compression(CODE_COMPRESSION_BZ2)
      self.compression_active = True
      self.compressed_data = 0
  def disable_compression(self):
    if self.compression_active:
      self.body_dumper.stop_compression()
      self.compression_active = False
  def _can_add_bytes(self, data_size):
    # MAX_COMPRESSED_DATA is a safeguard for compressed data which was not yet
    # put into the output
    # 64 is for the header of the header
    current_size = (self.body_dumper.total_size +
        self.header_dumper.total_size +
        MAX_COMPRESSED_DATA + 64)
    logging.debug("Container trying to add to %d bytes block of %d, max: %d" %
        (current_size, data_size, self.storage.container_size()))
    return current_size + data_size <= self.storage.container_size()
  def is_filled_by(self, num_blocks, size_blocks):
    """Test if the given number of blocks with given size
    will fill the container. Assume that the blocks are not compressible"""
    return not self._can_add_bytes(num_blocks * (8 + Digest.dataDigestSize()) + 
        size_blocks)
  def can_add(self, data):
    if self.max_num_piggyback_headers >= 3:
      # This header is reserved exclusively for piggyback headers.
      return False
    return self._can_add_bytes(len(data))
  def add_block(self, digest, code, data):
    logging.debug("Container %d :adding block %s, %s, size:%d" % (
      self.index, base64.b64encode(digest), code_name(code), len(data)))
    if self.compression_active and self.compressed_data > MAX_COMPRESSED_DATA:
      self.body_dumper.stop_compression()
      self.body_dumper.start_compression(CODE_COMPRESSION_BZ2)
      self.compressed_data = 0

    self.body_dumper.add_block(digest, code, data)
    self.compressed_data += len(data)
    
    self.body_blocks.append((digest, code))
  #
  # Support for adding piggyback headers.
  #
  def can_add_piggyback_header(self, header_data):
    if not self._can_add_bytes(len(header_data)):
      logging.debug("No space to add the piggyback header (%d bytes)" %
          len(header_data))
      return False
    if self.piggyback_headers_num >= self.max_num_piggyback_headers:
      logging.debug("Container already has %d piggyback headers" %
          self.piggyback_headers_num)
      return False
    return True
  def add_piggyback_header(self, header_index, header_data):
    # A piggyback header is not accessed by address, so we don't need its
    # digest. We thus use the digest field to store its index.
    # We encode the index as a decimal integer string, padded with spaces.
    header_index_str = encode_piggyback_container_index(header_index)
    self.add_block(header_index_str, CODE_HEADER, header_data)
    self.piggyback_headers_num += 1
    self.piggyback_headers_size += len(header_data)
  def finish_dump(self):
    if self.compression_active:
      self.body_dumper.stop_compression()
      self.compression_active = False
    if self.encryption_active:
      self.body_dumper.stop_encryption()

    #
    # Serialize the body block table
    #
    body_table_io = StringIO.StringIO()
    body_blocks = self.body_dumper.get_blocks()
    serialize_blocks(body_table_io, body_blocks)
    body_table_str = body_table_io.getvalue()

    #
    # Serialize the header table
    #
    message = "Manent container %d of sequence '%s'" % (
      self.index, base64.urlsafe_b64encode(self.sequence_id))
    self.header_dumper.add_block(Digest.dataDigest(message),
                   CODE_CONTAINER_DESCRIPTOR, message)
    self.header_dumper.add_block(Digest.dataDigest(body_table_str),
                   CODE_BLOCK_TABLE, body_table_str)

    if self.encryption_active:
      self.header_dumper.stop_encryption()
      self.encryption_active = False

    header_blocks = self.header_dumper.get_blocks()
    header_table_io = StringIO.StringIO()
    serialize_blocks(header_table_io, header_blocks)
    header_table_str = header_table_io.getvalue()
    
    #
    # Write the header
    #
    assert self.header_file.tell() == 0
    self.header_file.write(MAGIC)
    Format.write_int(self.header_file, VERSION)
    Format.write_int(self.header_file, self.index)
    Format.write_int(self.header_file, len(header_table_str))
    self.header_file.write(Digest.dataDigest(header_table_str))
    self.header_file.write(header_table_str)
    header_dump_str = self.header_dump_os.getvalue()
    Format.write_int(self.header_file, len(header_dump_str))
    self.header_file.write(header_dump_str)
    logging.debug("Container %d has header of size %d" %
        (self.index, self.header_file.tell()))
  def get_header_contents(self):
    # Returns the contents of the header. Should be called only after
    # finish_dump has been executed
    self.header_file.seek(0)
    return self.header_file.read()
  def upload(self):
    if self.report_manager is not None:
      reporter = self.report_manager.find_reporter(
          "storage/seq-%s/container.%d/upload_start" %
          (self.sequence_id, self.index),
          0)
    self.storage.upload_container(self.sequence_id, self.index,
      self.header_file, self.body_file)
    self.header_file = None
    self.body_file = None
  #
  # Loading mode implementation
  #
  def start_load(self, sequence_id, index):
    assert self.mode is None
    self.mode = "LOAD"
    self.sequence_id = sequence_id
    self.index = index
  def list_blocks(self):
    return self.body_blocks
  def info(self):
    print "Manent container #%d of storage %s" % (
      self.index, self.storage.get_label())
  def print_blocks(self):
    class PrintHandler:
      def is_requested(self, digest, code):
        return True
      def loaded(self, digest, code, data):
        print base64.b64encode(digest), CODE_NAME_TABLE[code], len(data)
    self.load_blocks(PrintHandler())
  def test_blocks(self, filename=None):
    class TestingBlockCache:
      def __init__(self):
        pass
      def block_needed(self, digest):
        return True
      def block_loaded(self, digest, block):
        new_digest = Digest.dataDigest(block)
        if new_digest != digest:
          raise Exception("Critical error: Bad digest in container!")
    bc = TestingBlockCache()
    self.read_blocks(bc, filename)
  def add_listener(self, listener):
    self._listeners.append(listener)
  def is_requested(self, digest, code):
    for listener in self._listeners:
      if listener.is_requested(digest, code):
        return True
    return False
  def load_blocks(self, listener):
    # Header could be already available without reading the container file,
    # if it was piggybacked in another container that was already read.
    # In such case, it will be supplied in header_file. If the header was
    # not piggybacked, header_file is None.
    # Since it might be unnecessary to load any blocks from the container,
    # we don't touch the body file before we know we need blocks from it.
    logging.debug("Container %d loading blocks", self.index)

    header_file = self.storage.load_header_file(self.sequence_id, self.index)
    body_file = None
    if header_file is None:
      logging.debug("Header file not ready. Reading it from body file")
      body_file = self.storage.load_body_file(self.sequence_id, self.index)
      header_file = body_file
    body_blocks = self._load_header(header_file)
    
    body_needed = False
    for (digest, size, code) in body_blocks:
      if is_user_code(code) and listener.is_requested(digest, code):
        body_needed = True
        logging.debug("Container %d requests block %s:%s. Body is needed" %
            (self.index, base64.b64encode(digest), code_name(code)))
        break

    if not body_needed:
      logging.debug("Container %d does not need to load body" % self.index)
      return
    
    if body_file is None:
      header_size = header_file.tell()
      body_file = self.storage.load_body_file(self.sequence_id, self.index)
      body_file.seek(header_size)

    body_dump_loader = DataDumpLoader(
        body_file, body_blocks,
        password=self.storage.get_encryption_key())
    body_dump_loader.load_blocks(listener)
  def _load_header(self, header_file):
    logging.debug("****************************** loading header")
    magic = header_file.read(len(MAGIC))
    if MAGIC != magic:
      raise Exception("Manent: magic number not found")
    version = Format.read_int(header_file)
    if version != VERSION:
      raise Exception("Container %d has unsupported version" % self.index)
    index = Format.read_int(header_file)
    if index != self.index:
      raise Exception(
        "Manent: wrong container file index. Expected %s, found %s"
        % (str(self.index), str(index)))
    
    header_table_size = Format.read_int(header_file)
    header_table_digest = header_file.read(Digest.dataDigestSize())
    header_table_str = header_file.read(header_table_size)
    if Digest.dataDigest(header_table_str) != header_table_digest:
      raise Exception("Manent: header of container file corrupted")
    header_dump_len = Format.read_int(header_file)
    header_dump_str = header_file.read(header_dump_len)
    
    header_table_io = StringIO.StringIO(header_table_str)
    header_blocks = unserialize_blocks(header_table_io)

    class BlockTableListener:
      def __init__(self):
        self.body_table_str = None
      def is_requested(self, digest, code):
        return code == CODE_BLOCK_TABLE
      def loaded(self, digest, code, data):
        assert code == CODE_BLOCK_TABLE
        self.body_table_str = data

    listener = BlockTableListener()
    header_dump_str_io = StringIO.StringIO(header_dump_str)
    header_dump_loader = DataDumpLoader(header_dump_str_io, header_blocks,
      password=self.storage.get_encryption_key())
    header_dump_loader.load_blocks(listener)

    body_table_io = StringIO.StringIO(listener.body_table_str)
    blocks = unserialize_blocks(body_table_io)
    return blocks

