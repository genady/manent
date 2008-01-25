import base64
import logging
import cStringIO as StringIO

import BlockManager
import Container
import Storage
import utils.Digest as Digest
import utils.IntegerEncodings as IE

PREFIX = "STORAGE_MANAGER."

logger_sm = logging.getLogger("manent.storage_manager")

class StorageManager:
	"""Handles the moving of blocks to and from storages.

	Input: a stream of blocks
	Creates containers and sends them to storage
	
	Data structure:
	block_container_db keeps for each hash the list of sequence_id+container_id
	
	aside_block_db holds the digests for blocks that have been set aside.
	The blocks themselves are supposed to be stored by the block manager.

	The sequences are numbered globally for all the storages, and are identified
	by a random sequence id generated by the storage automatically
	
	seq_to_index keeps for each sequence id its storage idx an global sequence idx
	index_to_seq keeps for each global sequence idx its storage idx and sequence id
	The information is encoded in config_db.
	
	storage idxs are stored in the config_db["storage_idxs"]
	"""
	def __init__(self, db_manager, txn_manager):
		self.db_manager = db_manager
		self.txn_manager = txn_manager
		self.block_manager = BlockManager.BlockManager(self.db_manager,
			self.txn_manager, self)

		self.config_db = db_manager.get_database_btree("config.db", "storage",
			txn_manager)
		self.block_container_db = db_manager.get_database_hash("storage.db",
			"blocks", txn_manager)
		self.aside_block_db = db_manager.get_database_btree("storage-aside.db",
			"blocks", txn_manager)
		self.current_open_container = None
		# Aside container holds the metadata that we strive to upload
		# in large chunks to minimize downloads of complete containers.
		self.current_aside_container = None
		self.num_aside_blocks = 0

		# Mapping of storage sequences to indices and vice versa
		# The storage sequence data consists of storage index and sequence
		# ID string
		# In the config_db we store the persistent copy of the information
		# in the seq_to_index and index_to_seq:
		# repo.%index.seq = sequence
		# repo.%index.storage = storage index
		# repo.next_index = <the next index>
		self.seq_to_index = {}
		self.index_to_seq = {}
		NS_KEY = self._key("next_seq")
		if self.config_db.has_key(NS_KEY):
			self.next_seq_idx = int(self.config_db[NS_KEY])
		else:
			self.next_seq_idx = 0
		SEQ_PREFIX = self._key("SEQ.")
		for key, val in self.config_db.iteritems_prefix(SEQ_PREFIX):
			sequence_id = key[len(SEQ_PREFIX):]
			storage_idx, sequence_idx = IE.binary_decode_int_varlen_list(val)
			self.seq_to_index[sequence_id] = (storage_idx, sequence_idx)
			self.index_to_seq[sequence_idx] = (storage_idx, sequence_id)
	def close(self):
		self.aside_block_db.close()
		self.block_container_db.close()
		self.config_db.close()
		self.block_manager.close()
	def _key(self, suffix):
		return PREFIX + suffix
	def _register_sequence(self, storage_idx, sequence_id):
		# Generate new index for this sequence
		logger_sm.debug("new sequence detected in storage %d: %s" %
			(storage_idx, base64.urlsafe_b64encode(sequence_id)))
		sequence_idx = self.next_seq_idx
		self.next_seq_idx += 1
		self.config_db[self._key("next_seq")] = str(self.next_seq_idx)
		self.config_db[self._key("SEQ."+sequence_id)] = \
			IE.binary_encode_int_varlen_list([storage_idx, sequence_idx])
			
		self.seq_to_index[sequence_id] = (storage_idx, sequence_idx)
		self.index_to_seq[sequence_idx] = (storage_idx, sequence_id)
	def get_sequence_idx(self, storage_idx, sequence_id):
		if not self.seq_to_index.has_key(sequence_id):
			self._register_sequence(storage_idx, sequence_id)
		dummy, sequence_idx = self.seq_to_index[sequence_id]
		return sequence_idx
	class PassThroughBlockHandler:
		def __init__(self, storage_manager, sequence_idx,
					container_idx, pass_block_handler):
			self.storage_manager = storage_manager
			self.sequence_idx = sequence_idx
			self.container_idx = container_idx
			self.pass_block_handler = pass_block_handler
		def is_requested(self, digest, code):
			encoded = self.storage_manager._encode_block_info(self.sequence_idx,
				self.container_idx)
			self.storage_manager.block_container_db[digest] = encoded

			if self.pass_block_handler is not None:
				return self.pass_block_handler.is_requested(digest, code)
			return False
		def loaded(self, digest, code, data):
			if self.pass_block_handler is not None:
				self.pass_block_handler.loaded(digest, code, data)
	class NewContainerHandler:
		def __init__(self, storage_manager, block_handler):
			self.storage_manager = storage_manager
			self.block_handler = block_handler
			self.new_containers = []
		def report_new_container(self, container):
			self.new_containers.append(container)
		def process_new_containers(self):
			for container in self.new_containers:
				sequence_id = container.get_sequence_id()
				storage_idx = container.get_storage().get_index()
				sequence_idx =\
					self.storage_manager.get_sequence_idx(storage_idx, sequence_id)
				block_handler = StorageManager.PassThroughBlockHandler(
					self.storage_manager, sequence_idx, container.get_index(),
					self.block_handler)
				container.load_header()
				container.load_blocks(block_handler)
	def add_storage(self, storage_params, new_block_handler):
		# When we add a storage, the following algorithm is executed:
		# 1. If the storage is already in the shared db, it is just added
		# 2. If the storage is not in the shared db, the storage location
		#    is rescanned. All storage locations found there are added as
		#    base storages, and a new one is created.
		storage_idxs = self.get_storage_idxs()
		if storage_idxs == []:
			storage_idx = 0
		else:
			storage_idx = max(storage_idxs) + 1
		self.write_storage_idxs(self.get_storage_idxs() + [storage_idx])

		handler = StorageManager.NewContainerHandler(self, new_block_handler)
		storage = Storage.create_storage(self.db_manager, self.txn_manager,
			storage_idx, storage_params, handler)
		self.storages[storage_idx] = storage
		handler.process_new_containers()
		return storage_idx
	def load_storages(self, new_block_handler):
		#
		# All storages except for the specified one are inactive, i.e., base.
		# Inactive storages can be used to pull data blocks from, and must
		# be updated on each invocation, since somebody else might be adding
		# blocks there
		#
		self.storages = {}
		self.active_storage_idx = None
		handler = StorageManager.NewContainerHandler(self, new_block_handler)
		for storage_idx in self.get_storage_idxs():
			storage = Storage.load_storage(self.db_manager, self.txn_manager,
				storage_idx, handler)
			self.storages[storage_idx] = storage
			if storage.is_active():
				seq_id = storage.get_active_sequence_id()
				self.active_storage_idx, seq_idx = self.seq_to_index[seq_id]
		# Load the aside blocks
		# We first read them into memory, then we clean the database.
		# The blocks will be re-inserted into the database when we do
		# add_block on them.
		aside_blocks = []
		# Read the blocks in the same order they have been inserted
		for key, digest in self.aside_block_db.iteritems():
			assert self.block_manager.has_block(digest)
			code = self.block_manager.get_block_code(digest)
			data = self.block_manager.load_block(digest)
			aside_blocks.append((digest, code, data))
		self.aside_block_db.truncate()
		for digest, code, data in aside_blocks:
			print "restarting aside block", base64.b64encode(digest), code, len(data)
			self.add_block(digest, code, data)
	def get_storage_idxs(self):
		KEY = self._key("storage_idxs")
		if not self.config_db.has_key(KEY):
			return []
		idxs_str = self.config_db[KEY]
		storage_idxs = IE.binary_decode_int_varlen_list(idxs_str)
		return storage_idxs
	def write_storage_idxs(self, storage_idxs):
		idxs_str = IE.binary_encode_int_varlen_list(storage_idxs)
		self.config_db[self._key("storage_idxs")] = idxs_str
	def get_storage_config(self, storage_index):
		return self.storages[storage_index].get_config()
	def make_active_storage(self, storage_index):
		if self.active_storage_idx is not None:
			raise Exception("Switching active storage not supported yet")
		storage = self.storages[storage_index]
		storage.make_active()
		seq_id = storage.get_active_sequence_id()
		self._register_sequence(storage_index, seq_id)
		self.active_storage_idx = storage_index
	def get_active_sequence_id(self):
		storage = self.storages[self.active_storage_idx]
		return storage.get_active_sequence_id()
	def get_active_storage_index(self):
		return self.active_storage_idx
	def get_block_size(self):
		storage = self.storages[self.active_storage_idx]
		return storage.get_block_size()
	def add_block(self, digest, code, data):
		self.block_manager.add_block(digest, code, data)

		if self.block_container_db.has_key(digest):
			return

		storage = self.storages[self.active_storage_idx]
		#
		# Make sure we have a container that can take this block
		#
		if code == Container.CODE_DATA:
			#print "Block", base64.b64encode(digest),\
				#Container.code_name(code), "sent to normal container"
			# Put the data to a normal container
			if self.current_open_container is None:
				self.current_open_container = storage.create_container()
			elif not self.current_open_container.can_add(data):
				self._write_container(self.current_open_container)
				self.current_open_container = storage.create_container()
			# add the block to the container
			self.current_open_container.add_block(digest, code, data)
		else:
			#print "Block", base64.b64encode(digest),\
				#Container.code_name(code), "sent to aside container"
			# Put the data into an aside container
			if self.current_aside_container is None:
				self.current_aside_container = storage.create_aside_container()
			elif not self.current_aside_container.can_add(data):
				storage.import_aside_container(self.current_aside_container)
				self._write_container(self.current_aside_container)
				self.aside_block_db.truncate()
				# TODO: we store all the data. Make the aside container more stupid!
				self.current_aside_container = storage.create_container()
			self.current_aside_container.add_block(digest, code, data)
			# Add the block to aside_block table to make sure it will be
			# visible even if we crash without saving the aside container
			key = IE.binary_encode_int_varlen(self.num_aside_blocks)
			self.num_aside_blocks += 1
			self.aside_block_db[key] = digest
	def load_block(self, digest):
		#print "SM loading block", base64.b64encode(digest)
		if not self.block_manager.has_block(digest):
			#print "calling load blocks for", base64.b64encode(digest)
			self.load_blocks_for(digest, self.block_manager.get_block_handler())
		return self.block_manager.load_block(digest)
	def request_block(self, digest):
		self.block_manager.request_block(digest)
	def get_block_code(self, digest):
		#print "SM getting code for block", base64.b64encode(digest)
		return self.block_manager.get_block_code(digest)
	def load_blocks_for(self, digest, handler):
		#print "Loading blocks for", base64.b64encode(digest)
		sequence_idx, container_idx = self._decode_block_info(
			self.block_container_db[digest])
		storage_idx, sequence_id = self.index_to_seq[sequence_idx]
		storage = self.storages[storage_idx]

		container = storage.get_container(sequence_id, container_idx)
		print "Digest", base64.b64encode(digest), "is in", sequence_idx, container_idx
		container.load_header()
		container.load_blocks(handler)
	def flush(self):
		storage = self.storages[self.active_storage_idx]

		if self.current_aside_container is not None:
			print "Exporting aside container to output stream"
			class Handler:
				def __init__(self, storage_manager, storage):
					self.sm = storage_manager
					self.storage = storage
				def is_requested(self, digest, code):
					return True
				def loaded(self, digest, code, data):
					# Make sure current container can accept the block
					print "Exporting block", base64.b64encode(digest),\
						Container.code_name(code)
					if self.sm.current_open_container is None:
						self.sm.current_open_container = self.storage.create_container()
					elif not self.sm.current_open_container.can_add(data):
						self.sm._write_container(self.sm.current_open_container)
						self.sm.current_open_container = self.storage.create_container()
					# add the block to the container
					self.sm.current_open_container.add_block(digest, code, data)
			self.current_aside_container.finish_dump()
			self.current_aside_container = None
			self.aside_block_db.truncate()
			aside_load_container = storage.get_aside_container()
			aside_load_container.load_header()
			aside_load_container.load_blocks(Handler(self, storage))
		if self.current_open_container is not None:
			# TODO: what if the container is empty???
			self._write_container(self.current_open_container)
			self.current_open_container = None
	def _write_container(self, container):
		print "Finalizing container", container.get_index()
		container.finish_dump()
		container.upload()
		#
		# Now we have container idx, update it in the blocks db
		#
		container_idx = container.get_index()
		storage_idx, seq_idx = self.seq_to_index[container.get_sequence_id()]
		encoded = self._encode_block_info(seq_idx, container_idx)
		print "Encoding block info seq=",seq_idx, "container=", container_idx
		for digest, code in container.list_blocks():
			print "   ", base64.b64encode(digest)
			self.block_container_db[digest] = encoded
		self.txn_manager.commit()
	#--------------------------------------------------------
	# Utility methods
	#--------------------------------------------------------
	def _encode_block_info(self, seq_idx, container_idx):
		io = StringIO.StringIO()
		io.write(IE.binary_encode_int_varlen(seq_idx))
		io.write(IE.binary_encode_int_varlen(container_idx))
		return io.getvalue()
	def _decode_block_info(self, encoded):
		io = StringIO.StringIO(encoded)
		seq_idx = IE.binary_read_int_varlen(io)
		container_idx = IE.binary_read_int_varlen(io)
		return (seq_idx, container_idx)
