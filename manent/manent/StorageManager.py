import base64
import cStringIO as StringIO

import Container
import Storage
import utils.IntegerEncodings as IE

PREFIX = "STORAGE_MANAGER."

class StorageManager:
	"""Handles the moving of blocks to and from storages.

	Input: a stream of blocks
	Creates containers and sends them to storage
	
	Data structure:
	block_container_db keeps for each hash the list of sequence_id+container_id
	
	The sequences are numbered globally for all the storages, and are identified
	by a random sequence id generated by the storage automatically
	
	seq_to_index keeps for each sequence id its storage idx an global sequence idx
	index_to_seq keeps for each global sequence idx its storage idx and sequence id
	The information is encoded in config_db.
	
	storage idxs are stored in the config_db["storage_idxs"]
	"""
	def __init__(self, config_db, block_container_db):
		self.config_db = config_db
		self.block_container_db = block_container_db
		self.current_open_container = None
		
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
	def _key(self, suffix):
		return PREFIX + suffix
	def register_container(self):
		# TODO: implement this
		self.fail()
	def register_sequence(self, storage_idx, sequence_id):
		# Generate new index for this sequence
		sequence_idx = self.next_seq_idx
		self.next_seq_idx += 1
		self.config_db[self._key("next_seq")] = str(self.next_seq_idx)
		self.config_db[self._key("SEQ."+sequence_id)] = \
			IE.binary_encode_int_varlen_list([storage_idx, sequence_idx])
			
		self.seq_to_index[sequence_id] = (storage_idx, sequence_idx)
		self.index_to_seq[sequence_idx] = (storage_idx, sequence_id)
	def get_sequence_idx(self, storage_idx, sequence_id):
		if not self.seq_to_index.has_key(sequence_id):
			self.register_sequence(storage_idx, sequence_id)
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
			print "container has block", base64.b64encode(digest), code
			# TODO: register the block with the storage manager
			encoded = self.storage_manager.encode_block_info(self.sequence_idx,
				self.container_idx)
			self.storage_manager.block_container_db[digest] = encoded

			if self.pass_block_handler is not None:
				return self.pass_block_handler.is_requested(digest, code)
			return False
		def loaded(self, digest, code, data):
			print "container loads block", base64.b64encode(digest), code
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
				print "New container reported", storage_idx,  sequence_idx, sequence_id
				block_handler = StorageManager.PassThroughBlockHandler(
					self.storage_manager, sequence_idx, container.get_index(),
					self.block_handler)
				container.load_header()
				container.load_body()
				print "Loading container blocks"
				container.load_blocks(block_handler)
	def add_storage(self, storage_type, storage_params, new_block_handler):
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
		storage = Storage.create_storage(self.config_db, storage_type,
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
			storage = Storage.load_storage(self.config_db, storage_idx,
				handler)
			self.storages[storage_idx] = storage
			if storage.is_active():
				seq_id = storage.get_active_sequence_id()
				self.active_storage_idx, seq_idx = self.seq_to_index[seq_id]
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
		self.register_sequence(storage_index, seq_id)
		self.active_storage_idx = storage_index
	def get_active_sequence_id(self):
		storage = self.storages[self.active_storage_idx]
		return storage.get_active_sequence_id()
	def load(self):
		for storage_index in range(int(self.config_db[self._key("next_storage")])):
			storage_type = self.config_db[self._key("storage.%d.type"%storage_index)]
	def close(self):
		self.block_container_db.close()
	def add_block(self, digest, code, data):
		storage = self.storages[self.active_storage_idx]
		#
		# Make sure we have a container that can take this block
		#
		if self.current_open_container is None:
			self.current_open_container = storage.create_container()
		elif not self.current_open_container.can_add_block(digest, code, data):
			self.write_container(self.current_open_container)
			self.current_open_container = storage.create_container()
		#
		# add the block to the container
		#
		self.current_open_container.add_block(digest, code, data)
	def flush(self):
		storage = self.storages[self.active_storage_idx]

		if self.current_open_container is not None:
			self.write_container(self.current_open_container)
			self.current_open_container = None
	def write_container(self, container):
		container.finish_dump()
		#
		# Now we have container idx, update it in the blocks db
		#
		container_idx = container.get_index()
		storage_idx, seq_idx = self.seq_to_index[container.get_sequence_id()]
		encoded = self.encode_block_info(seq_idx, container_idx)
		for digest, code in container.list_blocks():
			self.block_container_db[digest] = encoded
	def load_block(self, digest, handler):
		sequence_idx, container_idx = self.decode_block_info(self.block_container_db[digest])
		storage_idx, sequence_id = self.index_to_seq[sequence_idx]
		storage = self.storages[storage_idx]
		container = storage.get_container(sequence_id, container_idx)
		container.load_header()
		container.load_body()
		container.load_blocks(handler)
	def get_block_storage(self, digest):
		seq_idx, container_idx = self.decode_block_info(self.block_container_db[digest])
		storage_idx, seq_id = self.index_to_seq[seq_idx]
		return storage_idx
	def rescan_storage(self, handler):
		# TODO: this should proceed in a separate thread
		# actually, each storage could be processed in its own thread
		class Handler:
			def __init__(self, handler, storage_idx, sequence_idx):
				self.handler = handler
				self.storage_idx = storage_idx
				self.sequence_idx = sequence_idx
			def loaded(self, digest, data, code):
				self.handler.loaded(self.storage_idx, self.sequence_idx,
					digest, data, code)
		for storage in self.storages():
			# This is not active storage. Somebody else might be updating it,
			# so rescan
			new_containers = storage.rescan()
			for sequence_id, container_idx in new_containers:
				sequence_idx = self.get_sequence_idx(storage_idx, sequence_id)
				container = storage.get_container(container_idx)
				#
				# Register blocks of the container in the block_container_db
				#
				container.load_header()
				has_nondata_blocks = False
				has_data_blocks = False
				encoded = self.encode_block_info(storage_idx, container_idx)
				for digest, size, code in container.list_blocks():
					self.block_container_db[digest] = encoded
					if code != Container.CODE_DATA:
						has_nondata_blocks = True
					else:
						has_data_blocks = True
				#
				# Notify the caller of the nondata blocks, which are supposed
				# to be cached
				#
				if has_nondata_blocks:
					container.load_body()
					container.load_blocks(handler)
				if not has_data_blocks:
					container.remove_files()
	#--------------------------------------------------------
	# Utility methods
	#--------------------------------------------------------
	def encode_block_info(self, seq_idx, container_idx):
		io = StringIO.StringIO()
		io.write(IE.binary_encode_int_varlen(seq_idx))
		io.write(IE.binary_encode_int_varlen(container_idx))
		return io.getvalue()
	def decode_block_info(self, encoded):
		io = StringIO.StringIO(encoded)
		seq_idx = IE.binary_read_int_varlen(io)
		container_idx = IE.binary_read_int_varlen(io)
		return (seq_idx, container_idx)
