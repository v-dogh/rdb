#include <rdb_memory.hpp>
#include <rdb_locale.hpp>
#include <rdb_dbg.hpp>
#include <rdb_memunits.hpp>
#include <rdb_reflect.hpp>
#include <rdb_version.hpp>
#include <cmath>

namespace rdb
{
	void MemoryCache::_move(MemoryCache&& copy) noexcept
	{
		while (copy._flush_running.load() != 0)
			copy._flush_running.wait(copy._flush_running.load());
		_disk_logs = std::move(copy._disk_logs);
		_path = std::move(copy._path);
		_map = std::move(copy._map);
		_locks = std::move(copy._locks);
		_readonly_maps = std::move(copy._readonly_maps);
		_handle_cache = std::move(copy._handle_cache);
		_handle_cache_tracker = std::move(copy._handle_cache_tracker);
		_flush_thread = std::move(copy._flush_thread);
		_mappings = copy._mappings;
		_descriptors = copy._descriptors;
		_flush_id = copy._flush_id.load();
		_shared = copy._shared;
		_id = copy._id;
		_pressure = copy._pressure;
		_schema = copy._schema;
		_lock_cnt = copy._lock_cnt;
	}
	void MemoryCache::_push_bytes(int bytes) noexcept
	{
		_pressure += bytes;
	}

	MemoryCache::MemoryCache(Shared shared, std::size_t core, schema_type schema) :
		_shared(shared),
		_map(std::make_shared<write_store>()),
		_path(
			shared.cfg->root/
			std::format("vcpu{}", core)/
			std::format("[{}]", uuid::encode(schema, uuid::table_alnum))
		),
		_id(core),
		_disk_logs(shared, _path/"logs", schema),
		_schema(schema)
	{
		_handle_cache.reserve(164);
		_handle_cache_tracker.reserve(164);
		if (!std::filesystem::exists(_path))
		{
			RDB_LOG(mem, "vcpu", _id, "Generating memory cache")
			std::filesystem::create_directories(_path);
			std::filesystem::create_directory(_path/"flush");
			std::filesystem::create_directory(_path/"logs");
		}
		else
		{
			RDB_LOG(mem, "vcpu", _id, "Replaying memory cache")
			std::vector<std::filesystem::path> corrupted;
			for (decltype(auto) it : std::filesystem::directory_iterator(_path/"flush"))
			{
				if (std::filesystem::exists(it.path()/"lock"))
				{
					RDB_LOG(mem, "vcpu", _id, "MCR DETECTED CORRUPTED FLUSH{}", _id,
						std::stoul(it.path().filename().string().substr(1)))
					corrupted.push_back(it.path());
				}
				else
				{
					_handle_reserve(true);

					const auto id = std::stoul(it.path().filename().string().substr(1));
					if (const auto p = _path/"logs"/std::format("snapshot{}", id);
						std::filesystem::exists(p))
						std::filesystem::remove_all(p);

					_flush_id = std::max(
						id + 1,
						_flush_id.load()
					);
				}
			}
			for (decltype(auto) it : corrupted)
			{
				std::filesystem::remove_all(it);
			}

			_disk_logs.replay([this](WriteType type, key_type key, View sort, View data) {
				if (type == WriteType::CreatePartition)
				{
					RDB_TRACE(mem, "vcpu", _id, "Replay - create partition <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_create_partition_if(*_map, key, data);
				}
				else if (type == WriteType::Reset)
				{
					RDB_TRACE(mem, "vcpu", _id, "Replay - reset <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_reset_impl(_find_partition(*_map, key), sort);
					_flush_if();
				}
				else if (type == WriteType::Remov)
				{
					RDB_TRACE(mem, "vcpu", _id, "Replay - remove <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_remove_impl(_find_partition(*_map, key), sort);
					_flush_if();
				}
				else
				{
					RDB_TRACE(mem, "vcpu", _id, "Replay - write <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_write_impl(_find_partition(*_map, key), type, sort, data);
					_flush_if();
				}
			});
		}
	}
	MemoryCache::~MemoryCache()
	{
		_shutdown = true;
		_flush_tasks.enqueue(nullptr, 0);
	}

	RuntimeSchemaReflection::RTSI& MemoryCache::_info() const noexcept
	{
		[[ unlikely ]] if (_schema_info == nullptr || RuntimeSchemaReflection::stale(_schema_version))
		{
			const auto [ version, info ] = RuntimeSchemaReflection::version(_schema);
			_schema_version = version;
			_schema_info = info;
		}
		return *_schema_info;
	}
	std::size_t MemoryCache::core() const noexcept
	{
		return _id;
	}
	std::size_t MemoryCache::pressure() const noexcept
	{
		return _pressure;
	}
	std::size_t MemoryCache::descriptors() const noexcept
	{
		return _descriptors;
	}

	MemoryCache::FlushHandle& MemoryCache::_handle_open(std::size_t flush) const noexcept
	{
		constexpr auto map_cost = 3;
		constexpr auto descriptor_cost = 3;

		auto& handle = _handle_cache[flush];

		if (handle.idx)
		{
			std::swap(
				_handle_cache_tracker[handle.idx],
				_handle_cache_tracker[handle.idx - 1]
			);
			handle.idx--;
		}

		if (_descriptors + descriptor_cost >= _shared.cfg->cache.max_descriptors ||
			_mappings + map_cost >= _shared.cfg->cache.max_mappings)
		{
			RDB_WARN("vcpu", _id, "File resource limit")

			for (std::size_t i = 0; i < _handle_cache_tracker.size(); i++)
			{
				const auto c = _handle_cache_tracker[_handle_cache_tracker.size() - i - 1];
				if (_handle_cache[c].data.is_opened())
				{
					_handle_close(c);
					break;
				}
			}
		}

		const auto is_opened = handle.data.is_opened();
		const auto is_mapped = handle.data.is_mapped();
		if (!is_opened)
		{
			const auto path = _path/"flush"/std::format("f{}", flush);

			handle.data.open(
				path/"data.dat",
				Mapper::OpenMode::Read
			);
			handle.indexer.open(
				path/"indexer.idx",
				Mapper::OpenMode::Read
			);
			handle.bloom.open(
				path/"filter.blx",
				Mapper::OpenMode::Read
			);

			handle.data.map();
			handle.indexer.map();
			handle.bloom.map();

			// handle.data.hint(Mapper::Access::Hot);
			handle.indexer.hint(Mapper::Access::Sequential);
			handle.indexer.hint(Mapper::Access::Hot);

			_mappings += map_cost;
			_descriptors += descriptor_cost;
		}
		else if (!is_mapped)
		{
			handle.data.map();
			handle.indexer.map();
			handle.bloom.map();

			_mappings += map_cost;
		}
		return handle;
	}
	void MemoryCache::_handle_reserve(bool ready) const noexcept
	{
		_handle_cache_tracker.push_back(_handle_cache.size());
		_handle_cache.emplace_back(_handle_cache_tracker.size() - 1, ready);
	}
	void MemoryCache::_handle_close_soft(std::size_t flush) const noexcept
	{
		auto& handle = _handle_cache[flush];
		if (handle.data.is_mapped())
		{
			handle.data.hint(Mapper::Access::Cold);
			// handle.indexer.hint(Mapper::Access::Cold);

			handle.data.unmap();
			handle.indexer.unmap();
			handle.bloom.unmap();
			_mappings -= 3;
		}
	}
	void MemoryCache::_handle_close(std::size_t flush) const noexcept
	{
		auto& handle = _handle_cache[flush];
		if (handle.data.is_opened())
		{
			const auto was_mapped = handle.data.is_mapped();
			handle.data.close();
			handle.indexer.close();
			handle.bloom.close();
			_mappings -= 3 * was_mapped;
			_descriptors -= 3;
		}
	}

	std::size_t MemoryCache::_read_entry_size_impl(const View& view, DataType type) noexcept
	{
		auto& info = _info();
		if (type == DataType::FieldSequence)
		{
			std::size_t off = 0;
			auto cnt = view.data()[off++];
			while (cnt--)
			{
				RuntimeInterfaceReflection::RTII& field =
					info.reflect(view.data()[off++]);
				off += field.storage(view.data().data() + off);
			}
			return off;
		}
		else if (type == DataType::SchemaInstance)
		{
			return info.storage(view.data().data());
		}
		else
			return 0;
	}
	std::size_t MemoryCache::_read_entry_impl(const View& view, DataType type, field_bitmap& fields, const read_callback& callback) noexcept
	{
		auto& info = _info();
		std::size_t cnt = 0;
		if (type == DataType::FieldSequence)
		{
			std::size_t off = 0;
			do
			{
				const auto field = view.data()[off++];
				RuntimeInterfaceReflection::RTII& finf =
					info.reflect(field);
				const auto size = finf.storage(view.data().data() + off);

				if (callback)
				{
					if (fields.test(field))
					{
						fields.reset(field);
						cnt++;
						callback(field, View::view(view.data().subspan(off, size)));
					}
				}
				else
				{
					return 1;
				}
				off += size;
			} while (off < view.size());
		}
		else if (type == DataType::SchemaInstance)
		{
			std::size_t off = 0;
			std::size_t idx = 0;
			while (off < view.size())
			{
				RuntimeInterfaceReflection::RTII& finf =
					info.reflect(idx);
				const auto size = finf.storage(view.data().data() + off);

				if (callback)
				{
					if (fields.test(idx))
					{
						fields.reset(idx);
						cnt++;
						callback(idx, View::view(view.data().subspan(off, size)));
					}
				}
				else
				{
					return 1;
				}
				off += size;
				idx++;
			}
		}
		return cnt;
	}
	std::size_t MemoryCache::_read_cache_impl(write_store& map, key_type key, const View& sort, field_bitmap& fields, const read_callback& callback) noexcept
	{
		auto fp = map.find(key);
		if (fp == map.end())
			return 0;
		auto f = _find_slot(fp, sort);
		if (f == nullptr)
			return 0;
		return _read_entry_impl(View::view(f->buffer()), f->vtype, fields, callback);
	}	

	std::optional<std::size_t> MemoryCache::_disk_find_partition(key_type key, FlushHandle& handle) noexcept
	{
		auto& indexer = handle.indexer;

		std::size_t off = 0;
		const auto max_key = byte::sread<key_type>(indexer.memory(), off);
		const auto size = byte::sread<std::uint32_t>(indexer.memory(), off);

		if (key > max_key)
			return std::nullopt;

		if (const auto result = byte::search_partition<key_type, std::uint64_t>(
				key, indexer.memory().subspan(off), size
			); result.has_value())
		{
			return result.value();
		}
		return std::nullopt;
	}
	std::pair<std::size_t, MemoryCache::PartitionMetadata> MemoryCache::_disk_read_partition_metadata(FlushHandle& handle) noexcept
	{
		std::size_t off = 0;
		PartitionMetadata metadata;
		auto& data = handle.data;
		metadata.version = byte::sread<std::uint64_t>(data.memory(), off);
		metadata.partition_sparse_index =  byte::sread<std::uint64_t>(data.memory(), off);
		metadata.intra_partition_sparse_index =  byte::sread<std::uint64_t>(data.memory(), off);
		metadata.block_size = byte::sread<std::uint64_t>(data.memory(), off);
		return { off, metadata };
	}

	bool MemoryCache::_read_impl(key_type key, const View& sort, field_bitmap fields, const read_callback& callback) noexcept
	{
		RDB_LOG(mem, "vcpu", _id, "MC READ <{}>", _id, uuid::encode(key, uuid::table_alnum))

		// 1. Search cache
		// 2. If not found -> search disk (from newest to oldest)
		//
		// If requires accumulation -> get an accumulation handle and accumulate until tail or result
		// Accumulation applies even if the data is already in cache

		auto& info =
			_info();
		const auto dynamic = !info.static_prefix();
		const auto required = callback ? fields.count() : 1;
		const auto flush_running = _flush_running.load();

		// Search cache
		std::size_t found = 0;
		{
			if ((found += _read_cache_impl(
					*_map, key, View::view(sort), fields, callback
				)) == required)
			{
				return true;
			}
			else if (flush_running)
			{
				RDB_TRACE(mem, "vcpu", _id, "MC READ SCANNING RMPS")
				for (auto it = _readonly_maps.rbegin(); it != _readonly_maps.rend(); ++it)
				{
					if (const auto lock = it->lock(); lock != nullptr)
					{
						if ((found += _read_cache_impl(
								*lock, key, View::view(sort), fields, callback
							)) == required)
						{
							return true;
						}
					}
				}
			}
			else
				_readonly_maps.clear();
		}
		// Search disk
		{
			RDB_TRACE(mem, "vcpu", _id, "MC READ CACHE MISS")
			for (std::size_t j = _flush_id - flush_running; j > 0; j--)
			{
				RDB_TRACE(mem, "vcpu", _id, "MC READ SEARCHING FLUSH{}", _id, j - 1)

				const auto i = j - 1;

				auto& handle = _handle_open(i);
				auto& [ data, indexer, bloom, _1, _2 ] = handle;

				if (handle.ready() && _bloom_may_contain(key, handle))
				{
					// Search the indexer
					const auto partition_offset = _disk_find_partition(key, handle);
					if (!partition_offset.has_value())
						continue;
					const auto offset = partition_offset.value();
					auto off = partition_offset.value();

					// Search data

					// Wide partition
					if (info.skeys())
					{
						RDB_TRACE(mem, "vcpu", _id, "MC READ SEARCHING IN PARTITION")

						const auto partition_size = byte::sread<std::uint64_t>(data.memory(), off);

						std::size_t partition_footer_off = off + partition_size;

						const auto sparse_block_indices = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);
						const auto sort_bloom_offset = byte::sread<std::uint64_t>(data.memory(), partition_footer_off);

						if (!_bloom_may_contain(uuid::xxhash(sort), sort_bloom_offset, handle))
						{
							RDB_TRACE(mem, "vcpu", _id, "MC READ INTRA-PARTITION BLOOM DISCARD")
							continue;
						}

						const auto index = data.memory().subspan(partition_footer_off);
						const auto sparse_block_offset = sparse_block_indices ?
							(dynamic ?
								byte::search_partition_binary_indirect<std::uint64_t, std::uint64_t, std::uint16_t>(
									sort, index, data.memory(), sparse_block_indices, true, true
								) :
								byte::search_partition_binary<std::uint64_t>(
									sort, index, sparse_block_indices, true, true
								)
							) : std::optional<std::uint64_t>(off);

						// Linar search across blocks
						if (sparse_block_offset.has_value())
						{
							data.hint(Mapper::Access::Sequential);

							off = sparse_block_offset.value();

							RDB_TRACE(mem, "vcpu", _id, "MC READ SEARCHING IN BLOCK SEQUENCE")
							while (off < offset + partition_size)
							{
								const auto prefix = info.static_prefix();

								/*const auto checksum = */byte::sread<std::uint64_t>(data.memory(), off);
								const auto index_count = byte::sread<std::uint32_t>(data.memory(), off);

								View min_key;
								View max_key;

								auto saved_off = off;
								if (dynamic)
								{
									const auto keyspace_size = byte::sread<std::uint32_t>(data.memory(), off);
									const auto keyspace_last = byte::sread<std::uint32_t>(data.memory(), off);

									std::size_t min_off = off;
									std::size_t max_off = off + keyspace_last;

									const auto len_min = byte::sread<std::uint16_t>(data.memory(), min_off);
									const auto len_max = byte::sread<std::uint16_t>(data.memory(), max_off);

									min_key = View::view(data.memory().subspan(min_off, len_min));
									max_key = View::view(data.memory().subspan(max_off, len_max));

									saved_off += sizeof(std::uint32_t) * 2;
									off += keyspace_size + (index_count * (sizeof(std::uint32_t) * 2));
								}
								else
								{
									min_key = View::view(data.memory().subspan(off, prefix));
									max_key = View::view(data.memory().subspan(off + (index_count - 1) * (prefix + sizeof(std::uint32_t)), prefix));
									off += index_count * (prefix + sizeof(std::uint32_t));
								}

								const auto decompressed = byte::sread<std::uint32_t>(data.memory(), off);
								const auto compressed = byte::sread<std::uint32_t>(data.memory(), off);

								const auto result_min = byte::binary_compare(sort, min_key);
								const auto result_max = byte::binary_compare(sort, max_key);

								if ((result_min <= 0 && result_max >= 0) ||
									(result_max <= 0 && result_min >= 0))
								{
									const auto ascending = (result_min <= 0 && result_max >= 0);

									RDB_TRACE(mem, "vcpu", _id, "MC READ FOUND MATCHING BLOCK")

									const auto sparse_offset = dynamic ?
										byte::search_partition_binary_indirect<std::uint32_t, std::uint32_t, std::uint16_t>(
											sort, index, data.memory().subspan(saved_off), index_count, true
										) :
										byte::search_partition_binary<std::uint32_t>(
											sort.data(), data.memory().subspan(saved_off), prefix, index_count, ascending, true
										);

									if (sparse_offset.has_value())
									{
										StaticBufferSink sink = (decompressed == compressed) ?
											StaticBufferSink() : StaticBufferSink(decompressed);
										SourceView source(data.memory().subspan(off, compressed));
										std::span<const unsigned char> block;

										if (decompressed != compressed)
										{
											snappy::Uncompress(&source, &sink);
											block = sink.data();
										}
										else
										{
											block = data.memory().subspan(off, decompressed);
										}

										RDB_TRACE(mem, "vcpu", _id, "MC READ LINEAR BLOCK SEARCH")

										off = sparse_offset.value() + info.partition_size(block.data());
										do
										{
											const auto type = DataType(block[off++]);
											const auto instance = View::view(block.subspan(off));
											bool eq = false;
											if (type == DataType::SchemaInstance)
											{
												const auto len = info.prefix_length(block.data() + off);
												View prefix = View::copy(len);
												info.prefix(block.data() + off, View::view(prefix));
												eq = byte::binary_equal(sort, prefix.data());
											}
											else if (type == DataType::Tombstone)
											{
												RDB_TRACE(mem, "vcpu", _id, "MC READ VALUE REMOVED")
												return false;
											}
											else
											{
												const auto len = byte::sread<std::uint16_t>(block, off);
												eq = byte::binary_equal(sort, block.subspan(off, len));
												off += len;
											}

											if (eq)
											{
												RDB_TRACE(mem, "vcpu", _id, "MC READ FOUND VALUE")
												if ((found += _read_entry_impl(instance, type, fields, callback)) == required)
												{
													return true;
												}
												else
												{
													RDB_TRACE(mem, "vcpu", _id, "MC CONTINUE SEARCH")
													break;
												}
											}
											else
											{
												off += _read_entry_size_impl(
													View::view(block.subspan(off)), type
												);
											}
										} while (off < block.size());
									}
									else
									{
										RDB_TRACE(mem, "vcpu", _id, "MC READ BLOOM MISS")
									}
								}
								else
									off += compressed;
							}
						}
						else
						{
							RDB_TRACE(mem, "vcpu", _id, "MC READ BLOOM SORT MISS")
						}
					}
					// Unary partition
					else
					{
						// Search block
						// Search the sparse index table
						// Linear search the partition

						// Block header

						/*const auto checksum = */byte::sread<std::uint64_t>(data.memory(), off);
						const auto index_count = byte::sread<std::uint32_t>(data.memory(), off);

						// Search in block

						RDB_TRACE(mem, "vcpu", _id, "MC READ SEARCHING IN BLOCK")

						const auto sparse_offset = byte::search_partition<key_type, std::uint64_t>(
							key, data.memory().subspan(off), index_count, true
						);

						// Continue block hehader

						off += index_count * (sizeof(key_type) + sizeof(std::uint64_t));

						const auto decompressed = byte::sread<std::uint32_t>(data.memory(), off);
						const auto compressed = byte::sread<std::uint32_t>(data.memory(), off);

						if (sparse_offset.has_value())
						{
							data.hint(Mapper::Access::Sequential);

							StaticBufferSink sink = (decompressed == compressed) ?
								StaticBufferSink() : StaticBufferSink(decompressed);
							SourceView source(data.memory().subspan(off, compressed));
							std::span<const unsigned char> block;

							if (decompressed != compressed)
							{
								snappy::Uncompress(&source, &sink);
								block = sink.data();
							}
							else
							{
								block = data.memory().subspan(off, decompressed);
							}

							RDB_TRACE(mem, "vcpu", _id, "MC READ LINEAR BLOCK SEARCH")

							off = sparse_offset.value();
							do
							{
								if (byte::sread<key_type>(block, off) == key)
								{
									RDB_TRACE(mem, "vcpu", _id, "MC READ FOUND VALUE")

									const auto type = DataType(block[off++]);
									const auto instance = View::view(block.subspan(off));
									if (type == DataType::Tombstone)
										return false;

									if ((found += _read_entry_impl(instance, type, fields, callback)) == required)
									{
										return true;
									}
									else
									{
										RDB_TRACE(mem, "vcpu", _id, "MC CONTINUE SEARCH")
										break;
									}
								}
								else
								{
									const auto type = DataType(block[off++]);
									const auto instance = View::view(block.subspan(off));
									off += _read_entry_size_impl(instance, type);
									off += info.partition_size(block.data() + off);
								}
							} while (off < block.size());
						}
						else
						{
							RDB_TRACE(mem, "vcpu", _id, "MC READ BLOOM MISS")
						}
					}
				}
			}
		}

		return false;
	}

	std::tuple<std::size_t, View, View> MemoryCache::_page_map(write_store::iterator map, key_type key, const View& sort, std::size_t count) noexcept
	{
		std::tuple<std::size_t, View, View> ret;
		auto& [ cnt, result, last ] = ret;

		const auto& part = std::get<partition>(map->second.second);
		const auto saved_count = count;
		std::size_t size = 0;

		// Reserve required space up front (we also get to know which key is last)
		auto scan = [&](partition::const_key, partition::const_pointer value) {
			size += value->size;
			cnt++;
			if (--count == 0)
				return false;
			return true;
		};
		if (sort == nullptr) part.foreach(scan);
		else part.foreach(sort, scan);

		result = View::copy(size);

		std::size_t off = 0;
		std::size_t last_cnt_proper = 0;
		count = saved_count;

		// Copy the data into our buffer and get the actual last key
		auto accumulate = [&](partition::const_key key, partition::const_pointer value) {
			std::memcpy(
				result.mutate().data() + off,
				value->buffer().data(),
				value->size
			);
			off += value->size;
			if (--count == 0)
				return false;
			if (++last_cnt_proper == cnt)
				last = View::view(key);
			return true;
		};
		if (sort == nullptr) part.foreach(accumulate);
		else part.foreach(sort, accumulate);

		return ret;
	}
	std::tuple<std::size_t, View, View> MemoryCache::_page_disk(key_type key, const View& sort, std::size_t count, FlushHandle& handle) noexcept
	{
		// // Lots of it is straight up copied from _read_impl, probably should deduplicate later

		// auto& [ data, indexer, bloom, _1, _2 ] = handle;

		// // Search the indexer
		// const auto partition_offset = _disk_find_partition(key, handle);
		// if (!partition_offset.has_value())
		// 	return {};
		// const auto offset = partition_offset.value();
		// auto off = partition_offset.value();

		// const auto partition_size = byte::sread<std::uint64_t>(data.memory(), off);

		// std::size_t partition_footer_off = off + partition_size;

		// const auto dynamic_prefix = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);
		// const auto prefix = dynamic_prefix ? dynamic_prefix : sort.size();
		// const auto sparse_block_indices = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);
		// const auto sort_bloom_offset = byte::sread<std::uint64_t>(data.memory(), partition_footer_off);

		// if (!_bloom_may_contain(key, sort_bloom_offset, handle))
		// 	return {};

		// const auto sparse_block_offset = sparse_block_indices ? byte::search_partition_binary<std::uint32_t>(
		// 	sort.data(), data.memory().subspan(partition_footer_off), sparse_block_indices, true, true
		// ) : std::optional<std::uint32_t>(off);

		// // Linar search across blocks
		// if (sparse_block_offset.has_value())
		// {
		// 	data.hint(Mapper::Access::Sequential);

		// 	off = sparse_block_offset.value();
		// 	while (off < offset + partition_size)
		// 	{
		// 		/*const auto checksum = */byte::sread<std::uint64_t>(data.memory(), off);
		// 		/*const auto prefix = */byte::sread<std::uint32_t>(data.memory(), off);
		// 		const auto index_count = byte::sread<std::uint32_t>(data.memory(), off);

		// 		View min_key = View::view(data.memory().subspan(off, prefix));
		// 		View max_key = View::view(data.memory().subspan(off + (index_count - 1) * (prefix + sizeof(std::uint32_t)), prefix));

		// 		const auto saved_off = off;
		// 		off += index_count * (prefix + sizeof(std::uint32_t));

		// 		const auto decompressed = byte::sread<std::uint32_t>(data.memory(), off);
		// 		const auto compressed = byte::sread<std::uint32_t>(data.memory(), off);

		// 		const auto result_min = byte::binary_compare(sort, min_key);
		// 		const auto result_max = byte::binary_compare(sort, max_key);

		// 		if ((result_min <= 0 && result_max >= 0) ||
		// 			(result_max <= 0 && result_min >= 0))
		// 		{
		// 			const auto ascending = (result_min <= 0 && result_max >= 0);

		// 			const auto sparse_offset = byte::search_partition_binary<std::uint32_t>(
		// 				sort.data(), data.memory().subspan(saved_off), prefix, index_count, ascending, true
		// 			);

		// 			if (sparse_offset.has_value())
		// 			{
		// 				StaticBufferSink sink = (decompressed == compressed) ?
		// 					StaticBufferSink() : StaticBufferSink(decompressed);
		// 				SourceView source(data.memory().subspan(off, compressed));
		// 				std::span<const unsigned char> block;

		// 				if (decompressed != compressed)
		// 				{
		// 					snappy::Uncompress(&source, &sink);
		// 					block = sink.data();
		// 				}
		// 				else
		// 				{
		// 					block = data.memory().subspan(off, decompressed);
		// 				}

		// 				off = sparse_offset.value();
		// 				do
		// 				{
		// 					const auto type = DataType(block[off++]);
		// 					const auto instance = View::view(block.subspan(off));
		// 					bool eq = false;
		// 					if (type == DataType::SchemaInstance)
		// 					{
		// 						const auto len = info.prefix_length(block.data() + off);
		// 						View prefix = View::copy(len);
		// 						info.prefix(block.data() + off, View::view(prefix));
		// 						eq = byte::binary_equal(sort, prefix.data());
		// 					}
		// 					else if (type == DataType::Tombstone)
		// 					{
		// 						RDB_TRACE(mem, "vcpu", _id, "MC READ VALUE REMOVED")
		// 						return false;
		// 					}
		// 					else
		// 					{
		// 						const auto len = byte::sread<std::uint32_t>(block, off);
		// 						eq = byte::binary_equal(sort, block.subspan(off, len));
		// 						off += len;
		// 					}

		// 					if (eq)
		// 					{
		// 						RDB_TRACE(mem, "vcpu", _id, "MC READ FOUND VALUE")
		// 						if ((found += _read_entry_impl(instance, type, fields, callback)) == required)
		// 						{
		// 							return true;
		// 						}
		// 						else
		// 						{
		// 							RDB_TRACE(mem, "vcpu", _id, "MC CONTINUE SEARCH")
		// 							break;
		// 						}
		// 					}
		// 					else
		// 					{
		// 						off += _read_entry_size_impl(
		// 							View::view(block.subspan(off)), type
		// 						);
		// 					}
		// 				} while (off < block.size());
		// 			}
		// 			else
		// 			{
		// 				return {};
		// 			}
		// 		}
		// 		else
		// 			off += compressed;
		// 	}
		// }

		return {};
	}

	View MemoryCache::page(key_type key, std::size_t count) noexcept
	{
		return page_from(key, nullptr, count);
	}
	View MemoryCache::page_from(key_type key, const View& sort, std::size_t count) noexcept
	{
		thread_local std::unique_ptr<View[]> frag_pool_data{ new View[4] };
		thread_local std::span<View> frag_pool{ frag_pool_data.get(), 4 };

		RDB_LOG(mem, "vcpu", _id, "MC PAGE {}c <{}>", _id, count, uuid::encode(key, uuid::table_alnum))

		auto& info = _info();
		if (!info.skeys())
		{
			RDB_WARN("vcpu", _id, "VCPU{} MC PAGE NULL READ <{}>", _id, count, uuid::encode(key, uuid::table_alnum))
			return nullptr;
		}
		else if (count == 0)
		{
			RDB_WARN("vcpu", _id, "VCPU{} MC PAGE ZERO READ <{}>", _id, count, uuid::encode(key, uuid::table_alnum))
			return nullptr;
		}
		else
		{
			auto partition = _find_partition(*_map, key);
			View result = nullptr;
			View last = nullptr;

			auto push = [&](View value, std::size_t cnt) {
				count -= cnt;
				if (cnt)
				{
					if (result == nullptr)
					{
						if (!count)
						{
							result = std::move(value);
						}
						else
						{
							result = View::copy();
							auto& v = *result.vec();
							v.reserve((count * value.size()) / cnt);
							v.insert(v.end(), value.begin(), value.end());
						}
					}
					else
					{
						auto& v = *result.vec();
						v.insert(v.end(), value.begin(), value.end());
					}

				}
			};

			// Check primary cache
			if (partition != _map->end())
			{
				auto [ cnt, li, lkey ] = _page_map(partition, key, sort, count);
				push(std::move(li), cnt);
				last = std::move(lkey);
			}
			// Check readonly maps
			if (count)
			{
				if (_flush_running)
				{
					RDB_TRACE(mem, "vcpu", _id, "MC PAGE SCANNING RMPS")
					for (auto it = _readonly_maps.rbegin(); it != _readonly_maps.rend(); ++it)
					{
						if (const auto lock = it->lock(); lock != nullptr)
						{
							auto data = _find_partition(*lock, key);
							if (data != lock->end())
							{
								auto [ cnt, li, lkey ] = _page_map(data, key, last, count);
								push(std::move(li), cnt);
								if (lkey != nullptr)
									last = std::move(lkey);
								if (!count)
									break;
							}
						}
					}
				}
				else
					_readonly_maps.clear();
			}
			// Check disk
			if (count)
			{
				const auto flush_running = _flush_running.load();
				for (std::size_t j = _flush_id - flush_running; j > 0; j--)
				{
					RDB_TRACE(mem, "vcpu", _id, "MC PAGE SEARCHING FLUSH{}", _id, j - 1)

					const auto i = j - 1;

					auto& handle = _handle_open(i);
					auto& [ data, indexer, bloom, _1, _2 ] = handle;

					if (handle.ready() && _bloom_may_contain(key, handle))
					{
						auto [ cnt, li, lkey ] = _page_disk(key, last, count, handle);
						push(std::move(li), cnt);
						if (lkey != nullptr)
							last = std::move(lkey);
						if (!count)
							break;
					}
				}
			}
			RDB_LOG(mem, "vcpu", _id, "MC PAGE SIZE {}b <{}>", _id, result.size(), uuid::encode(key, uuid::table_alnum))
			return result;
		}
	}
	bool MemoryCache::read(key_type key, const View& sort, field_bitmap fields, const read_callback& callback) noexcept
	{
		return _read_impl(key, sort, fields, callback);
	}
	bool MemoryCache::exists(key_type key, const View& sort) noexcept
	{
		return _read_impl(key, sort, field_bitmap(), nullptr);
	}

	MemoryCache::write_store::iterator MemoryCache::_create_partition_log_if(write_store& map, key_type key, const View& pkey) noexcept
	{
		auto& schema = _info();
		auto [ it, created ] = map.emplace(
			key,
			std::make_pair(
				View::copy(pkey),
				schema.skeys() ? partition_variant(partition()) : partition_variant(single_slot())
			)
		);
		if (created)
		{
			RDB_LOG(mem, "vcpu", _id, "MC CREATE PARTITION <{}>", _id, uuid::encode(key, uuid::table_alnum))
			_disk_logs.log(WriteType::CreatePartition, key, nullptr, pkey);
		}
		return it;
	}
	MemoryCache::write_store::iterator MemoryCache::_create_partition_if(write_store& map, key_type key, const View& pkey) noexcept
	{
		auto& schema = _info();
		return map.emplace(
			key,
			std::make_pair(
				View::copy(pkey),
				schema.skeys() ? partition_variant(partition()) : partition_variant(single_slot())
			)
		).first;
	}
	MemoryCache::write_store::iterator MemoryCache::_find_partition(write_store& map, key_type key) noexcept
	{
		return map.find(key);
	}

	MemoryCache::slot MemoryCache::_create_sorted_slot(write_store::iterator partition, const View& sort, DataType vtype, std::size_t reserve)
	{
		auto& data = std::get<MemoryCache::partition>(partition->second.second);
		if (const auto f = data.find(sort);
			f != nullptr && f->capacity >= reserve)
		{
			f->vtype = vtype;
			f->size = reserve;
			return f;
		}
		return data.insert(sort, vtype, reserve);
	}
	MemoryCache::slot MemoryCache::_create_unsorted_slot(write_store::iterator partition, DataType vtype, std::size_t reserve)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		if (ptr != nullptr && ptr->capacity >= reserve)
		{
			ptr->vtype = vtype;
			ptr->size = reserve;
			return ptr.get();
		}
		else
		{
			ptr.reset(SlotDeleter::allocate(vtype, reserve));
			return ptr.get();
		}
	}
	MemoryCache::slot MemoryCache::_create_slot(write_store::iterator partition, const View& sort, DataType vtype, std::size_t reserve)
	{
		auto& schema = _info();
		if (schema.skeys())
			return _create_sorted_slot(partition, sort, vtype, reserve);
		return _create_unsorted_slot(partition, vtype, reserve);
	}

	MemoryCache::slot MemoryCache::_create_sorted_slot(write_store::iterator partition, const View& sort, DataType vtype, std::span<const unsigned char> buffer)
	{
		auto& data = std::get<MemoryCache::partition>(partition->second.second);
		if (const auto f = data.find(sort);
			f != nullptr && f->capacity >= buffer.size())
		{
			f->vtype = vtype;
			f->size = buffer.size();
			std::memcpy(
				f->buffer().data(),
				buffer.data(),
				buffer.size()
			);
			return f;
		}
		return data.insert(sort, vtype, buffer);
	}
	MemoryCache::slot MemoryCache::_create_unsorted_slot(write_store::iterator partition, DataType vtype, std::span<const unsigned char> buffer)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		if (ptr != nullptr && ptr->capacity >= buffer.size())
		{
			ptr->vtype = vtype;
			ptr->size = buffer.size();
			std::memcpy(
				ptr->buffer().data(),
				buffer.data(),
				buffer.size()
			);
			return ptr.get();
		}
		else
		{
			ptr.reset(SlotDeleter::allocate(vtype, buffer));
			return ptr.get();
		}
	}
	MemoryCache::slot MemoryCache::_create_slot(write_store::iterator partition, const View& sort, DataType vtype, std::span<const unsigned char> buffer)
	{
		auto& schema = _info();
		if (schema.skeys())
			return _create_sorted_slot(partition, sort, vtype, buffer);
		return _create_unsorted_slot(partition, vtype, buffer);
	}

	MemoryCache::slot MemoryCache::_resize_sorted_slot(write_store::iterator partition, const View& sort, std::size_t size)
	{
		auto& data = std::get<MemoryCache::partition>(partition->second.second);
		auto* ptr = data.find(sort);
		auto* nptr = SlotDeleter::allocate(ptr->vtype, size);

		std::memcpy(
			nptr->buffer().data(),
			ptr->buffer().data(),
			ptr->size
		);
		data.insert(sort, nptr);

		return nptr;
	}
	MemoryCache::slot MemoryCache::_resize_unsorted_slot(write_store::iterator partition, std::size_t size)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		auto* nptr = SlotDeleter::allocate(ptr->vtype, size);

		std::memcpy(
			nptr->buffer().data(),
			ptr->buffer().data(),
			ptr->size
		);
		ptr.reset(nptr);

		return ptr.get();
	}
	MemoryCache::slot MemoryCache::_resize_slot(write_store::iterator partition, const View& sort, std::size_t size)
	{
		auto& schema = _info();
		if (schema.skeys())
			return _resize_sorted_slot(partition, sort, size);
		return _resize_unsorted_slot(partition, size);
	}

	MemoryCache::slot MemoryCache::_find_sorted_slot(write_store::iterator partition, const View& sort)
	{
		auto& data = std::get<MemoryCache::partition>(partition->second.second);
		return data.find(sort);
	}
	MemoryCache::slot MemoryCache::_find_unsorted_slot(write_store::iterator partition)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		return ptr.get();
	}
	MemoryCache::slot MemoryCache::_find_slot(write_store::iterator partition, const View& sort)
	{
		auto& schema = _info();
		if (schema.skeys())
			return _find_sorted_slot(partition, sort);
		return _find_unsorted_slot(partition);
	}

	void MemoryCache::_write_impl(write_store::iterator partition, WriteType type, const View& sort, std::span<const unsigned char> data) noexcept
	{
		if (type == WriteType::Table)
		{
			auto& info =
				_info();

			View prefix = nullptr;
			if (info.skeys())
			{
				const auto plen = info.prefix_length(data.data());
				prefix = View::copy(plen);
				info.prefix(data.data(), View::view(prefix));
			}
			_create_slot(partition, prefix, DataType::SchemaInstance, data);
			_push_bytes(data.size() + prefix.size() + sizeof(key_type));

			return;
		}

		auto* slot = _find_slot(partition, sort);
		if (slot == nullptr)
		{
			if (type == WriteType::Field)
			{
				_create_slot(partition, sort, DataType::FieldSequence, data);
				_push_bytes(data.size() + sort.size() + sizeof(key_type));
			}
			else if (type == WriteType::WProc)
			{
				field_bitmap fields;
				fields.set(data[0]);

				View result;
				if (!read(partition->first, sort, fields,
						[&](std::size_t, View value) {
							result = std::move(value);
						})
					) return;

				auto& info =
					_info();
				RuntimeInterfaceReflection::RTII& finfo =
					info.reflect(data[0]);

				auto* sdata = result.mutate().data();
				const auto args = View::view(data.subspan(2));
				const auto op = data[1];
				const auto type = finfo.wproc(sdata, op, args, wproc_query::Type);
				if (type == wproc_type::Dynamic)
				{
					const auto size = finfo.wproc(sdata, op, args, wproc_query::Storage);
					if (size > slot->size)
						slot = _create_slot(partition, sort, DataType::FieldSequence, size + 1);
				}
				else
					slot = _create_slot(partition, sort, DataType::FieldSequence, result.size() + 1);

				slot->buffer()[0] = data[0];
				std::memcpy(slot->buffer().data() + 1, result.data().data(), result.size());
				finfo.wproc(slot->buffer().data() + 1, op, args, wproc_query::Commit);

				_push_bytes(result.size() + sort.size() + sizeof(key_type));
			}
		}
		else if (type == WriteType::Field)
		{
			auto& info =
				_info();

			if (slot->vtype == DataType::SchemaInstance)
			{
				auto state = FieldWriteApplyState{
					.size = slot->size,
					.capacity = slot->capacity
				};
				const auto psize = static_cast<int>(slot->capacity);
				const auto size = info.fwapply(
					slot->buffer().data(),
					data[0], View::view(data.subspan(1)),
					state
				);
				if (size > slot->capacity)
				{
					slot = _resize_slot(partition, sort, size);
					state.capacity = size;
					info.fwapply(
						slot->buffer().data() + 1,
						data[0], View::view(data.subspan(1)),
						state
					);
				}
				slot->size = size;
				_push_bytes(static_cast<int>(slot->capacity) - psize);
			}
			else
			{
				RuntimeInterfaceReflection::RTII& finfo =
					info.reflect(data[0]);

				auto* sdata = slot->buffer().data();
				for (std::size_t off = 0; off != slot->size;)
				{
					const auto field = sdata[off++];
					RuntimeInterfaceReflection::RTII& cinfo =
						info.reflect(field);

					const auto fsize = cinfo.storage(sdata + off);
					if (field == data[0])
					{
						const auto args = View::view(data.subspan(1));
						const auto psize = static_cast<int>(slot->capacity);
						auto req = slot->size;
						if (args.size() != fsize)
						{
							const auto diff = static_cast<int>(args.size()) - static_cast<int>(fsize);
							req = static_cast<std::size_t>(psize + diff);
							if (req > slot->capacity)
								slot = _resize_slot(partition, sort, req);
							auto* src = slot->buffer().data() + off;
							auto* fdata = src + fsize;
							const auto back = slot->size - (src - reinterpret_cast<unsigned char*>(this));
							std::memmove(
								fdata + diff,
								fdata,
								back
							);
						}
						std::memcpy(
							slot->buffer().data() + off,
							args.data().data(),
							args.size()
						);
						slot->size = req;
						_push_bytes(static_cast<int>(slot->capacity) - static_cast<int>(psize));
						break;
					}
					off += fsize;
				}
			}
		}
		else if (type == WriteType::WProc)
		{
			auto& info =
				_info();
			RuntimeInterfaceReflection::RTII& finfo =
				info.reflect(data[0]);

			if (finfo.fragmented())
			{

			}
			else
			{
				if (slot->vtype == DataType::Tombstone)
				{
					return;
				}
				else if (slot->vtype == DataType::SchemaInstance)
				{
					auto state = WriteProcApplyState{
						.size = slot->size,
						.capacity = slot->capacity
					};
					const auto ptr = slot->buffer().data();
					const auto field = data[0];
					const auto op = data[1];
					const auto args = View::view(data.subspan(2));
					const auto size = info.wpapply(ptr, field, op, args, state);
					const auto psize = static_cast<int>(slot->capacity);
					if (size > slot->capacity)
					{
						slot = _resize_slot(partition, sort, size);
						state.capacity = slot->capacity;
						info.wpapply(slot->buffer().data(), field, op, args, state);
					}
					slot->size = size;
					_push_bytes(static_cast<int>(slot->capacity) - psize);
				}
				else if (slot->vtype == DataType::FieldSequence)
				{
					auto* sdata = slot->buffer().data();
					for (std::size_t off = 0; off != slot->size;)
					{
						const auto field = sdata[off++];
						RuntimeInterfaceReflection::RTII& cinfo =
							info.reflect(field);
						const auto fsize = cinfo.storage(sdata + off);
						if (field == data[0])
						{
							const auto args = View::view(data.subspan(2));
							const auto op = data[1];
							const auto type = finfo.wproc(sdata + off, op, args, wproc_query::Type);
							const auto psize = static_cast<int>(slot->capacity);
							auto req = slot->size;
							if (type == wproc_type::Dynamic)
							{
								const auto size = finfo.wproc(sdata + off, op, args, wproc_query::Storage);
								const auto diff = static_cast<int>(size) - static_cast<int>(fsize);
								req = static_cast<std::size_t>(psize + diff);
								if (req > slot->capacity)
									slot = _resize_slot(partition, sort, req);
								auto* src = slot->buffer().data() + off;
								auto* fdata = src + fsize;
								const auto back = slot->size - (src - reinterpret_cast<unsigned char*>(this));
								std::memmove(
									fdata + diff,
									fdata,
									back
								);
							}
							finfo.wproc(slot->buffer().data() + off, op, args, wproc_query::Commit);
							slot->size = req;
							_push_bytes(static_cast<int>(slot->capacity) - static_cast<int>(psize));
							break;
						}
						off += fsize;
					}
				}
			}
		}
	}
	void MemoryCache::_reset_impl(write_store::iterator partition, const View& sort) noexcept
	{
		auto& schema = _info();
		auto* slot = _create_slot(partition, sort, DataType::SchemaInstance, schema.cstorage(sort));
		schema.construct(slot->buffer().data(), sort);
		_pressure += slot->size + sizeof(key_type) + 16;
	}
	void MemoryCache::_remove_impl(write_store::iterator partition, const View& sort) noexcept
	{
		_pressure += sizeof(key_type) + 24;
		_create_slot(partition, View::view(sort), DataType::Tombstone, 0);
	}

	void MemoryCache::write(WriteType type, key_type key, const View& partition, const View& sort, std::span<const unsigned char> data, Origin origin) noexcept
	{
		RDB_LOG(mem, "vcpu", _id, "MC WRITE <{}> {}b", _id, uuid::encode(key, uuid::table_alnum), data.size())
		[[ unlikely ]] if (is_locked(key, sort, origin))
		{
			RDB_LOG(mem, "vcpu", _id, "MC LOCKED <{}>", _id, uuid::encode(key, uuid::table_alnum))
			return;
		}
		auto& schema
			= _info();
		const auto part = _create_partition_log_if(*_map, key, partition);
		_disk_logs.log(type, key, sort, View::view(data));
		_write_impl(part, type, sort, data);
		_flush_if();
	}
	void MemoryCache::reset(key_type key, const View& partition, const View& sort, Origin origin) noexcept
	{
		RDB_LOG(mem, "vcpu", _id, "MC RESET <{}>", _id, uuid::encode(key, uuid::table_alnum))
		[[ unlikely ]] if (is_locked(key, sort, origin))
		{
			RDB_LOG(mem, "vcpu", _id, "MC LOCKED <{}>", _id, uuid::encode(key, uuid::table_alnum))
			return;
		}
		const auto part = _create_partition_log_if(*_map, key, partition);
		_disk_logs.log(WriteType::Reset, key, sort);
		_reset_impl(part, sort);
		_flush_if();
	}
	void MemoryCache::remove(key_type key, const View& sort, Origin origin) noexcept
	{
		RDB_LOG(mem, "vcpu", _id, "MC REMOVE <{}>", _id, uuid::encode(key, uuid::table_alnum))
		[[ unlikely ]] if (is_locked(key, sort, origin))
		{
			RDB_LOG(mem, "vcpu", _id, "MC LOCKED <{}>", _id, uuid::encode(key, uuid::table_alnum))
			return;
		}
		_disk_logs.log(WriteType::Remov, key, sort);
		_remove_impl(
			_create_partition_log_if(*_map, key, sort),
			sort
		);
		_flush_if();
	}

	MemoryCache::lock_type MemoryCache::_emplace_sorted_lock_if(lock_store::iterator partition, const View& sort)
	{
		auto& data = std::get<MemoryCache::partition_lock>(partition->second);
		return data.try_emplace(sort);
	}
	MemoryCache::lock_type MemoryCache::_emplace_unsorted_lock_if(lock_store::iterator partition)
	{
		auto& ptr = std::get<single_lock>(partition->second);
		return &ptr;
	}
	MemoryCache::lock_type MemoryCache::_emplace_lock_if(key_type key, const View& sort)
	{
		auto [ partition, created ] = _locks.emplace(key, lock_partition_variant());
		auto& schema = _info();
		if (schema.skeys())
		{
			[[ unlikely ]] if (created)
				partition->second = partition_lock();
			return _emplace_sorted_lock_if(partition, sort);
		}
		[[ unlikely ]] if (created)
			partition->second = single_lock();
		return _emplace_unsorted_lock_if(partition);
	}

	MemoryCache::Lock MemoryCache::lock(key_type key, const View& sort, Origin origin) noexcept
	{
		auto lock = _emplace_lock_if(key, sort);
		if (!lock->expired())
			return Lock(*lock);
		lock->lock(origin);
		return Lock(nullptr);
	}
	bool MemoryCache::unlock(key_type key, const View& sort, Origin origin) noexcept
	{
		auto lock = _emplace_lock_if(key, sort);
		if (lock->origin == origin)
		{
			const auto res = !lock->expired();
			lock->unlock();
			_lock_cnt--;
			RDB_WARN_IF(lock->expired(), mem, "vcpu", _id, "MC LOCK EXPIRED BEFORE UNLOCK <", uuid::encode(key, uuid::table_compact), ">")
			return res;
		}
		return false;
	}
	bool MemoryCache::is_locked(key_type key, const View& sort, Origin origin) noexcept
	{
		if (_lock_cnt == 0)
			return false;
		auto partition = _locks.find(key);
		if (partition == _locks.end())
			return false;

		auto& schema = _info();
		if (schema.skeys())
		{
			auto& data = std::get<MemoryCache::partition_lock>(partition->second);
			auto* f = data.find(sort);
			if (f->expired())
			{
				const auto lcnt = _lock_cnt;
				[[ unlikely ]] if (f->expired_auto())
					_lock_cnt--;
				if (lcnt > _shared.cfg->cache.max_locks)
				{
					if (data.size() == 1)
						_locks.erase(key);
					else
						data.remove(sort);
				}
				return false;
			}
			return f->origin != origin;
		}
		else
		{
			auto& ptr = std::get<single_lock>(partition->second);
			if (ptr.expired())
			{
				const auto lcnt = _lock_cnt;
				[[ unlikely ]] if (ptr.expired_auto())
					_lock_cnt--;
				if (lcnt > _shared.cfg->cache.max_locks)
					_locks.erase(key);
				return false;
			}
			return ptr.origin != origin;
		}
	}

	bool MemoryCache::_bloom_may_contain(key_type key, FlushHandle& handle) const noexcept
	{
		return _bloom_may_contain(key, sizeof(std::uint8_t), handle);
	}
	bool MemoryCache::_bloom_may_contain(key_type key, std::size_t off, FlushHandle& handle) const noexcept
	{
		auto& bloom = handle.bloom;

		const auto prob = byte::sread<std::uint16_t>(bloom.memory(), off) / 10'000.f;
		const auto size = byte::sread<std::uint32_t>(bloom.memory(), off);
		const auto bits = _bloom_bits(size, prob);
		const auto hashes = _bloom_hashes(bits, size);

		const auto* buffer = bloom.memory().data() + off;
		const auto [ k1, k2 ] = _hash_pair(key);
		for (std::size_t i = 0; i < hashes; i++)
		{
			const auto idx = (k1 + (i * k2)) % bits;
			const auto quot = idx >> 3;
			const auto rem = idx & 7;
			if ((buffer[quot] &
				(std::uint8_t(1) << rem)) == 0)
				return false;
		}

		return true;
	}
	std::size_t MemoryCache::_bloom_bits(std::size_t keys, float probability) const noexcept
	{
		const float nkeys = keys;
		const float l2 = std::log(2.f);
		return static_cast<std::size_t>((-nkeys * std::log(probability)) / (l2 * l2));
	}
	std::size_t MemoryCache::_bloom_hashes(std::size_t bits, std::size_t keys) const noexcept
	{
		const float nbits = bits;
		const float nkeys = keys;
		return static_cast<std::size_t>(
			std::max(
				1.f,
				std::round((nbits * std::log(2.f)) / nkeys)
			)
		);
	}
	std::pair<key_type, key_type> MemoryCache::_hash_pair(key_type key) const noexcept
	{
		return {
			uuid::xxhash({ reinterpret_cast<const unsigned char*>(&key), sizeof(key) }, 0xfabb318e),
			uuid::xxhash({ reinterpret_cast<const unsigned char*>(&key), sizeof(key) }, 0xa65ffcf46)
		};
	}

	void MemoryCache::_bloom_round_impl(key_type key, unsigned char* buffer, std::size_t count, std::size_t bits) noexcept
	{
		const auto hashes = _bloom_hashes(bits, count);
		const auto [ k1, k2 ] = _hash_pair(key);
		for (std::size_t i = 0; i < hashes; i++)
		{
			const auto idx = (k1 + (i * k2)) % bits;
			const auto quot = idx >> 3;
			const auto rem = idx & 7;
			buffer[quot] |=
				std::uint8_t(1) << rem;
		}
	}
	void MemoryCache::_bloom_impl(const write_store& map, Mapper& bloom, int id) noexcept
	{
		// [ uint8(flag,type) | [ uint32(key-count) | uint16[probability as 1/100 of percentage] | ... ] ... ]

		if (_shared.cfg->cache.partition_bloom_fp_rate == 1.f)
			return;

		const auto prob = _shared.cfg->cache.partition_bloom_fp_rate;
		const auto prob_conv = static_cast<std::uint16_t>(prob * 10'000);
		const auto bits = _bloom_bits(map.size(), prob);

		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} BEGIN BLOOM WRITE {}bits", _id, id, bits)

		bloom.vmap();
		bloom.hint(Mapper::Access::Random);
		bloom.hint(Mapper::Access::Hot);

		bloom.vmap_increment(byte::swrite<std::uint8_t>(bloom.append(), BloomType::PK_SK));
		bloom.vmap_increment(byte::swrite<std::uint16_t>(bloom.append(), prob_conv));
		bloom.vmap_increment(byte::swrite<std::uint32_t>(bloom.append(), map.size()));
		const auto buffer = bloom.append();
		for (decltype(auto) key : map)
			_bloom_round_impl(key.first, buffer, map.size(), bits);
		bloom.vmap_increment((bits + 7) / 8);

		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} END BLOOM WRITE {}b", _id, id, bloom.size())
	}

	std::size_t MemoryCache::_bloom_intra_partition_begin_impl(write_store::const_iterator part, Mapper& bloom, int id) noexcept
	{
		if (_shared.cfg->cache.intra_partition_bloom_fp_rate == 1.f)
			return 0;

		const auto size = std::get<partition>(part->second.second).size();
		const auto prob = _shared.cfg->cache.intra_partition_bloom_fp_rate;
		const auto prob_conv = static_cast<std::uint16_t>(prob * 10'000);
		const auto bits = _bloom_bits(size, prob);

		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} BEGIN PARTITION BLOOM WRITE {}bits", _id, id, bits)

		bloom.vmap_increment(byte::swrite<std::uint16_t>(bloom.append(), prob_conv));
		bloom.vmap_increment(byte::swrite<std::uint32_t>(bloom.append(), size));

		return bits;
	}
	void MemoryCache::_bloom_intra_partition_round_impl(write_store::const_iterator part, const View& key, std::size_t bits, Mapper& bloom, int id) noexcept
	{
		if (!bits)
			return;
		_bloom_round_impl(
			uuid::xxhash(key),
			bloom.append(),
			std::get<partition>(part->second.second).size(),
			bits
		);
	}
	void MemoryCache::_bloom_intra_partition_end_impl(write_store::const_iterator partition, std::size_t bits, Mapper& bloom, int id) noexcept
	{
		if (_shared.cfg->cache.intra_partition_bloom_fp_rate == 1.f)
			return;
		bloom.vmap_increment((bits + 7) / 8);
		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} END BLOOM WRITE {}b", _id, id, bloom.size())
	}
	void MemoryCache::_data_impl(const write_store& map, Mapper& data, Mapper& indexer, Mapper& bloom, int id) noexcept
	{
		const auto amortized_block_size = static_cast<std::size_t>(_shared.cfg->cache.block_size * 1.2);

		thread_local std::unique_ptr<BlockSourceMultiplexer::Node[]> frag_pool_data{ new BlockSourceMultiplexer::Node[1024] };
		thread_local std::unique_ptr<unsigned char[]> block_pool_data{ new unsigned char[amortized_block_size] };
		thread_local std::unique_ptr<unsigned char[]> compressed_block_pool_data{ new unsigned char[amortized_block_size] };

		thread_local std::span<BlockSourceMultiplexer::Node> frag_pool{ frag_pool_data.get(), 1024 };
		thread_local std::span<unsigned char> block_pool{ block_pool_data.get(), amortized_block_size };
		thread_local std::span<unsigned char> compressed_block_pool{ compressed_block_pool_data.get(), amortized_block_size };

		// Unique partition key format (i.e. partition key maps to a single value)
		// Data header
		// stores -
		// uint64(version)
		// uint64(sparse index size)
		// uint64(block size)
		// Indexer
		// [ key_type(max_partition) uint32(count) index[ key_type(min-key), uint64(block-metadata-offset) ], ... ]
		// Sparse indexer
		// [ index[ key_type(min-key), uint32(offset-from-block-begin) ], ... ]
		// Data
		// [ block[ uint32(decompressed-size) | uint32(compressed-size) | uint64(checksum) | uint32(index-count) ], data(compressed/or not) ... ]
		// Every micro_index_ratio KV-pairs write a local index (for binary search inside block)
		// Data point
		// [ byte(type) | ... ]
		// REMEMBER
		// implement prefixes for dynamic sort key types
		// implement per-partition sort-key block index (could just push it at the beginning)

		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} BEGIN DATA WRITE", _id, id)
		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} BEGIN INDEXER WRITE {}", _id, id, map.size())

		auto& info =
			_info();
		const auto keys = info.skeys();

		indexer.reserve(sizeof(key_type) + sizeof(std::uint32_t) + (sizeof(key_type) + sizeof(std::uint64_t)) * map.size());
		indexer.map();
		data.vmap();
		indexer.hint(Mapper::Access::Sequential);
		data.hint(Mapper::Access::Sequential);
		data.hint(Mapper::Access::Huge);

		// Sort keys
		std::vector<key_type> offsets{};
		{
			RDB_TRACE(mem, "vcpu", _id, "MC FLUSH{} SORTING KEYS", _id, id)
			offsets.resize(map.size());
			std::transform(map.begin(), map.end(), offsets.begin(), [](const auto& it) { return it.first; });
			std::sort(offsets.begin(), offsets.end());
		}
		// Metadata
		std::size_t idxoff = 0;
		{
			RDB_TRACE(mem, "vcpu", _id, "MC FLUSH{} WRITING METADATA", _id, id)
			idxoff += byte::swrite<key_type>(indexer.memory(), idxoff, offsets.back());
			idxoff += byte::swrite<std::uint32_t>(indexer.memory(), idxoff, map.size());
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), version));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _shared.cfg->cache.block_sparse_index_ratio));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _shared.cfg->cache.partition_sparse_index_ratio));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _shared.cfg->cache.block_size));
		}
		// Stream blocks
		{
			const bool dynamic = !info.static_prefix();
			bool is_begin = true;
			std::size_t blocks = 0;
			std::size_t start = data.size();
			std::size_t bloom_offset = 0;
			std::size_t bloom_bits = 0;
			std::size_t i = 0;

			// For unary partitions

			std::vector<std::pair<key_type, std::uint64_t>> indices{};

			// For static wide partitions
			// Since keys are static length we just put them directly in the binary search table

			std::vector<std::pair<std::span<const unsigned char>, std::uint64_t>> sort_block_indices{};
			std::vector<std::pair<std::span<const unsigned char>, std::uint32_t>> sort_indices{};

			// For dynamic wide partitions
			// We store all keys in their own block (uncompressed) and then binary search a table with offsets to the block
			// The zero keyspace offset is since the beginning of the data file

			std::size_t sort_keyspace_offset = 0;
			std::size_t zero_keyspace_offset = ~0ull;
			std::vector<std::span<const unsigned char>> sort_keyspace{};
			std::vector<std::pair<std::uint64_t, std::uint64_t>> sort_block_dynamic_indices{};
			std::vector<std::pair<std::uint32_t, std::uint32_t>> sort_dynamic_indices{};

			if (!keys)
				indices.reserve((offsets.size() / _shared.cfg->cache.partition_sparse_index_ratio) / 2);

			BlockSourceMultiplexer source(block_pool, frag_pool);

			// Indexer logic
			auto index = [&]
			{
				idxoff += byte::swrite<key_type>(indexer.memory().subspan(idxoff), offsets[i]);
				idxoff += byte::swrite<std::uint64_t>(indexer.memory().subspan(idxoff), start);
			};

			// Block write logic
			auto write_block = [&]
			{
				if (source.empty())
					return;

				RDB_TRACE(mem, "vcpu", _id, "MC FLUSH{} EMITTING BLOCK", _id, id)
				RDB_WARN_IF(
					source.fragments() >= frag_pool.size(),
					"VCPU{} MC FLUSH{} HIGH FRAGMENTATION: {}", _id, id, source.fragments()
				)

				const auto saved_zero_index = (keys && !dynamic ? sort_indices[0].first : std::span<const unsigned char>());

				source.flush();
				// Index
				{
					RDB_TRACE(mem, "vcpu", _id, "MC FLUSH{} WRITING INDEX", _id, id)
					data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), source.digest()));
					if (keys)
					{
						if (dynamic)
						{
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_dynamic_indices.size()));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_keyspace_offset));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_keyspace_offset - sort_keyspace.back().size() - sizeof(std::uint16_t)));
							if (zero_keyspace_offset == ~0ull)
								zero_keyspace_offset = data.size();
							for (decltype(auto) it : sort_keyspace)
							{
								data.vmap_increment(byte::swrite<std::uint16_t>(data.append(), it.size()));
								data.vmap_increment(byte::swrite(data.append(), it));
							}
							for (decltype(auto) it : sort_dynamic_indices)
							{
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.first));
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.second));
							}
							sort_dynamic_indices.clear();
						}
						else
						{
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_indices.size()));
							for (decltype(auto) it : sort_indices)
							{
								data.vmap_increment(byte::swrite(data.append(), it.first));
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.second));
							}
							sort_indices.clear();
						}
					}
					else
					{
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), indices.size()));
						for (decltype(auto) it : indices)
						{
							data.vmap_increment(byte::swrite<key_type>(data.append(), it.first));
							data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), it.second));
						}
						indices.clear();
					}
				}
				// Compress and write (+write block index)
				RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} WRITE BLOCK{} {}b", _id, id, blocks++, source.size())
				{
					const auto psize = source.size();
					StaticBufferSink sink(psize, compressed_block_pool);
					snappy::Compress(&source, &sink);
					const auto compsize = sink.size();
					RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} WRITE BLOCK{} COMPRESSION RATIO {}%", _id, id, blocks - 1, (std::round((float(compsize) / psize) * 100) / 100) * 100)

					if (compsize / psize < _shared.cfg->cache.compression_ratio)
					{
						RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} WRITE BLOCK{} COMPRESSED", _id, id, blocks - 1)
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sink.size()));
						data.vmap_increment(byte::swrite(data.append(), sink.data()));
					}
					else
					{
						RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} WRITE BLOCK{} RAW", _id, id, blocks - 1)
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite(data.append(), source.block()));
					}

					source.clear();
					sink.clear();

					if (keys)
					{
						if (dynamic)
						{
							if (is_begin)
							{
								byte::swrite<std::uint64_t>(data.memory().subspan(start), data.size() - start - sizeof(std::uint64_t));
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_block_dynamic_indices.size()));
								data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), bloom_offset));
								for (decltype(auto) it : sort_block_dynamic_indices)
								{
									data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), it.first));
									data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), it.second));
								}
								sort_block_dynamic_indices.clear();
							}
							else if (blocks % _shared.cfg->cache.block_sparse_index_ratio == 0)
							{
								sort_block_dynamic_indices.push_back({
									zero_keyspace_offset,
									start
								});
								zero_keyspace_offset = ~0ull;
							}
						}
						else
						{
							if (is_begin)
							{
								byte::swrite<std::uint64_t>(data.memory().subspan(start), data.size() - start - sizeof(std::uint64_t));
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_block_indices.size()));
								data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), bloom_offset));
								for (decltype(auto) it : sort_block_indices)
								{
									data.vmap_increment(byte::swrite(data.append(), it.first));
									data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), it.second));
								}
								sort_block_indices.clear();
							}
							else if (blocks % _shared.cfg->cache.block_sparse_index_ratio == 0)
							{
								sort_block_indices.push_back({
									saved_zero_index,
									start
								});
							}
						}
					}
				}
				is_begin = false;
				source.clear();
			};

			for (i = 0; i < offsets.size(); i++)
			{
				// Gather range

				start = data.size();

				if (keys)
				{
					const auto part_iterator = map.find(offsets[i]);
					const auto& [ pkey, pdata ] = part_iterator->second;
					const auto& part = std::get<partition>(pdata);

					if (dynamic)
					{
						sort_block_dynamic_indices.reserve(part.size() / _shared.cfg->cache.partition_sparse_index_ratio);
						sort_dynamic_indices.reserve(sort_block_indices.capacity() / _shared.cfg->cache.block_sparse_index_ratio);
					}
					else
					{
						sort_block_indices.reserve(part.size() / _shared.cfg->cache.partition_sparse_index_ratio);
						sort_indices.reserve(sort_block_indices.capacity() / _shared.cfg->cache.block_sparse_index_ratio);
					}

					// Reserve partition data and setup partition
					{
						source.push({ .data = pkey });
						// Partition size (reserve)
						data.vmap_increment(sizeof(std::uint64_t));
					}
					// Start bloom filter
					{
						bloom_offset = bloom.size();
						bloom_bits = _bloom_intra_partition_begin_impl(part_iterator, bloom, id);
					}

					std::size_t j = 0;
					part.foreach([&](partition::const_key key, const Slot* data) {
						// Bloom filter

						_bloom_intra_partition_round_impl(part_iterator, View::view(key), bloom_bits, bloom, id);

						// Write data

						const auto buffer = data->flush_buffer();
						if (j % _shared.cfg->cache.partition_sparse_index_ratio == 0)
						{
							if (dynamic)
							{
								sort_dynamic_indices.push_back({
									sort_keyspace_offset,
									source.size()
								});
								sort_keyspace.push_back(key);
								sort_keyspace_offset += key.size() + sizeof(std::uint16_t);
							}
							else
							{
								sort_indices.push_back({
									key,
									source.size()
								});
							}
						}

						if (data->vtype == DataType::SchemaInstance) source.push({ .data = buffer });
						else source.push({ .key = key, .data = buffer });

						if (source.size() >= _shared.cfg->cache.block_size)
						{
							RDB_TRACE(mem, "vcpu", _id, "MC FLUSH{} BLOCK{} PRESSURE REACHED {}", _id, id, blocks, source.size())
							write_block();
						}
						return true;
					});

					// End bloom filter
					{
						_bloom_intra_partition_end_impl(part_iterator, bloom_bits, bloom, id);
					}
					// Write footer and end block
					{
						is_begin = true;
						index();
						write_block();
					}
				}
				else
				{
					// Construct block
					for (; i < offsets.size() && source.size() <= _shared.cfg->cache.block_size; i++)
					{
						const auto& [ pkey, pdata ] = map.at(offsets[i]);
						const auto buffer = std::get<single_slot>(pdata)->flush_buffer();
						source.push({ .data = pkey });
						if (!buffer.empty())
						{
							if (i % _shared.cfg->cache.partition_sparse_index_ratio == 0)
							{
								indices.push_back({
									offsets[i],
									source.size()
								});
							}
							source.push({ .data = byte::tspan(offsets[i]) });
						}
						source.push({ .data = buffer });
					}
					// Index and write block
					i--;
					{
						index();
						write_block();
					}
					i++;
				}
			}
		}

		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} END INDEXER WRITE {}b", _id, id, indexer.size())
		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} END DATA WRITE {}b", _id, id, data.size())
	}

	void MemoryCache::_data_close_impl(Mapper& data) noexcept
	{
		data.vmap_flush();
		data.close();
	}
	void MemoryCache::_indexer_close_impl(Mapper& indexer) noexcept
	{
		indexer.close();
	}
	void MemoryCache::_bloom_close_impl(Mapper& bloom) noexcept
	{
		bloom.vmap_flush();
		bloom.close();
	}

	void MemoryCache::_flush_impl(const write_store& map, int id) noexcept
	{
		std::filesystem::path fpath = _path/"flush"/std::format("f{}", std::to_string(id));
		std::filesystem::create_directory(fpath);

		Mapper data;
		Mapper indexer;
		Mapper bloom;
		Mapper lock;

		data.open(fpath/"data.dat");
		indexer.open(fpath/"indexer.idx");
		bloom.open(fpath/"filter.blx");
		lock.open(fpath/"lock");

		_bloom_impl(map, bloom, id);
		_data_impl(map, data, indexer, bloom, id);
		_data_close_impl(data);
		_indexer_close_impl(indexer);
		_bloom_close_impl(bloom);

		lock.remove();
	}
	void MemoryCache::_flush_if() noexcept
	{
		[[ unlikely ]] if (_pressure > _shared.cfg->cache.flush_pressure)
		{
			[[ unlikely ]] if (!_flush_thread.joinable())
			{
				_flush_thread = std::jthread([this]() {
					while (!_shutdown)
					{
						std::pair<std::shared_ptr<write_store>, std::size_t> flush_data;
						auto& [ map, id ] = flush_data;
						if (_flush_tasks.dequeue(flush_data) && flush_data.first != nullptr)
						{
							_flush_impl(*map, id);
							_disk_logs.mark(id);
							RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} END", _id, id)
							_handle_cache[id].unlocked.store(true, std::memory_order::release);
							--_flush_running;
							_flush_running.notify_all();
						}
					}
				});
			}
			flush();
		}
	}

	void MemoryCache::flush() noexcept
	{
		if (_map->empty())
			return;

		++_flush_running;
		_readonly_maps.push_back(_map);
		_disk_logs.snapshot(_flush_id);
		_handle_reserve();
		RDB_LOG(mem, "vcpu", _id, "MC FLUSH{} BEGIN {}b", _id, _flush_id.load(), _pressure)
		_flush_tasks.enqueue(_map, _flush_id++);

		_pressure = 0;
		_map = std::make_shared<write_store>();
	}
	void MemoryCache::clear() noexcept
	{
		if (_map->empty())
			return;
		_pressure = 0;
		_map->clear();
	}
}
