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
		_path = std::move(copy._path);
		_map = std::move(copy._map);
		_logs = std::move(copy._logs);
		_flush_id = copy._flush_id.load();
		_cfg = copy._cfg;
		_id = copy._id;
		_pressure = copy._pressure;
		_schema = copy._schema;
	}
	void MemoryCache::_push_bytes(std::size_t bytes) noexcept
	{
		_pressure += bytes;
	}

	MemoryCache::MemoryCache(Config* cfg, std::size_t core, schema_type schema) :
		_cfg(cfg),
		_map(std::make_shared<write_store>()),
		_path(
			cfg->root/
			std::format("vcpu{}", core)/
			std::format("[{}]", uuid::encode(schema, uuid::table_alnum))
		),
		_id(core),
		_logs(_cfg, _path/"logs", schema),
		_schema(schema)
	{
		_handle_cache.reserve(164);
		_handle_cache_tracker.reserve(164);
		if (!std::filesystem::exists(_path))
		{
			RDB_FMT("VCPU{} MC GENERATE", _id)
			std::filesystem::create_directories(_path);
			std::filesystem::create_directory(_path/"flush");
			std::filesystem::create_directory(_path/"logs");
		}
		else
		{
			RDB_FMT("VCPU{} MC REPLAY", _id)
			std::vector<std::filesystem::path> corrupted;
			for (decltype(auto) it : std::filesystem::directory_iterator(_path/"flush"))
			{
				if (std::filesystem::exists(it.path()/"lock"))
				{
					RDB_FMT("VCPU{} MCR DETECTED CORRUPTED FLUSH{}", _id,
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

			_logs.replay([this](WriteType type, key_type key, View sort, View data) {
				if (type == WriteType::CreatePartition)
				{
					RDB_FMT("VCPU{} MCR CREATE PARTITION <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_create_partition_if(*_map, key, data);
				}
				else if (type == WriteType::Reset)
				{
					RDB_FMT("VCPU{} MCR RESET <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_reset_impl(_find_partition(*_map, key), sort);
					_flush_if();
				}
				else if (type == WriteType::Remov)
				{
					RDB_FMT("VCPU{} MCR REMOVE <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_remove_impl(_find_partition(*_map, key), sort);
					_flush_if();
				}
				else
				{
					RDB_FMT("VCPU{} MCR WRITE <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_write_impl(_find_partition(*_map, key), type, sort, data);
					_flush_if();
				}
			});
		}
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

		if (_descriptors + descriptor_cost >= _cfg->cache.max_descriptors ||
			_mappings + map_cost >= _cfg->cache.max_mappings)
		{
			RDB_WARN("File resource limit")

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
		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
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
	std::size_t MemoryCache::_read_entry_impl(const View& view, DataType type, field_bitmap& fields, const read_callback* callback) noexcept
	{
		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
		std::size_t cnt = 0;
		if (type == DataType::FieldSequence)
		{
			std::size_t off = 0;
			do
			{
				const auto field = view.data()[off++];
				const auto beg = off;
				RuntimeInterfaceReflection::RTII& finf =
					info.reflect(field);
				off += finf.storage(view.data().data() + off);

				if (callback)
				{
					if (fields.test(field))
					{
						fields.reset(field);
						cnt++;
						(*callback)(field, View::view(view.data().subspan(beg, off - beg)));
					}
				}
				else
				{
					return 1;
				}
			} while (off < view.size());
		}
		else if (type == DataType::SchemaInstance)
		{
			std::size_t off = 0;
			std::size_t idx = 0;
			while (off < view.size())
			{
				const auto beg = off;
				RuntimeInterfaceReflection::RTII& finf =
					info.reflect(idx);
				off += finf.storage(view.data().data() + off);

				if (callback)
				{
					if (fields.test(idx))
					{
						fields.reset(idx);
						cnt++;
						(*callback)(idx, View::view(view.data().subspan(beg, off - beg)));
					}
				}
				else
				{
					return 1;
				}
				idx++;
			}
		}
		return cnt;
	}
	std::size_t MemoryCache::_read_cache_impl(write_store& map, key_type key, const View& sort, field_bitmap& fields, const read_callback* callback) noexcept
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

	bool MemoryCache::_read_impl(key_type key, const View& sort, field_bitmap fields, const read_callback* callback) noexcept
	{
		RDB_FMT("VCPU{} MC READ <{}>", _id, uuid::encode(key, uuid::table_alnum))

		// 1. Search cache
		// 2. If not found -> search disk (from newest to oldest)
		//
		// If requires accumulation -> get an accumulation handle and accumulate until tail or result
		// Accumulation applies even if the data is already in cache

		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
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
				RDB_TRACE("VCPU{} MC READ SCANNING RMPS", _id)
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
			RDB_TRACE("VCPU{} MC READ CACHE MISS", _id)
			for (std::size_t j = _flush_id - flush_running; j > 0; j--)
			{
				RDB_TRACE("VCPU{} MC READ SEARCHING FLUSH{}", _id, j - 1)

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
					{
						// Wide partition
						if (info.skeys())
						{
							RDB_TRACE("VCPU{} MC READ SEARCHING IN PARTITION", _id)

							const auto partition_size = byte::sread<std::uint64_t>(data.memory(), off);

							std::size_t partition_footer_off = off + partition_size;

							const auto dynamic_prefix = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);
							const auto prefix = dynamic_prefix ? dynamic_prefix : sort.size();
							const auto sparse_block_indices = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);
							const auto sort_bloom_offset = byte::sread<std::uint64_t>(data.memory(), partition_footer_off);

							if (!_bloom_may_contain(uuid::xxhash(sort), sort_bloom_offset, handle))
							{
								RDB_TRACE("VCPU{} MC READ INTRA-PARTITION BLOOM DISCARD", _id)
								continue;
							}

							const auto sparse_block_offset = sparse_block_indices ? byte::search_partition_binary<std::uint32_t>(
								sort.data(), data.memory().subspan(partition_footer_off), sparse_block_indices, true, true
							) : std::optional<std::uint32_t>(off);

							// Linar search across blocks
							if (sparse_block_offset.has_value())
							{
								data.hint(Mapper::Access::Sequential);

								off = sparse_block_offset.value();

								RDB_TRACE("VCPU{} MC READ SEARCHING IN BLOCK SEQUENCE", _id)
								while (off < offset + partition_size)
								{
									/*const auto checksum = */byte::sread<std::uint64_t>(data.memory(), off);
									/*const auto prefix = */byte::sread<std::uint32_t>(data.memory(), off);
									const auto index_count = byte::sread<std::uint32_t>(data.memory(), off);

									View min_key = View::view(data.memory().subspan(off, prefix));
									View max_key = View::view(data.memory().subspan(off + (index_count - 1) * (prefix + sizeof(std::uint32_t)), prefix));

									const auto saved_off = off;
									off += index_count * (prefix + sizeof(std::uint32_t));

									const auto decompressed = byte::sread<std::uint32_t>(data.memory(), off);
									const auto compressed = byte::sread<std::uint32_t>(data.memory(), off);

									const auto result_min = byte::binary_compare(sort, min_key);
									const auto result_max = byte::binary_compare(sort, max_key);

									if ((result_min <= 0 && result_max >= 0) ||
										(result_max <= 0 && result_min >= 0))
									{
										const auto ascending = (result_min <= 0 && result_max >= 0);

										RDB_TRACE("VCPU{} MC READ FOUND MATCHING BLOCK", _id)

										const auto sparse_offset = byte::search_partition_binary<std::uint32_t>(
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

											RDB_TRACE("VCPU{} MC READ LINEAR BLOCK SEARCH", _id)

											off = sparse_offset.value();
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
													RDB_TRACE("VCPU{} MC READ VALUE REMOVED", _id)
													return false;
												}
												else
												{
													const auto len = byte::sread<std::uint32_t>(block, off);
													eq = byte::binary_equal(sort, block.subspan(off, len));
													off += len;
												}

												if (eq)
												{
													RDB_TRACE("VCPU{} MC READ FOUND VALUE", _id)
													if ((found += _read_entry_impl(instance, type, fields, callback)) == required)
													{
														return true;
													}
													else
													{
														RDB_TRACE("VCPU{} MC CONTINUE SEARCH", _id)
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
											RDB_TRACE("VCPU{} MC READ BLOOM MISS", _id)
										}
									}
									else
										off += compressed;
								}
							}
							else
							{
								RDB_TRACE("VCPU{} MC READ BLOOM SORT MISS", _id)
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

							RDB_TRACE("VCPU{} MC READ SEARCHING IN BLOCK", _id)

							const auto sparse_offset = byte::search_partition<key_type, std::uint32_t>(
								key, data.memory().subspan(off), index_count, true
							);

							// Continue block hehader

							off += index_count * (sizeof(key_type) + sizeof(std::uint32_t));

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

								RDB_TRACE("VCPU{} MC READ LINEAR BLOCK SEARCH", _id)

								off = sparse_offset.value();
								do
								{
									if (byte::sread<key_type>(block, off) == key)
									{
										RDB_TRACE("VCPU{} MC READ FOUND VALUE", _id)

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
											RDB_TRACE("VCPU{} MC CONTINUE SEARCH", _id)
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
								RDB_TRACE("VCPU{} MC READ BLOOM MISS", _id)
							}
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
		// 						RDB_TRACE("VCPU{} MC READ VALUE REMOVED", _id)
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
		// 						RDB_TRACE("VCPU{} MC READ FOUND VALUE", _id)
		// 						if ((found += _read_entry_impl(instance, type, fields, callback)) == required)
		// 						{
		// 							return true;
		// 						}
		// 						else
		// 						{
		// 							RDB_TRACE("VCPU{} MC CONTINUE SEARCH", _id)
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

		RDB_FMT("VCPU{} MC PAGE {}c <{}>", _id, count, uuid::encode(key, uuid::table_alnum))

		auto& info = RuntimeSchemaReflection::info(_schema);
		if (!info.skeys())
		{
			RDB_WARN("VCPU{} MC PAGE NULL READ <{}>", _id, count, uuid::encode(key, uuid::table_alnum))
			return nullptr;
		}
		else if (count == 0)
		{
			RDB_WARN("VCPU{} MC PAGE ZERO READ <{}>", _id, count, uuid::encode(key, uuid::table_alnum))
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
					RDB_TRACE("VCPU{} MC PAGE SCANNING RMPS", _id)
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
					RDB_TRACE("VCPU{} MC PAGE SEARCHING FLUSH{}", _id, j - 1)

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
			RDB_FMT("VCPU{} MC PAGE SIZE {}b <{}>", _id, result.size(), uuid::encode(key, uuid::table_alnum))
			return result;
		}
	}
	void MemoryCache::read(key_type key, const View& sort, field_bitmap fields, const read_callback& callback) noexcept
	{
		_read_impl(key, sort, fields, &callback);
	}
	bool MemoryCache::exists(key_type key, const View& sort) noexcept
	{
		return _read_impl(key, sort, field_bitmap(), nullptr);
	}

	MemoryCache::write_store::iterator MemoryCache::_create_partition_log_if(write_store& map, key_type key, const View& pkey) noexcept
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
		auto [ it, created ] = map.emplace(
			key,
			std::make_pair(
				View::copy(pkey),
				schema.skeys() ? partition_variant(partition()) : partition_variant(single_slot())
			)
		);
		if (created)
		{
			RDB_FMT("VCPU{} MC CREATE PARTITION <{}>", _id, uuid::encode(key, uuid::table_alnum))
			_logs.log(WriteType::CreatePartition, key, pkey);
		}
		return it;
	}
	MemoryCache::write_store::iterator MemoryCache::_create_partition_if(write_store& map, key_type key, const View& pkey) noexcept
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
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
		return data.insert(sort, vtype, reserve);
	}
	MemoryCache::slot MemoryCache::_create_unsorted_slot(write_store::iterator partition, DataType vtype, std::size_t reserve)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		ptr.reset(SlotDeleter::allocate(vtype, reserve));
		return ptr.get();
	}
	MemoryCache::slot MemoryCache::_create_slot(write_store::iterator partition, const View& sort, DataType vtype, std::size_t reserve)
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
		if (schema.skeys())
			return _create_sorted_slot(partition, sort, vtype, reserve);
		return _create_unsorted_slot(partition, vtype, reserve);
	}

	MemoryCache::slot MemoryCache::_create_sorted_slot(write_store::iterator partition, const View& sort, DataType vtype, std::span<const unsigned char> buffer)
	{
		auto& data = std::get<MemoryCache::partition>(partition->second.second);
		return data.insert(sort, vtype, buffer);
	}
	MemoryCache::slot MemoryCache::_create_unsorted_slot(write_store::iterator partition, DataType vtype, std::span<const unsigned char> buffer)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		ptr.reset(SlotDeleter::allocate(vtype, buffer));
		return ptr.get();
	}
	MemoryCache::slot MemoryCache::_create_slot(write_store::iterator partition, const View& sort, DataType vtype, std::span<const unsigned char> buffer)
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
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
			ptr->buffer().data(),
			ptr->buffer().data(),
			ptr->size
		);
		data.insert(sort, nptr);

		return nptr;
	}
	MemoryCache::slot MemoryCache::_resize_unsorted_slot(write_store::iterator partition, std::size_t size)
	{
		auto& ptr = std::get<single_slot>(partition->second.second);
		auto* buffer = SlotDeleter::allocate(ptr->vtype, ptr->size + size);

		std::memcpy(
			buffer->buffer().data(),
			ptr->buffer().data(),
			ptr->size
		);
		ptr.reset(buffer);

		return ptr.get();
	}
	MemoryCache::slot MemoryCache::_resize_slot(write_store::iterator partition, const View& sort, std::size_t size)
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
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
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
		if (schema.skeys())
			return _find_sorted_slot(partition, sort);
		return _find_unsorted_slot(partition);
	}

	void MemoryCache::_write_impl(write_store::iterator partition, WriteType type, const View& sort, std::span<const unsigned char> data) noexcept
	{
		if (type == WriteType::Table)
		{
			RuntimeSchemaReflection::RTSI& info =
				RuntimeSchemaReflection::info(_schema);

			const auto plen = info.prefix_length(data.data());
			auto prefix = View::copy(plen);
			info.prefix(data.data(), View::view(prefix));

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
				std::terminate();
			}
		}
		else if (type == WriteType::Field)
		{
			if (slot->vtype == DataType::SchemaInstance)
			{
				RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
				if (const auto size = schema.fwapply(
						slot->buffer().data(),
						data[0], View::view(data.subspan(1)),
						slot->size
					); size > slot->size)
				{
					_push_bytes(size - slot->size);
					_resize_slot(partition, sort, size);
					schema.fwapply(
						slot->buffer().data() + 1,
						data[0], View::view(data.subspan(1)),
						~0ull
					);
				}
			}
			else
			{
				// if (slot.data()[0] == data[0])
				// {
				// 	RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
				// 	storage = _make_shared_buffer(
				// 		type, data, schema.reflect(data[0]).alignment()
				// 	);
				// }
				// else
				// {
				// 	_merge_field_buffers(type, storage, data);
				// 	_pressure += data.size() + sizeof(key_type) + 32;
				// }
			}
		}
		else if (type == WriteType::WProc)
		{
			std::terminate();
			// RuntimeSchemaReflection::RTSI& info =
			// 	RuntimeSchemaReflection::info(_schema);
			// RuntimeInterfaceReflection::RTII& finfo =
			// 	info.reflect(data[0]);

			// if (finfo.fragmented())
			// {

			// }
			// else
			// {
			// 	if (wtype == WriteType::Remov)
			// 	{
			// 		return;
			// 	}
			// 	else if (wtype == WriteType::Table)
			// 	{
			// 		if (const auto size = info.wpapply(
			// 				storage.data().data() + 1,
			// 				data[0], data[1], View::view(data.subspan(2)),
			// 				storage.size()
			// 			); size > storage.data().size())
			// 		{
			// 			storage.resize(size);
			// 			info.wpapply(
			// 				storage.data().data() + 1,
			// 				data[0], data[1], View::view(data.subspan(2)),
			// 				~0ull
			// 			);
			// 		}
			// 	}
			// 	else if (wtype == WriteType::Field)
			// 	{
			// 		if (data[0] == storage.data()[0])
			// 		{
			// 			// RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
			// 			// const auto type = schema.reflect(data[0]).wproc(
			// 			// 	storage.data().subspan(1).data(),
			// 			// 	data[1],
			// 			// 	View::view(data.subspan(2)),
			// 			// 	wproc_query::Type
			// 			// );
			// 			// if (type == wproc_type::Dynamic)
			// 			// {
			// 			// 	const auto req = schema.reflect(data[0]).wproc(
			// 			// 		storage.data().subspan(1).data(),
			// 			// 		data[1],
			// 			// 		View::view(data.subspan(2)),
			// 			// 		wproc_query::Storage
			// 			// 	);
			// 			// 	if (req > storage.size())
			// 			// 		storage.resize(req);
			// 			// }
			// 			// schema.reflect(data[0]).wproc(
			// 			// 	storage.data().subspan(1).data(),
			// 			// 	data[1],
			// 			// 	View::view(data.subspan(2)),
			// 			// 	wproc_query::Commit
			// 			// );
			// 		}
			// 		else
			// 		{

			// 		}
			// 	}
			// 	else
			// 	{
			// 		// field_bitmap fields{};
			// 		// fields.set(data[0]);
			// 		// read(partition->first, sort, fields, [&](std::size_t, View view) {
			// 		// 	const auto field = info.reflect(data[0]);

			// 		// 	write()
			// 		// });
			// 	}
			// }
		}
	}
	void MemoryCache::_reset_impl(write_store::iterator partition, const View& sort) noexcept
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
		auto* slot = _create_slot(partition, sort, DataType::SchemaInstance, schema.cstorage(sort));
		schema.construct(slot->buffer().data(), sort);
		_pressure += slot->size + sizeof(key_type) + 16;
	}
	void MemoryCache::_remove_impl(write_store::iterator partition, const View& sort) noexcept
	{
		_pressure += sizeof(key_type) + 24;
		_create_slot(partition, View::view(sort), DataType::Tombstone, 0);
	}

	void MemoryCache::write(WriteType type, key_type key, const View& partition, const View& sort, std::span<const unsigned char> data) noexcept
	{
		RDB_FMT("VCPU{} MC WRITE <{}> {}b", _id, uuid::encode(key, uuid::table_alnum), data.size())
		RuntimeSchemaReflection::RTSI& schema
			= RuntimeSchemaReflection::info(_schema);
		const auto part = _create_partition_log_if(*_map, key, partition);
		_logs.log(type, key, sort, View::view(data));
		_write_impl(part, type, sort, data);
		_flush_if();
	}
	void MemoryCache::reset(key_type key, const View& partition, const View& sort) noexcept
	{
		RDB_FMT("VCPU{} MC RESET <{}>", _id, uuid::encode(key, uuid::table_alnum))
		const auto part = _create_partition_log_if(*_map, key, partition);
		_logs.log(WriteType::Reset, key, sort);
		_reset_impl(part, sort);
		_flush_if();
	}
	void MemoryCache::remove(key_type key, const View& sort) noexcept
	{
		RDB_FMT("VCPU{} MC REMOVE <{}>", _id, uuid::encode(key, uuid::table_alnum))
		_logs.log(WriteType::Remov, key, sort);
		_remove_impl(
			_create_partition_log_if(*_map, key, sort),
			sort
		);
		_flush_if();
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

		if (_cfg->cache.partition_bloom_fp_rate == 1.f)
			return;

		const auto prob = _cfg->cache.partition_bloom_fp_rate;
		const auto prob_conv = static_cast<std::uint16_t>(prob * 10'000);
		const auto bits = _bloom_bits(map.size(), prob);

		RDB_FMT("VCPU{} MC FLUSH{} BEGIN BLOOM WRITE {}bits", _id, id, bits)

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

		RDB_FMT("VCPU{} MC FLUSH{} END BLOOM WRITE {}b", _id, id, bloom.size())
	}

	std::size_t MemoryCache::_bloom_intra_partition_begin_impl(write_store::const_iterator part, Mapper& bloom, int id) noexcept
	{
		if (_cfg->cache.intra_partition_bloom_fp_rate == 1.f)
			return 0;

		const auto size = std::get<partition>(part->second.second).size();
		const auto prob = _cfg->cache.intra_partition_bloom_fp_rate;
		const auto prob_conv = static_cast<std::uint16_t>(prob * 10'000);
		const auto bits = _bloom_bits(size, prob);

		RDB_FMT("VCPU{} MC FLUSH{} BEGIN PARTITION BLOOM WRITE {}bits", _id, id, bits)

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
		if (_cfg->cache.intra_partition_bloom_fp_rate == 1.f)
			return;
		bloom.vmap_increment((bits + 7) / 8);
		RDB_FMT("VCPU{} MC FLUSH{} END BLOOM WRITE {}b", _id, id, bloom.size())
	}
	void MemoryCache::_data_impl(const write_store& map, Mapper& data, Mapper& indexer, Mapper& bloom, int id) noexcept
	{
		const auto amortized_block_size = static_cast<std::size_t>(_cfg->cache.block_size * 1.2);

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

		RDB_FMT("VCPU{} MC FLUSH{} BEGIN DATA WRITE", _id, id)
		RDB_FMT("VCPU{} MC FLUSH{} BEGIN INDEXER WRITE {}", _id, id, map.size())

		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
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
			RDB_TRACE("VCPU{} MC FLUSH{} SORTING KEYS", _id, id)
			offsets.resize(map.size());
			std::transform(map.begin(), map.end(), offsets.begin(), [](const auto& it) { return it.first; });
			std::sort(offsets.begin(), offsets.end());
		}
		// Metadata
		std::size_t idxoff = 0;
		{
			RDB_TRACE("VCPU{} MC FLUSH{} WRITING METADATA", _id, id)
			idxoff += byte::swrite<key_type>(indexer.memory(), idxoff, offsets.back());
			idxoff += byte::swrite<std::uint32_t>(indexer.memory(), idxoff, map.size());
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), version));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _cfg->cache.block_sparse_index_ratio));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _cfg->cache.partition_sparse_index_ratio));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _cfg->cache.block_size));
		}
		// Stream blocks
		{
			const bool dynamic_skey = !info.static_prefix();
			bool is_begin = true;
			std::size_t blocks = 0;
			std::size_t prefix = 0;
			std::size_t start = data.size();
			std::size_t bloom_offset = 0;
			std::size_t bloom_bits = 0;
			std::size_t i = 0;
			std::vector<std::pair<key_type, std::uint32_t>> indices{};
			std::vector<std::pair<View, std::uint32_t>> sort_block_indices{};
			std::vector<std::pair<View, std::uint32_t>> sort_indices{};

			if (keys && !dynamic_skey)
				prefix = info.static_prefix();
			else
				indices.reserve((offsets.size() / _cfg->cache.block_sparse_index_ratio) / 2);

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

				RDB_TRACE("VCPU{} MC FLUSH{} EMITTING BLOCK", _id, id)
				RDB_WARN_IF(
					source.fragments() >= frag_pool.size(),
					"VCPU{} MC FLUSH{} HIGH FRAGMENTATION: {}", _id, id, source.fragments()
				)

				const auto saved_zero_index = keys ? View::view(sort_indices[0].first) : View();

				source.flush();
				// Index
				{
					RDB_TRACE("VCPU{} MC FLUSH{} WRITING INDEX", _id, id)
					data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), source.digest()));
					if (keys)
					{
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), prefix));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_indices.size()));
						for (decltype(auto) it : sort_indices)
						{
							data.vmap_increment(byte::swrite(data.append(), it.first));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.second));
						}
						sort_indices.clear();
					}
					else
					{
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), indices.size()));
						for (decltype(auto) it : indices)
						{
							data.vmap_increment(byte::swrite<key_type>(data.append(), it.first));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.second));
						}
						indices.clear();
					}
				}
				// Compress and write (+write block index)
				RDB_FMT("VCPU{} MC FLUSH{} WRITE BLOCK{} {}b", _id, id, blocks++, source.size())
				{
					const auto psize = source.size();
					StaticBufferSink sink(psize, compressed_block_pool);
					snappy::Compress(&source, &sink);
					const auto compsize = sink.size();
					RDB_FMT("VCPU{} MC FLUSH{} WRITE BLOCK{} COMPRESSION RATIO {}%", _id, id, blocks - 1, (std::round((float(compsize) / psize) * 100) / 100) * 100)

					if (compsize / psize < _cfg->cache.compression_ratio)
					{
						RDB_FMT("VCPU{} MC FLUSH{} WRITE BLOCK{} COMPRESSED", _id, id, blocks - 1)
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sink.size()));
						data.vmap_increment(byte::swrite(data.append(), sink.data()));
					}
					else
					{
						RDB_FMT("VCPU{} MC FLUSH{} WRITE BLOCK{} RAW", _id, id, blocks - 1)
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite(data.append(), source.block()));
					}

					source.clear();
					sink.clear();

					if (keys)
					{
						if (is_begin)
						{
							byte::swrite(data.memory().subspan(start), data.size() - start - sizeof(std::uint64_t));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), prefix));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_block_indices.size()));
							data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), bloom_offset));
							for (decltype(auto) it : sort_block_indices)
							{
								data.vmap_increment(byte::swrite(data.append(), it.first));
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.second));
							}
							sort_block_indices.clear();
						}
						else if (blocks % _cfg->cache.block_sparse_index_ratio == 0)
						{
							sort_block_indices.push_back({
								saved_zero_index, start
							});
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
						if (j % _cfg->cache.partition_sparse_index_ratio == 0)
						{
							if (dynamic_skey)
							{
								// Perform prefix calculations
							}
							sort_indices.push_back({
								View::view(key),
								source.size()
							});
						}

						if (data->vtype == DataType::SchemaInstance) source.push({ .data = buffer });
						else source.push({ .key = key, .data = buffer });

						if (source.size() >= _cfg->cache.block_size)
						{
							RDB_TRACE("VCPU{} MC FLUSH{} BLOCK{} PRESSURE REACHED {}", _id, id, blocks, source.size())
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
					for (; i < offsets.size(); i++)
					{
						const auto& [ pkey, pdata ] = map.at(offsets[i]);
						const auto buffer = std::get<single_slot>(pdata)->flush_buffer();
						source.push({ .data = pkey });
						if (!buffer.empty())
						{
							if (i % _cfg->cache.block_sparse_index_ratio == 0)
							{
								indices.push_back({
									offsets[i],
									source.size()
								});
							}
							source.push({ .data = byte::tspan(offsets[i]) });
						}
						source.push({ .data = buffer });
						if (source.size() > _cfg->cache.block_size)
							break;
					}
					index();
					write_block();
				}
			}
		}

		RDB_FMT("VCPU{} MC FLUSH{} END INDEXER WRITE {}b", _id, id, indexer.size())
		RDB_FMT("VCPU{} MC FLUSH{} END DATA WRITE {}b", _id, id, data.size())
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
		if (_pressure > _cfg->cache.flush_pressure)
			flush();
	}

	void MemoryCache::flush() noexcept
	{
		if (_map->empty())
			return;

		++_flush_running;
		_readonly_maps.push_back(_map);
		_logs.snapshot(_flush_id);
		_handle_reserve();
		RDB_FMT("VCPU{} MC FLUSH{} BEGIN {}b", _id, _flush_id.load(), _pressure)
		std::thread([map = _map, this, id = _flush_id++]() {
			_flush_impl(*map, id);
			_logs.mark(id);
			RDB_FMT("VCPU{} MC FLUSH{} END", _id, id)
			_handle_cache[id].unlocked.store(true, std::memory_order::release);
			--_flush_running;
			_flush_running.notify_all();
		}).detach();

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
