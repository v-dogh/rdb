#include "rdb_memory.hpp"
#include "rdb_locale.hpp"
#include "rdb_dbg.hpp"
#include "rdb_memunits.hpp"
#include "rdb_reflect.hpp"
#include "rdb_version.hpp"
#include <cmath>

namespace rdb
{
	void MemoryCache::_move(MemoryCache&& copy) noexcept
	{
		while (copy._flush_running.load() != 0)
			copy._flush_running.wait(copy._flush_running.load());
		_path = std::move(copy._path);
		_map = std::move(copy._map);
		_qcache = std::move(copy._qcache);
		_logs = std::move(copy._logs);
		_flush_id = copy._flush_id.load();
		_cfg = copy._cfg;
		_id = copy._id;
		_pressure = copy._pressure;
		_schema = copy._schema;
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
				if (type == WriteType::Reset)
				{
					RDB_FMT("VCPU{} MCR RESET <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_reset_impl(*_map, key, sort);
					_flush_if();
				}
				else if (type == WriteType::Remov)
				{
					RDB_FMT("VCPU{} MCR REMOVE <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_remove_impl(*_map, key, sort);
					_flush_if();
				}
				else
				{
					RDB_FMT("VCPU{} MCR WRITE <{}>", _id, uuid::encode(key, uuid::table_alnum))
					_write_impl(*_map, type, key, sort, data);
					_flush_if();
				}
			});
		}
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
		constexpr auto map_cost = 2;
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
			_mappings -= 2;
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
			_mappings -= 2 * was_mapped;
			_descriptors -= 3;
		}
	}

	MemoryCache::slot& MemoryCache::_emplace_sorted_slot(write_store& map, hash_type key, View sort) noexcept
	{
		return std::get<partition>(
			map.emplace(key, _make_partition()).first->second
		).emplace(View::copy(std::move(sort)), slot()).first->second;
	}
	MemoryCache::slot& MemoryCache::_emplace_unsorted_slot(write_store& map, hash_type key) noexcept
	{
		return std::get<slot>(
			map.emplace(key, slot()).first->second
		);
	}
	MemoryCache::slot& MemoryCache::_emplace_slot(write_store& map, hash_type key, View sort) noexcept
	{
		RuntimeSchemaReflection::RTSI& info = RuntimeSchemaReflection::info(_schema);
		if (info.skeys())
			return _emplace_sorted_slot(map, key, View::view(sort));
		return _emplace_unsorted_slot(map, key);
	}

	const MemoryCache::slot* MemoryCache::_find_sorted_slot(const write_store& map, hash_type key, View sort) noexcept
	{
		auto f = map.find(key);
		if (f == map.end())
			return nullptr;
		auto fp = std::get<partition>(f->second).find(View::view(sort));
		if (fp == std::get<partition>(f->second).end())
			return nullptr;
		return &fp->second;
	}
	const MemoryCache::slot* MemoryCache::_find_unsorted_slot(const write_store& map, hash_type key) noexcept
	{
		auto f = map.find(key);
		if (f == map.end())
			return nullptr;
		return &std::get<slot>(f->second);
	}
	const MemoryCache::slot* MemoryCache::_find_slot(const write_store& map, hash_type key, View sort) noexcept
	{
		RuntimeSchemaReflection::RTSI& info = RuntimeSchemaReflection::info(_schema);
		if (info.skeys())
			return _find_sorted_slot(map, key, View::view(sort));
		return _find_unsorted_slot(map, key);
	}

	std::size_t MemoryCache::_read_entry_size_impl(View view) noexcept
	{
		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
		if (view.data()[0] == char(DataType::FieldSequence))
		{
			std::size_t off = 1;
			auto cnt = view.data()[off++];
			while (cnt--)
			{
				RuntimeInterfaceReflection::RTII& field =
					info.reflect(view.data()[off++]);
				off += field.storage(view.data().data() + off);
			}
			return off;
		}
		else if (view.data()[0] == char(DataType::SchemaInstance))
		{
			return info.storage(view.data().data() + 1);
		}
		else
			return sizeof(DataType);
	}
	View MemoryCache::_read_entry_impl(View view, std::size_t field) noexcept
	{
		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
		if (view.data()[0] == char(DataType::FieldSequence))
		{
			RuntimeInterfaceReflection::RTII& ffield =
				info.reflect(field);

			std::size_t off = 1;
			while (view.data()[off++] != field &&
				   off < view.size())
			{
				RuntimeInterfaceReflection::RTII& field =
					info.reflect(view.data()[off]);
				off += field.storage(view.data().data() + off);
			}

			if (off < view.size())
			{
				return View::copy(std::span(
					view.data().subspan(
						off,
						ffield.storage(view.data().data() + off)
					)
				));
			}
		}
		else if (view.data()[0] == char(DataType::SchemaInstance))
		{
			return View::copy(
				info.cfield(view.data().data() + 1, field)
			);
		}
		return nullptr;
	}
	View MemoryCache::_read_cache_impl(const write_store& map, hash_type key, View sort, std::size_t field) noexcept
	{
		auto f = _find_slot(map, key, sort);
		if (f == nullptr)
			return nullptr;
		return _read_entry_impl(View::view(f->second.data()), field);
	}

	View MemoryCache::read(hash_type key, View sort, unsigned char fields) noexcept
	{
		RDB_FMT("VCPU{} MC READ <{}>", _id, uuid::encode(key, uuid::table_alnum))

		// Search cache
		{			
			if (auto result = _read_cache_impl(
					*_map, key, View::view(sort), field
				); result != nullptr)
			{
				return result;
			}
			else if (_flush_running)
			{
				for (auto it = _readonly_maps.rbegin(); it != _readonly_maps.rend(); ++it)
				{
					if (const auto lock = it->lock(); lock != nullptr)
					{
						if (auto result = _read_cache_impl(
								*lock, key, View::view(sort), field
							); result != nullptr)
						{
							return result;
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

			RuntimeSchemaReflection::RTSI& info =
				RuntimeSchemaReflection::info(_schema);

			for (std::size_t j = _flush_id; j > 0; j--)
			{
				RDB_TRACE("VCPU{} MC READ SEARCHING FLUSH{}", _id, j - 1)

				const auto i = j - 1;

				auto& handle = _handle_open(i);
				auto& [ data, indexer, bloom, _1, _2 ] = handle;

				if (handle.ready() && _bloom_may_contain(key, &handle))
				{
					// Search the indexer
					std::uint64_t offset = ~0ull;
					{
						std::size_t off = 0;
						const auto max_key = byte::sread<key_type>(indexer.memory(), off);
						const auto size = byte::sread<std::uint32_t>(indexer.memory(), off);

						if (key > max_key)
							continue;

						if (const auto result = byte::search_partition<key_type, std::uint64_t>(
								key, indexer.memory().subspan(off), size
							); result.has_value())
						{
							offset = result.value();
						}
						else
							continue;
						RDB_TRACE("VCPU{} MC READ FOUND BLOCK", _id)
					}

					// Search data
					{
						std::size_t off = 0;

						// Metadata

						RDB_TRACE("VCPU{} MC READ READING METADATA", _id)

						/*const auto version = */byte::sread<std::uint64_t>(data.memory(), off);
						/*const auto index_ratio = */byte::sread<std::uint64_t>(data.memory(), off);
						/*const auto block_size = */byte::sread<std::uint64_t>(data.memory(), off);

						off = offset;

						// Wide partition
						if (info.skeys())
						{
							RDB_TRACE("VCPU{} MC READ SEARCHING IN PARTITION", _id)

							const auto comparator = ThreewaySortKeyComparator(_schema);
							const auto eq_comparator = EqualityKeyComparator(_schema);
							const auto partition_size = byte::sread<std::uint64_t>(data.memory(), off);

							std::size_t partition_footer_off = off + partition_size;

							const auto prefix = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);
							const auto sparse_block_indices = byte::sread<std::uint32_t>(data.memory(), partition_footer_off);

							const auto sparse_block_offset = sparse_block_indices ? byte::search_partition_binary<std::uint32_t>(
								sort.data(), data.memory().subspan(partition_footer_off), sparse_block_indices, comparator, true
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

									const auto result_min = comparator(View::view(sort), min_key);
									const auto result_max = comparator(View::view(sort), max_key);

									if (result_min <= 0 && result_max >= 0)
									{
										RDB_TRACE("VCPU{} MC READ FOUND MATCHING BLOCK", _id)

										const auto sparse_offset = byte::search_partition_binary<std::uint32_t>(
											sort.data(), data.memory().subspan(saved_off), index_count, comparator, true
										);

										if (sparse_offset.has_value())
										{
											SharedBufferSink sink = (decompressed == compressed) ?
												SharedBufferSink() : SharedBufferSink(decompressed);
											SourceView source(data.memory().subspan(off, compressed));
											std::span<const unsigned char> block;

											if (decompressed != compressed)
											{
												snappy::Uncompress(&source, &sink);
												block = sink.data();
												data.close();
											}
											else
											{
												block = data.memory().subspan(off, decompressed);
											}

											RDB_TRACE("VCPU{} MC READ LINEAR BLOCK SEARCH", _id)

											off = sparse_offset.value();
											do
											{
												const auto [ eq, size ] = eq_comparator(View::view(sort), View::view(block.subspan(off)));
												off += size;
												if (eq)
												{
													RDB_TRACE("VCPU{} MC READ FOUND VALUE", _id)

													if (block[off] == char(DataType::Tombstone))
														return nullptr;

													if (const auto result = _read_entry_impl(
														View::view(block.subspan(off)),
														field
													); result != nullptr)
													{
														return result;
													}
													else
													{
														RDB_TRACE("VCPU{} MC READ FIELD MISS", _id)
														if (block[off] == char(DataType::SchemaInstance))
														{
															return nullptr;
														}
														else
														{
															break;
														}
													}
												}
												else
												{
													off += _read_entry_size_impl(
														View::view(block.subspan(off))
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

								SharedBufferSink sink = (decompressed == compressed) ?
									SharedBufferSink() : SharedBufferSink(decompressed);
								SourceView source(data.memory().subspan(off, compressed));
								std::span<const unsigned char> block;

								if (decompressed != compressed)
								{
									snappy::Uncompress(&source, &sink);
									block = sink.data();
									data.close();
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

										if (block[off] == char(DataType::Tombstone))
											return nullptr;

										if (const auto result = _read_entry_impl(
											View::view(block.subspan(off)),
											field
										); result != nullptr)
										{
											return result;
										}
										else
										{
											RDB_TRACE("VCPU{} MC READ FIELD MISS", _id)
											if (block[off] == char(DataType::SchemaInstance))
											{
												return nullptr;
											}
											else
											{
												break;
											}
										}
									}
									else
									{
										off += _read_entry_size_impl(
											View::view(block.subspan(off))
										);
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
		return nullptr;
	}

	SharedBuffer MemoryCache::_make_shared_buffer(WriteType type, std::span<const unsigned char> data, std::size_t alignment) noexcept
	{
		DataType dtype = DataType::Tombstone;

		if (type == WriteType::Field)
		{
			SharedBuffer buffer(data.size() + sizeof(WriteType) + sizeof(std::uint8_t), alignment);
			buffer.data()[0] = char(dtype);
			buffer.data()[1] = 1;
			std::memcpy(buffer.data().data() + 2, data.data(), data.size());
			return buffer;
		}
		else if (type == WriteType::Table) dtype = DataType::SchemaInstance;
		else if (type == WriteType::Remov) dtype = DataType::Tombstone;
		else
		{
			return SharedBuffer(data, alignment);
		}

		SharedBuffer buffer(data.size() + sizeof(WriteType), alignment);
		buffer.data()[0] = char(dtype);
		std::memcpy(buffer.data().data() + 1, data.data(), data.size());
		return buffer;
	}
	void MemoryCache::_merge_field_buffers(WriteType type, SharedBuffer buffer, std::span<const unsigned char> data) noexcept
	{
		const auto off = buffer.size();
		buffer.resize(
			buffer.size() + data.size(),
			data.size()
		);
		std::memcpy(
			buffer.data().data() + off,
			data.data(), data.size()
		);
		buffer.data()[1]++;
	}

	void MemoryCache::_write_impl(write_store& map, WriteType type, hash_type key, View sort, std::span<const unsigned char> data) noexcept
	{
		auto& [ wtype, storage ] = _emplace_slot(map, key, View::view(sort));
		if (storage == nullptr)
		{
			if (type == WriteType::Table)
			{
				RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
				storage = _make_shared_buffer(type, data, schema.alignment());
				wtype = type;
			}
			else
			{
				storage = _make_shared_buffer(type, data);
				wtype = type;
			}
			_pressure += data.size() + sizeof(key) + 32;
		}
		else if (type == WriteType::Table)
		{
			RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
			storage = _make_shared_buffer(type, data, schema.alignment());
			wtype = type;
		}
		else if (type == WriteType::Field)
		{
			if (wtype == WriteType::Table)
			{
				RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
				if (const auto size = schema.fwapply(
						storage.data().data(),
						data[0], View::view(data.subspan(1)),
						storage.size()
					); size > storage.size())
				{
					storage.resize(size);
				}
				schema.fwapply(
					storage.data().data(),
					data[0], View::view(data.subspan(1)),
					~0ull
				);
			}
			else
			{
				if (storage.data()[0] == data[0])
				{
					RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
					storage = _make_shared_buffer(
						type, data, schema.reflect(data[0]).alignment()
					);
				}
				else
				{
					_merge_field_buffers(type, storage, data);
					_pressure += data.size() + sizeof(key) + 32;
				}
			}
		}
		else if (type == WriteType::WProc)
		{
			// if (f->second.first == WriteType::Remov)
			// {
			// 	return;
			// }
			// else if (f->second.first == WriteType::Table)
			// {
			// 	RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
			// 	if (const auto size = schema.wpapply(
			// 			f->second.second.data().data(),
			// 			data[0], data[1], View::view(data.subspan(2)),
			// 			f->second.second.size()
			// 		); size > f->second.second.data().size())
			// 	{
			// 		f->second.second.resize(size);
			// 	}
			// 	schema.wpapply(
			// 		f->second.second.data().data(),
			// 		data[0], data[1], View::view(data.subspan(2)),
			// 		~0ull
			// 	);
			// }
			// else if (f->second.first == WriteType::Field)
			// {
			// 	if (data[0] == f->second.second.data()[0])
			// 	{
			// 		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
			// 		const auto type = schema.reflect(data[0]).wproc(
			// 			f->second.second.data().subspan(1).data(),
			// 			data[1],
			// 			View::view(data.subspan(2)),
			// 			wproc_query::Type
			// 		);
			// 		if (type == wproc_type::Dynamic)
			// 		{
			// 			const auto req = schema.reflect(data[0]).wproc(
			// 				f->second.second.data().subspan(1).data(),
			// 				data[1],
			// 				View::view(data.subspan(2)),
			// 				wproc_query::Storage
			// 			);
			// 			if (req > f->second.second.size())
			// 				f->second.second.resize(req);
			// 		}
			// 		schema.reflect(data[0]).wproc(
			// 			f->second.second.data().subspan(1).data(),
			// 			data[1],
			// 			View::view(data.subspan(2)),
			// 			wproc_query::Commit
			// 		);
			// 	}
			// 	else
			// 	{

			// 	}
			// }
		}
	}
	void MemoryCache::_reset_impl(write_store& map, hash_type key, View sort) noexcept
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);
		SharedBuffer buffer(schema.cstorage() + 1, schema.alignment());
		buffer.data()[0] = char(DataType::SchemaInstance);
		schema.construct(buffer.data().data() + 1);
		_pressure += buffer.size() + sizeof(key) + 16;
		_emplace_slot(map, key, View::view(sort)) = slot(WriteType::Table, buffer);
	}
	void MemoryCache::_remove_impl(write_store& map, hash_type key, View sort) noexcept
	{
		_pressure += sizeof(key) + 24;
		_emplace_slot(map, key, View::view(sort)) = slot(WriteType::Remov, nullptr);
	}

	void MemoryCache::write(WriteType type, hash_type key, View sort, std::span<const unsigned char> data) noexcept
	{
		RDB_FMT("VCPU{} MC WRITE <{}> {}b", _id, uuid::encode(key, uuid::table_alnum), data.size())
		_logs.log(type, key, sort, View::view(data));
		_write_impl(*_map, type, key, sort, data);
		_flush_if();
	}
	void MemoryCache::reset(hash_type key, View sort) noexcept
	{
		RDB_FMT("VCPU{} MC RESET <{}>", _id, uuid::encode(key, uuid::table_alnum))
		_logs.log(WriteType::Reset, key, sort);
		_reset_impl(*_map, key, sort);
		_flush_if();
	}
	void MemoryCache::remove(hash_type key, View sort) noexcept
	{
		RDB_FMT("VCPU{} MC REMOVE <{}>", _id, uuid::encode(key, uuid::table_alnum))
		_logs.log(WriteType::Remov, key, sort);
		_remove_impl(*_map, key, sort);
		_flush_if();
	}

	bool MemoryCache::_bloom_may_contain(key_type key, FlushHandle* handle) const noexcept
	{
		auto& [ data, indexer, bloom, ready, _ ] = *handle;

		std::size_t off = 1;
		const auto size = byte::sread<std::uint32_t>(bloom.read(0, sizeof(std::uint32_t)).data(), off);
		const auto bits = _bloom_bits(size);
		const auto hashes = _bloom_hashes(bits, size);

		const auto [ k1, k2 ] = _hash_pair(key);
		for (std::size_t i = 0; i < hashes; i++)
		{
			const auto idx = (k1 + (i * k2)) % bits;
			const auto quot = idx >> 3;
			const auto rem = idx & 7;
			if (bloom.read(off + quot) &
				std::uint8_t(1) << rem)
				return true;
		}

		return false;
	}
	std::size_t MemoryCache::_bloom_bits(std::size_t keys) const noexcept
	{
		constexpr float prob = 0.01f;
		const float nkeys = keys;
		const float l2 = std::log(2.f);
		return static_cast<std::size_t>((-nkeys * std::log(prob)) / (l2 * l2));
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

	void MemoryCache::_bloom_impl(const write_store& map, const std::filesystem::path& base, int id) noexcept
	{
		// [ uint8(flag,blooms) | [ uint32(key-count) | ... ] ... ]
		// Right now we store three blooms
		// 1. PK - partition key bloom
		// 2. PK-F - parition key + field bloom
		// 3. PK-SK - partition key + sort key bloom (full sort key)

		RuntimeSchemaReflection::RTSI& info =
			RuntimeSchemaReflection::info(_schema);
		const auto keys = info.skeys();
		const auto cnt = (2 + bool(keys));

		const auto bits = _bloom_bits(map.size());
		const auto hashes = _bloom_hashes(bits, map.size());

		RDB_FMT("VCPU{} MC FLUSH{} BEGIN BLOOM WRITE {}bits", _id, id, bits)

		Mapper bloom;
		bloom.open(
			base/"filter.blx",
			sizeof(std::uint8_t) + (sizeof(std::uint32_t) + ((bits / 8) + 1)) * cnt
		);
		bloom.map();
		bloom.hint(Mapper::Access::Random);
		bloom.hint(Mapper::Access::Hot);

		std::size_t off = 0;
		off += byte::swrite<std::uint8_t>(bloom.memory(), off, BloomType::PK | BloomType::PK_F | (keys ? BloomType::PK_SK : 0x00));
		off += byte::swrite<std::uint32_t>(bloom.memory(), off, map.size());
		for (decltype(auto) key : map)
		{
			const auto [ k1, k2 ] = _hash_pair(key.first);
			for (std::size_t i = 0; i < hashes; i++)
			{
				const auto idx = (k1 + (i * k2)) % bits;
				const auto quot = idx >> 3;
				const auto rem = idx & 7;
				bloom.memory()[off + quot] |=
					std::uint8_t(1) << rem;
			}
		}

		RDB_FMT("VCPU{} MC FLUSH{} END BLOOM WRITE {}b", _id, id, bloom.size())

		bloom.flush();
		bloom.close();
	}
	void MemoryCache::_data_impl(const write_store& map, const std::filesystem::path& base, int id) noexcept
	{		
		thread_local std::array<std::span<const unsigned char>, 512> frag_pool_data;
		thread_local std::array<unsigned char, mem::KiB(32)> block_pool_data;

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

		Mapper indexer;
		Mapper data;

		indexer.open(
			base/"indexer.idx",
			sizeof(key_type) + sizeof(std::uint32_t) + (sizeof(key_type) + sizeof(std::uint64_t)) * map.size()
		);
		data.open(
			base/"data.dat"
		);

		indexer.map();
		data.vmap();
		indexer.hint(Mapper::Access::Sequential);
		data.hint(Mapper::Access::Sequential);

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
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _cfg->cache.sparse_index_ratio));
			data.vmap_increment(byte::swrite<std::uint64_t>(data.append(), _cfg->cache.block_size));
		}
		// Stream blocks
		{
			bool dynamic_skey = false;
			std::size_t blocks = 0;
			std::size_t prefix = 0;
			std::vector<std::pair<key_type, std::uint32_t>> indices{};
			std::vector<std::pair<View, std::uint32_t>> sort_block_indices{};
			std::vector<std::pair<View, std::uint32_t>> sort_indices{};
			partition::const_iterator begin = partition::const_iterator();
			partition::const_iterator value = partition::const_iterator();
			partition::const_iterator end = partition::const_iterator();

			if (keys)
			{
				for (std::size_t i = 0; i < keys; i++)
				{
					auto& key = info.reflect_skey(i);
					if (key.dynamic())
					{
						dynamic_skey = true;
						break;
					}
					else
						prefix += key.sstorage();
				}
			}
			else
				indices.reserve((offsets.size() / _cfg->cache.sparse_index_ratio) / 2);

			SourceMultiplexer source(block_pool_data, frag_pool_data);
			for (std::size_t i = 0; i < offsets.size(); i++)
			{
				// Gather range

				std::size_t start = data.size();
				std::size_t size = 0;
				std::size_t j = i;

				if (keys)
				{
					if (value == partition::const_iterator())
					{
						const auto& part = std::get<partition>(map.at(
							offsets[j]
						));
						begin = part.begin();
						value = part.begin();
						end = part.end();
						// Partition size (reserve)
						data.vmap_increment(sizeof(std::uint64_t));
					}

					for (; value != end; ++value)
					{
						const auto& buffer = value->second.second;
						size += buffer.size();

						if (j % _cfg->cache.sparse_index_ratio == 0)
						{
							if (dynamic_skey)
							{
								// Perform prefix calculations
							}
							sort_indices.push_back({
								View::view(value->first),
								source.size()
							});
						}
						source.push(value->first);
						source.push(buffer.data());

						if (size > _cfg->cache.block_size)
							break;
					}

					if (value == end)
					{
						value = partition::const_iterator();
						j++;
					}
				}
				else
				{
					for (; j < offsets.size(); j++)
					{
						const auto& buffer = std::get<slot>(map.at(
							offsets[j]
						)).second;
						size += buffer.size();

						if (j % _cfg->cache.sparse_index_ratio == 0)
						{
							indices.push_back({
								offsets[j],
								source.size()
							});
						}
						source.push(byte::tspan(offsets[j]));
						source.push(buffer.data());

						if (size > _cfg->cache.block_size)
							break;
					}
				}

				RDB_TRACE("VCPU{} MC FLUSH{} EMITTING BLOCK", _id, id)
				RDB_WARN_IF(
					source.fragments() >= frag_pool_data.size(),
					"VCPU{} MC FLUSH{} HIGH FRAGMENTATION: {}", _id, id, source.fragments()
				)

				source.flush();
				// Index
				{
					RDB_TRACE("VCPU{} MC FLUSH{} WRITING INDEX", _id, id)

					if (!keys || value == partition::const_iterator())
					{
						idxoff += byte::swrite<key_type>(indexer.memory().subspan(idxoff), offsets[i]);
						idxoff += byte::swrite<std::uint64_t>(indexer.memory().subspan(idxoff), start);
					}

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
					SharedBufferSink sink(0, 0, source.size());
					if (float(snappy::Compress(&source, &sink)) / psize < _cfg->cache.compression_ratio)
					{
						source.clear();
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sink.size()));
						data.vmap_increment(byte::swrite(data.append(), sink.data()));
					}
					else
					{
						sink.clear();
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), psize));
						data.vmap_increment(byte::swrite(data.append(), source.block()));
					}

					if (keys)
					{
						if (i != j)
						{
							byte::swrite(data.memory().subspan(start), data.size() - start - sizeof(std::uint64_t));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), prefix));
							data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), sort_block_indices.size()));
							for (decltype(auto) it : sort_block_indices)
							{
								data.vmap_increment(byte::swrite(data.append(), it.first));
								data.vmap_increment(byte::swrite<std::uint32_t>(data.append(), it.second));
							}
							sort_block_indices.clear();
						}
						else if (blocks++ % _cfg->cache.sparse_index_ratio == 0)
						{
							sort_block_indices.push_back({
								View::view(sort_indices[0].first), start
							});
						}
					}
				}
				source.clear();

				i = j;
			}
		}
		data.vmap_flush();

		RDB_FMT("VCPU{} MC FLUSH{} END INDEXER WRITE {}b", _id, id, indexer.size())
		RDB_FMT("VCPU{} MC FLUSH{} END DATA WRITE {}b", _id, id, data.size())

		indexer.close();
		data.close();
	}
	void MemoryCache::_flush_impl(const write_store& map, int id) noexcept
	{
		std::filesystem::path fpath = _path/"flush"/std::format("f{}", std::to_string(id));
		std::filesystem::create_directory(fpath);

		Mapper lock;
		lock.open(fpath/"lock");

		_bloom_impl(map, fpath, id);
		_data_impl(map, fpath, id);

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
}
