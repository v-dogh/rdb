#ifndef RDB_MEMORY_HPP
#define RDB_MEMORY_HPP

#include <bitset>
#include <unordered_map>
#include <filesystem>
#include <cstring>
#include <span>
#include <map>
#include "rdb_reflect.hpp"
#include "rdb_shared_buffer.hpp"
#include "rdb_root_config.hpp"
#include "rdb_utils.hpp"
#include "rdb_writetype.hpp"
#include "rdb_keytype.hpp"
#include "rdb_log.hpp"

namespace rdb
{
	// Caches queries for specific table instances
	// Works in coordination with the memory cache
	class QueryCache
	{

	};

	// The memory cache is a per-core immutable structure responsible for batching new writes for a schema
	// The mount point is responsible for passing reads/writes to correct memory caches
	// Whenever we receive a new write, we first log it, at the same time we wait until the pressure reaches a critical point (in the MemoryCache)
	// Then we commit all of the data to disk
	class MemoryCache
	{
	public:
		using field_bitmap = std::bitset<256>;
		using WriteType = rdb::WriteType;
		using ReadType = rdb::ReadType;
	private:
		struct FlushHandle
		{
			Mapper data{};
			Mapper indexer{};
			Mapper bloom{};
			std::atomic<bool> unlocked{ false };
			std::size_t idx{ 0 };

			FlushHandle(std::size_t index, bool ready)
				: idx(index), unlocked(ready) {}
			FlushHandle(const FlushHandle&) = delete;
			FlushHandle(FlushHandle&& copy) :
				data(std::move(copy.data)),
				indexer(std::move(copy.indexer)),
				bloom(std::move(copy.bloom)),
				unlocked(copy.unlocked.load(std::memory_order::relaxed)),
				idx(copy.idx)
			{}

			bool ready() const noexcept
			{
				return unlocked.load(std::memory_order::acquire);
			}
		};
		using slot = std::pair<WriteType, SharedBuffer>;
		using partition = std::map<View, slot, SortKeyComparator>;
		using write_store =
			std::unordered_map<
				hash_type,
				std::variant<
					slot,
					partition
				>
			>;
		enum class DataType : char
		{
			FieldSequence = 'f',
			SchemaInstance = 's',
			Tombstone = 'r'
		};
		enum BloomType : char
		{
			PK = 1 << 0,
			PK_F = 1 << 1,
			PK_SK = 1 << 2,
		};
	private:
		std::filesystem::path _path{};
		std::atomic<std::size_t> _flush_running{ 0 };
		std::atomic<std::size_t> _flush_id{ 0 };
		std::shared_ptr<write_store> _map{};
		std::vector<std::weak_ptr<write_store>> _readonly_maps{};

		mutable std::vector<FlushHandle> _handle_cache{};
		mutable std::vector<std::size_t> _handle_cache_tracker{};
		mutable std::size_t _mappings{ 0 };
		mutable std::size_t _descriptors{ 0 };

		std::size_t _pressure{ 0 };
		std::size_t _id{ 0 };
		schema_type _schema{};
		QueryCache _qcache{};
		Config* _cfg{ nullptr };
		Log _logs{};

		std::size_t _cpu() const noexcept;

		FlushHandle& _handle_open(std::size_t flush) const noexcept;
		void _handle_reserve(bool ready = false) const noexcept;
		void _handle_close_soft(std::size_t flush) const noexcept;
		void _handle_close(std::size_t flush) const noexcept;

		partition _make_partition() const noexcept
		{
			return partition(SortKeyComparator{ _schema });
		}
		SharedBuffer _make_shared_buffer(WriteType type, std::span<const unsigned char> data, std::size_t alignment = 0) noexcept;
		void _merge_field_buffers(WriteType type, SharedBuffer buffer, std::span<const unsigned char> data) noexcept;

		const slot* _find_sorted_slot(const write_store& map, hash_type key, View sort) noexcept;
		const slot* _find_unsorted_slot(const write_store& map, hash_type key) noexcept;
		const slot* _find_slot(const write_store& map, hash_type key, View sort) noexcept;

		slot& _emplace_sorted_slot(write_store& map, hash_type key, View sort) noexcept;
		slot& _emplace_unsorted_slot(write_store& map, hash_type key) noexcept;
		slot& _emplace_slot(write_store& map, hash_type key, View sort) noexcept;

		std::size_t _read_entry_size_impl(View view) noexcept;
		std::size_t _read_entry_impl(View view, field_bitmap& fields, std::span<View> out, bool is_primary = true) noexcept;
		std::size_t _read_cache_impl(const write_store& map, hash_type key, View sort, field_bitmap& fields, std::span<View> out) noexcept;

		void _write_impl(write_store& map, WriteType type, hash_type key, View sort, std::span<const unsigned char> data) noexcept;
		void _reset_impl(write_store& map, hash_type key, View sort) noexcept;
		void _remove_impl(write_store& map, hash_type key, View sort) noexcept;

		bool _bloom_may_contain(key_type key, FlushHandle* handle) const noexcept;
		std::size_t _bloom_bits(std::size_t keys) const noexcept;
		std::size_t _bloom_hashes(std::size_t bits, std::size_t keys) const noexcept;
		std::pair<key_type, key_type> _hash_pair(key_type key) const noexcept;

		void _bloom_impl(const write_store& data, const std::filesystem::path& base, int id) noexcept;
		void _data_impl(const write_store& data, const std::filesystem::path& base, int id) noexcept;
		void _flush_impl(const write_store& data, int id) noexcept;
		void _flush_if() noexcept;

		void _move(MemoryCache&& copy) noexcept;
	public:
		MemoryCache(Config* cfg, std::size_t core, schema_type schema);
		MemoryCache(const MemoryCache&) = delete;
		MemoryCache(MemoryCache&& copy)
		{
			_move(std::move(copy));
		}

		std::size_t pressure() const noexcept;
		std::size_t descriptors() const noexcept;

		View read(hash_type key, View sort, field_bitmap fields) noexcept;

		void write(WriteType type, hash_type key, View sort, std::span<const unsigned char> data) noexcept;
		void reset(hash_type key, View sort) noexcept;
		void remove(hash_type key, View sort) noexcept;
		void flush() noexcept;

		MemoryCache& operator=(const MemoryCache&) = delete;
		MemoryCache& operator=(MemoryCache&& copy)
		{
			_move(std::move(copy));
			return *this;
		}
	};
}

#endif // RDB_MEMORY_HPP
