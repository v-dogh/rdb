#ifndef RDB_MEMORY_HPP
#define RDB_MEMORY_HPP

#include <bitset>
#include <filesystem>
#include <rdb_reflect.hpp>
#include <rdb_shared_buffer.hpp>
#include <rdb_root_config.hpp>
#include <rdb_utils.hpp>
#include <rdb_writetype.hpp>
#include <rdb_keytype.hpp>
#include <rdb_log.hpp>
#include <rdb_containers.hpp>
#include <rdb_task_ring.hpp>

namespace rdb
{
	// The memory cache is a per-core immutable structure responsible for batching new writes for a schema
	// The mount point is responsible for passing reads/writes to correct memory caches
	// Whenever we receive a new write, we first log it, at the same time we wait until the pressure reaches a critical point (in the MemoryCache)
	// Then we commit all of the data to disk
	class MemoryCache
	{
	public:
		using read_callback = std::function<void(std::size_t, View)>;
		using field_bitmap = std::bitset<256>;
		using WriteType = rdb::WriteType;
		struct Origin
		{
			std::thread::id tid{ std::this_thread::get_id() };

			auto operator<=>(const Origin& origin) const noexcept = default;
		};
	private:
		enum class DataType : unsigned char
		{
			FieldSequence,
			SchemaInstance,
			Tombstone
		};
		enum BloomType : unsigned char
		{
			PK = 1 << 0,
			PK_F = 1 << 1,
			PK_SK = 1 << 2,
		};

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
		struct Slot
		{
			std::uint32_t capacity;
			std::uint32_t size;
			DataType vtype;

			Slot(DataType vtype, std::span<const unsigned char> data) noexcept
				: capacity(data.size()), size(data.size()), vtype(vtype)
			{
				std::memcpy(buffer().data(), data.data(), data.size());
			}
			Slot(DataType vtype, std::size_t size) noexcept
				: capacity(size), size(size), vtype(vtype) {}

			std::span<const unsigned char> buffer() const noexcept
			{
				return std::span(
					reinterpret_cast<const unsigned char*>(this) + sizeof(std::uint32_t) * 2 + sizeof(DataType),
					size
				);
			}
			std::span<unsigned char> buffer() noexcept
			{
				return std::span(
					reinterpret_cast<unsigned char*>(this) + sizeof(std::uint32_t) * 2 + sizeof(DataType),
					size
				);
			}

			std::span<const unsigned char> flush_buffer() const noexcept
			{
				return std::span(
					reinterpret_cast<const unsigned char*>(this) + offsetof(Slot, vtype),
					size + sizeof(DataType)
				);
			}
			std::span<unsigned char> flush_buffer() noexcept
			{
				return std::span(
					reinterpret_cast<unsigned char*>(this) + offsetof(Slot, vtype),
					size + sizeof(DataType)
				);
			}

			static auto allocation_size(DataType vtype, std::span<const unsigned char> data) noexcept
			{
				return sizeof(std::uint32_t) * 2 + sizeof(DataType) + data.size();
			}
			static auto allocation_size(DataType vtype, std::size_t size) noexcept
			{
				return sizeof(std::uint32_t) * 2 + sizeof(DataType) + size;
			}
		};
		struct SlotDeleter
		{
			static Slot* allocate(DataType vtype, std::span<const unsigned char> data) noexcept
			{
				return ct::ordered_byte_map<Slot>::allocate_node(vtype, data);
			}
			static Slot* allocate(DataType vtype, std::size_t size) noexcept
			{
				return ct::ordered_byte_map<Slot>::allocate_node(vtype, size);
			}
			void operator()(Slot* slot) noexcept
			{
				ct::ordered_byte_map<Slot>::delete_node(slot);
			}
		};
		struct alignas (std::atomic<std::chrono::system_clock::time_point>) LockData
		{
			using ref = std::atomic_ref<std::chrono::system_clock::time_point>;

			static constexpr auto max = std::chrono::seconds(15);

			std::chrono::system_clock::time_point timestamp{};
			Origin origin{};

			bool expired_auto() const noexcept
			{
				return std::chrono::system_clock::now() - timestamp > max;
			}
			bool expired_man() const noexcept
			{
				return timestamp == std::chrono::system_clock::time_point();
			}
			bool expired() const noexcept
			{
				return
					expired_man() ||
					expired_auto();
			}
			void lock(Origin source) noexcept
			{
				auto lock = ref(timestamp);
				lock = std::chrono::system_clock::now();
				source = source;
				lock.notify_all();
			}
			void unlock() noexcept
			{
				auto lock = ref(timestamp);
				lock = std::chrono::system_clock::time_point();
				lock.notify_all();
			}

			static auto allocation_size(std::chrono::system_clock::time_point, Origin) noexcept
			{
				return sizeof(LockData);
			}
			static auto allocation_size() noexcept
			{
				return sizeof(LockData);
			}
		};
		struct Lock
		{
		private:
			LockData* _lock{ nullptr };
		public:
			Lock(std::nullptr_t) {}
			Lock(LockData& lock)
				: _lock(&lock) {}
			Lock(const Lock&) = delete;
			Lock(Lock&&) = default;

			bool is_ready() const noexcept
			{
				return _lock != nullptr;
			}
			void wait() noexcept
			{
				if (_lock == nullptr)
					return;

				auto lock = LockData::ref(_lock->timestamp);
				auto expected = lock.load();
				while (expected != std::chrono::system_clock::time_point() &&
					   expected - std::chrono::system_clock::now() < LockData::max)
				{
					lock.wait(expected);
					expected = lock.load();
				}
			}

			Lock& operator=(const Lock&) = delete;
			Lock& operator=(Lock&& copy) noexcept = default;
		};
		struct PartitionMetadata
		{
			std::uint64_t version{};
			std::uint64_t partition_sparse_index{};
			std::uint64_t intra_partition_sparse_index{};
			std::uint64_t block_size{};
		};

		using slot = Slot*;
		using const_slot = const Slot*;
		using single_slot = std::unique_ptr<Slot, SlotDeleter>;
		using partition = ct::ordered_byte_map<Slot>;
		using partition_variant = std::variant<single_slot, partition>;
		using write_store =
			ct::hash_map<
				key_type,
				std::pair<
					View,
					partition_variant
				>
			>;

		using lock_type = LockData*;
		using single_lock = LockData;
		using partition_lock = ct::ordered_byte_map<LockData>;
		using lock_partition_variant = std::variant<single_lock, partition_lock>;
		using lock_store =
			ct::hash_map<
				key_type,
				lock_partition_variant
			>;
	private:
		std::filesystem::path _path{};
		std::atomic<std::size_t> _flush_running{ 0 };
		std::atomic<std::size_t> _flush_id{ 0 };
		std::shared_ptr<write_store> _map{};
		ct::vector<std::weak_ptr<write_store>> _readonly_maps{};

		mutable ct::vector<FlushHandle> _handle_cache{};
		mutable ct::vector<std::size_t> _handle_cache_tracker{};
		mutable std::size_t _mappings{ 0 };
		mutable std::size_t _descriptors{ 0 };

		mutable RuntimeSchemaReflection::RTSI* _schema_info{ nullptr };
		mutable std::size_t _schema_version{ 0 };

		std::atomic<bool> _shutdown{ false };
		std::jthread _flush_thread{};
		ct::TaskRing<std::pair<std::shared_ptr<write_store>, std::size_t>, 4> _flush_tasks{};

		std::size_t _pressure{ 0 };
		std::size_t _id{ 0 };
		std::size_t _lock_cnt{ 0 };
		lock_store _locks{};
		schema_type _schema{};
		Config* _cfg{ nullptr };
		Log _logs{};

		RuntimeSchemaReflection::RTSI& _info() const noexcept;
		std::size_t _cpu() const noexcept;
		void _push_bytes(int) noexcept;

		FlushHandle& _handle_open(std::size_t flush) const noexcept;
		void _handle_reserve(bool ready = false) const noexcept;
		void _handle_close_soft(std::size_t flush) const noexcept;
		void _handle_close(std::size_t flush) const noexcept;

		std::optional<std::size_t> _disk_find_partition(key_type key, FlushHandle& handle) noexcept;
		std::pair<std::size_t, MemoryCache::PartitionMetadata> _disk_read_partition_metadata(FlushHandle& handle) noexcept;

		std::size_t _read_entry_size_impl(const View& view, DataType type) noexcept;
		std::size_t _read_entry_impl(const View& view, DataType type, field_bitmap& fields, const read_callback& callback) noexcept;
		std::size_t _read_cache_impl(write_store& map, key_type key, const View& sort, field_bitmap& fields, const read_callback& callback) noexcept;

		bool _read_impl(key_type key, const View& sort, field_bitmap fields, const read_callback& callback) noexcept;

		std::tuple<std::size_t, View, View> _page_map(write_store::iterator map, key_type key, const View& sort, std::size_t count) noexcept;
		std::tuple<std::size_t, View, View> _page_disk(key_type key, const View& sort, std::size_t count, FlushHandle& handle) noexcept;

		write_store::iterator _create_partition_log_if(write_store& map, key_type key, const View& partition) noexcept;
		write_store::iterator _create_partition_if(write_store& map, key_type key, const View& partition) noexcept;
		write_store::iterator _find_partition(write_store& map, key_type key) noexcept;

		slot _create_sorted_slot(write_store::iterator partition, const View& sort, DataType vtype, std::size_t reserve);
		slot _create_unsorted_slot(write_store::iterator partition, DataType vtype, std::size_t reserve);
		slot _create_slot(write_store::iterator partition, const View& sort, DataType vtype, std::size_t reserve);

		slot _create_sorted_slot(write_store::iterator partition, const View& sort, DataType vtype, std::span<const unsigned char> buffer);
		slot _create_unsorted_slot(write_store::iterator partition, DataType vtype, std::span<const unsigned char> buffer);
		slot _create_slot(write_store::iterator partition, const View& sort, DataType vtype, std::span<const unsigned char> buffer);

		slot _find_sorted_slot(write_store::iterator partition, const View& sort);
		slot _find_unsorted_slot(write_store::iterator partition);
		slot _find_slot(write_store::iterator partition, const View& sort);

		slot _resize_sorted_slot(write_store::iterator partition, const View& sort, std::size_t size);
		slot _resize_unsorted_slot(write_store::iterator partition, std::size_t size);
		slot _resize_slot(write_store::iterator partition, const View& sort, std::size_t size);

		lock_type _emplace_sorted_lock_if(lock_store::iterator partition, const View& sort);
		lock_type _emplace_unsorted_lock_if(lock_store::iterator partition);
		lock_type _emplace_lock_if(key_type key, const View& sort);

		void _write_impl(write_store::iterator partition, WriteType type, const View& sort, std::span<const unsigned char> data) noexcept;
		void _reset_impl(write_store::iterator partition, const View& sort) noexcept;
		void _remove_impl(write_store::iterator partition, const View& sort) noexcept;

		bool _bloom_may_contain(key_type key, FlushHandle& handle) const noexcept;
		bool _bloom_may_contain(key_type key, std::size_t off, FlushHandle& handle) const noexcept;
		std::size_t _bloom_bits(std::size_t keys, float probability) const noexcept;
		std::size_t _bloom_hashes(std::size_t bits, std::size_t keys) const noexcept;
		std::pair<key_type, key_type> _hash_pair(key_type key) const noexcept;

		void _bloom_impl(const write_store& map, Mapper& bloom, int id) noexcept;
		void _bloom_round_impl(key_type key, unsigned char* buffer, std::size_t space, std::size_t bits) noexcept;

		std::size_t _bloom_intra_partition_begin_impl(write_store::const_iterator partition, Mapper& bloom, int id) noexcept;
		void _bloom_intra_partition_round_impl(write_store::const_iterator part, const View& key, std::size_t bits, Mapper& bloom, int id) noexcept;
		void _bloom_intra_partition_end_impl(write_store::const_iterator partition, std::size_t bits, Mapper& bloom, int id) noexcept;
		void _data_impl(const write_store& map, Mapper& data, Mapper& indexer, Mapper& bloom, int id) noexcept;

		void _data_close_impl(Mapper& data) noexcept;
		void _indexer_close_impl(Mapper& indexer) noexcept;
		void _bloom_close_impl(Mapper& bloom) noexcept;

		void _flush_impl(const write_store& data, int id) noexcept;
		void _flush_if() noexcept;

		void _move(MemoryCache&& copy) noexcept;
	public:
		static auto origin() noexcept
		{
			return Origin();
		}

		MemoryCache(Config* cfg, std::size_t core, schema_type schema);
		MemoryCache(const MemoryCache&) = delete;
		MemoryCache(MemoryCache&& copy)
		{
			_move(std::move(copy));
		}
		~MemoryCache();

		std::size_t core() const noexcept;
		std::size_t pressure() const noexcept;
		std::size_t descriptors() const noexcept;

		View page(key_type key, std::size_t count) noexcept;
		View page_from(key_type key, const View& sort, std::size_t count) noexcept;
		bool read(key_type key, const View& sort, field_bitmap fields, const read_callback& callback) noexcept;
		bool exists(key_type key, const View& sort) noexcept;

		void write(WriteType type, key_type key, const View& partition, const View& sort, std::span<const unsigned char> data, Origin origin) noexcept;
		void reset(key_type key, const View& partition, const View& sort, Origin origin) noexcept;
		void remove(key_type key, const View& sort, Origin origin) noexcept;

		Lock lock(key_type key, const View& sort, Origin origin) noexcept;
		bool unlock(key_type key, const View& sort, Origin origin) noexcept;
		bool is_locked(key_type key, const View& sort, Origin origin) noexcept;

		void flush() noexcept;
		void clear() noexcept;

		MemoryCache& operator=(const MemoryCache&) = delete;
		MemoryCache& operator=(MemoryCache&& copy)
		{
			_move(std::move(copy));
			return *this;
		}
	};
}

#endif // RDB_MEMORY_HPP
