#ifndef RDB_ROOT_CONFIG_HPP
#define RDB_ROOT_CONFIG_HPP

#include <rdb_meta.hpp>
#include <rdb_runtime_logs.hpp>
#include <rdb_memunits.hpp>
#include <rdb_writetype.hpp>
#include <filesystem>
#include <functional>
#include <shared_mutex>
#include <mutex>
#include <thread>

namespace rdb
{
    enum class Event
    {
        // A new core was launched
        CoreStart,
        // An active core was stopped
        CoreStop,

        // Received a new query to parse
        QueryReceive,
        // Query finished parsing
        QueryFinish,

        // A write is received by the memory cache
        Write,
        // A read is received by the memory cache
        Read,

        // A write failed
        WriteFailure,
        // A read failed
        ReadFailure,

        // The memory usage of a memory cache instance changed (estimated)
        MemoryPressure,
        // Pressure in the disk cache changed
        DiskCachePressure,
        // Pressure in the handle cache changed
        HandleCachePressure,
        // A flush has begun
        FlushStart,
        // A flush has ended
        FlushEnd,
    };

    namespace impl
    {
        template<typename Ret, typename... Argv>
        using event_callback = std::vector<std::function<Ret(Argv...)>>;
        using event_callbacks =
            meta::PackInfo<
            event_callback<void, std::size_t>,
            event_callback<void, std::size_t>,
            event_callback<void, std::span<const unsigned char>>,
            event_callback<void, std::span<const unsigned char>, std::string_view>,
            event_callback<void, WriteType, std::span<const unsigned char>>,
            event_callback<void, ReadType, std::span<const unsigned char>>,
            event_callback<void, WriteType, std::span<const unsigned char>>,
            event_callback<void, ReadType, std::span<const unsigned char>>,
            // Est. memory usage | field count
            event_callback<void, std::size_t, std::size_t>,
            // Est. memory usage
            event_callback<void, std::size_t>,
            // Data handles | indexer handles | bloom handles
            event_callback<void, std::size_t, std::size_t, std::size_t>,
            // Est. memory to be flushed | fields flushed
            event_callback<void, std::size_t, std::size_t>,
            // Data size | indexer size | bloom size | success
            event_callback<void, std::size_t, std::size_t, std::size_t, bool>
            >;
    }

    class EventStore :
        private meta::EnumStateMap<Event, impl::event_callbacks>,
        std::enable_shared_from_this<EventStore>
    {
    public:
        using ptr = std::shared_ptr<EventStore>;
        using wptr = std::weak_ptr<EventStore>;
        class Handle
        {
        private:
            wptr _store{};
            std::size_t _id{ 0 };
            void(*_release)(ptr, std::size_t) { nullptr };
        public:
            Handle(wptr store, std::size_t id, void(*rel)(ptr, std::size_t));
            Handle(const Handle&) = delete;
            Handle(Handle&&) = default;
            ~Handle();

            void release();
            void drop();

            Handle& operator=(const Handle&) = delete;
            Handle& operator=(Handle&&) = default;
        };
    private:
        std::shared_mutex _mtx{};
    public:
        EventStore() = default;
        EventStore(const EventStore&) = delete;
        EventStore(EventStore&&) = delete;

        template<Event Ev, typename Func>
        Handle listen(Func&& callback)
        {
            auto dealloc = +[](ptr ptr, std::size_t id)
            {
                std::unique_lock lock(ptr->_mtx);
                ptr->state<Ev>()[id] = nullptr;
            };

            std::unique_lock lock(_mtx);

            auto& state = this->state<Ev>();
            for (std::size_t i = 0; i < state.size(); i++)
                if (state[i] == nullptr)
                {
                    state[i] = std::forward<Func>(callback);
                    return Handle(weak_from_this(), i, dealloc);
                }

            state.push_back(std::forward<Func>(callback));
            return Handle(weak_from_this(), state.size(), dealloc);
        }

        template<Event Ev, typename... Argv>
        void trigger(Argv&&... args)
        {
            std::shared_lock lock(_mtx);
            for (decltype(auto) it : this->state<Ev>())
                it(args...);
        }

        EventStore& operator=(const EventStore&) = delete;
        EventStore& operator=(EventStore&&) = delete;
    };

    struct Config
    {
        // Root directory of the database
        std::filesystem::path root{ "/rdb/" };
        struct Mount
        {
            enum class CPUProfile
            {
                OptimizeSpeed,
                OptimizeUsage
            };
            static inline auto default_cores = std::thread::hardware_concurrency();

            // Number of cores for the database to distribute load
            std::size_t cores{ default_cores };
            // Whether to enable NUMA awareness
            bool numa{ true };
            // Optimizes for chosen qualities when it comes to CPU usage
            CPUProfile cpu_profile{ CPUProfile::OptimizeUsage };
            // Runtime logs config
            rs::RuntimeLogs::Config logs{};
        } mnt;
        struct Logs
        {
            // The size of a single log shard (bytes)
            std::size_t log_shard_size{ 1024 * 1024 * 4 };
            // The amount of data required for a flush of the WAL logs
            std::size_t flush_pressure{ 0 };
            // Whether to log each write
            bool enable{ true };
        } logs;
        struct Cache
        {
            enum class Type
            {
                ALC,
                LRU,
                LFU,
            };

            // The block size during a flush
            std::size_t block_size{ mem::KiB(64) };
            // The number of partitions to linearly scan
            std::size_t partition_sparse_index_ratio{ 4 };
            // The number of blocks to linearly scan
            std::size_t block_sparse_index_ratio{ 8 };
            // The number of sorted values to linearly scan
            std::size_t sort_sparse_index_ratio{ 16 };
            // Amount of data in the memory cache that triggers a flush (bytes)
            std::size_t flush_pressure{ mem::MiB(256) };
            // Automatic compaction fold ratio determines how many flushes fold into a single flush
            std::size_t compaction_fold_ratio{ 8 };
            // How many flushes are allowed before forced compaction
            std::size_t compaction_pressure{ 24 };
            // Maximum open file descriptors
            std::size_t max_descriptors{ 4096 };
            // Maximum open mappings
            std::size_t max_mappings{ 8192 };
            // Maximum created locks (that possibly are expired)
            std::size_t max_locks{ 128 };
            // The ratio of compressed data to decompressed data below which we write a compressed block
            float compression_ratio{ 0.9f };
            // The average chance for a false positive in the partition bloom filter
            float partition_bloom_fp_rate{ 0.001f };
            // The average chance for a false positive in the intra-partition bloom filter
            float intra_partition_bloom_fp_rate{ 0.01f };
            // Query cache type (trigerred when a disk read is performed)
            Type cache_type{ Type::ALC };
            // Maximum allowed memory usage of the cache (bytes) (only considers data stored, not the total memory used by structures)
            std::size_t max_cache_volume{ mem::MiB(512) };
            // Maximum allowed memory usage for the page cache (bytes)
            std::size_t max_page_cache_volume{ mem::MiB(64) };
            // Whether page requests should be cached
            bool cache_page{ false };
        } cache;
    };
    struct Shared
    {
        rs::RuntimeLogs::ptr logs;
        EventStore::ptr events;
        std::shared_ptr<Config> cfg;
    };
}

#endif // RDB_ROOT_CONFIG_HPP
