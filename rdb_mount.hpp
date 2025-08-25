#ifndef RDB_MOUNT_HPP
#define RDB_MOUNT_HPP

#include <condition_variable>
#include <memory_resource>
#include <functional>
#include <memory>
#include <vector>
#include <rdb_root_config.hpp>
#include <rdb_task_ring.hpp>
#include <rdb_memory.hpp>
#include <rdb_dsl.hpp>

namespace rdb
{
    class Mount :
        private QueryEngine<Mount>,
        public std::enable_shared_from_this<Mount>
    {
    public:
        using ptr = std::shared_ptr<Mount>;
        enum class Status
        {
            Warning,
            Error,
            Running,
            Stopped
        };
    private:
        struct Thread
        {
            using task = std::pair<
                         schema_type, std::function<void(MemoryCache*)>
                         >;

            ct::TaskRing<task, 128> queue{};

            bool stop{ false };
            std::thread thread{};

            Thread() = default;
            Thread(Thread&&) = default;
            Thread(const Thread&) = delete;

            void launch(schema_type schema, std::function<void(MemoryCache*)> func) noexcept
            {
                queue.enqueue(
                    std::move(schema),
                    std::move(func)
                );
            }
        };
        struct ControlFlowInfo
        {
        private:
            std::atomic<std::size_t> _order_ctr{ 0 };
            std::size_t _order_max{ 0 };
            std::size_t _chain_size{ ~0ull };
            bool _state{ false };
            bool(*_filter)(bool, bool)
            {
                [](bool o, bool n)
                {
                    return n;
                }
            };
        public:
            ControlFlowInfo() = default;
            ControlFlowInfo(ControlFlowInfo&&) = delete;
            ControlFlowInfo(const ControlFlowInfo&) = delete;
            ~ControlFlowInfo()
            {
                util::nano_wait_for(_order_ctr, _order_max, std::memory_order::relaxed);
            }

            auto order() noexcept
            {
                return _order_max++;
            }
            bool set(bool value, std::size_t order) noexcept
            {
                bool result;
                util::nano_wait_for(_order_ctr, order, std::memory_order::relaxed);
                _state = _filter(_state, value);
                result = _state;
                ++_order_ctr;
                _order_ctr.notify_all();
                return result;
            }
            bool get() const noexcept
            {
                util::nano_wait_for(_order_ctr, _order_max, std::memory_order::relaxed);
                return _state;
            }
            void set_filter(bool(*filter)(bool, bool)) noexcept
            {
                const auto o = order();
                util::nano_wait_for(_order_ctr, o, std::memory_order::relaxed);
                _filter = filter;
                ++_order_ctr;
                _order_ctr.notify_all();
            }
            void reset_filter() noexcept
            {
                set_filter([](bool o, bool n)
                {
                    return n;
                });
            }
            void set_chain(std::size_t size) noexcept
            {
                _chain_size = size;
            }
            auto get_chain() const noexcept
            {
                return _chain_size;
            }
        };
        struct ParserInfo
        {
            unsigned short operand_idx{ 0 };
            unsigned short operator_idx{ 0 };
        };
        struct ParserState
        {
            using fragment = std::pair<ParserInfo, View>;

            std::atomic_flag push_spinlock{};
            std::atomic<std::size_t> ref{ 0 };
            std::size_t operand{ 0 };
            std::pmr::vector<fragment> response{};
            QueryEngine::ReadChainStore::ptr store{};

            ParserState(
                std::pmr::memory_resource* res,
                QueryEngine::ReadChainStore::ptr ptr
            ) : response(res), store(std::move(ptr)) {}

            View push(View view, ParserInfo info) noexcept
            {
                while (push_spinlock.test_and_set(std::memory_order::acquire));
                auto res = View::view(
                               response.emplace_back(info, View::copy(std::move(view))).second
                           );
                push_spinlock.clear(std::memory_order::release);
                return res;
            }
            void wait() const noexcept
            {
                util::nano_wait_for(ref, 0ul, std::memory_order::relaxed);
            }
            void acquire() noexcept
            {
                ++ref;
            }
            void release() noexcept
            {
                if (--ref == 0) ref.notify_all();
            }
        };
        struct QueryLogShard
        {
            std::size_t offset{ 0 };
            std::size_t count{ 0 };
            std::atomic<std::size_t> resolved{ 0 };
            Mapper data{};
        };

        enum class QueryLogToken : unsigned char
        {
            Invalid,
            Waiting,
            Resolved,
        };
        using query_log_id = std::pair<std::size_t, std::size_t>;
    private:
        mutable std::mutex _mtx;
        mutable std::shared_mutex _query_log_mtx;
        mutable std::condition_variable _cv;

        // Query logging

        std::size_t _shard_id{ 0 };
        std::unordered_map<std::size_t, QueryLogShard> _log_shards{};

        // Other stuff

        std::vector<Thread> _threads{};
        Status _status{ Status::Stopped };
        Shared _shared{};

        query_log_id _log_query(std::span<const unsigned char> packet) noexcept;
        void _resolve_query(query_log_id id) noexcept;
        void _replay_queries() noexcept;

        std::size_t _vcpu(key_type key) const noexcept;

        std::tuple<std::size_t, schema_type, const RuntimeSchemaReflection::RTSI*> _query_parse_op_rtsi(
            std::span<const unsigned char> packet, ParserState& state, ParserInfo info) noexcept;
        std::tuple<std::size_t, View, key_type> _query_parse_op_pkey(std::span<const unsigned char> packet,
                const RuntimeSchemaReflection::RTSI& inf, ParserState& state, ParserInfo info) noexcept;
        std::tuple<std::size_t, View> _query_parse_op_skey(std::span<const unsigned char> packet,
                const RuntimeSchemaReflection::RTSI& inf, ParserState& state, ParserInfo info) noexcept;

        std::size_t _query_parse_op_fetch(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                          ParserInfo info) noexcept;
        std::size_t _query_parse_op_create(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                           ParserInfo info) noexcept;
        std::size_t _query_parse_op_remove(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                           ParserInfo info) noexcept;
        std::size_t _query_parse_op_page(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                         ParserInfo info) noexcept;
        std::size_t _query_parse_op_page_from(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                              ParserInfo info) noexcept;
        std::size_t _query_parse_op_check(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                          ParserInfo info) noexcept;
        std::size_t _query_parse_op_if(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                       ParserInfo info) noexcept;
        std::size_t _query_parse_op_atomic(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                           ParserInfo info) noexcept;
        std::size_t _query_parse_op_lock(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                         ParserInfo info) noexcept;
        std::size_t _query_parse_op_barrier(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi,
                                            ParserInfo info) noexcept;

        std::size_t _query_parse_operand(std::span<const unsigned char> packet, ParserState& state, ParserInfo info) noexcept;
        std::size_t _query_parse_schema_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort,
                schema_type schema, ParserState& state, ControlFlowInfo& cfi, ParserInfo& info) noexcept;
        std::size_t _query_parse_predicate_operator(std::span<const unsigned char> packet, key_type key, View partition,
                View sort, schema_type schema, ParserState& state, ControlFlowInfo& cfi, ParserInfo& info) noexcept;

        static inline const std::array _op_parse_table
        {
            &Mount::_query_parse_op_fetch,
            &Mount::_query_parse_op_create,
            &Mount::_query_parse_op_remove,
            &Mount::_query_parse_op_page,
            &Mount::_query_parse_op_page_from,
            &Mount::_query_parse_op_check,
            &Mount::_query_parse_op_if,
            &Mount::_query_parse_op_atomic,
            &Mount::_query_parse_op_lock,
            &Mount::_query_parse_op_barrier
        };
    protected:
        virtual void _core_impl(std::size_t core);
    public:
        friend class QueryEngine;
        QueryEngine& query{ *this };

        static auto make(Config cfg)
        {
            return std::make_shared<Mount>(cfg);
        }

        Mount() = default;
        Mount(Config cfg);
        Mount(const Mount&) = delete;
        Mount(Mount&&) = delete;
        ~Mount()
        {
            stop();
        }

        const Config& cfg() const noexcept;
        Config& cfg() noexcept;

        EventStore::ptr events() const noexcept;

        std::size_t cores() const noexcept;
        rs::RuntimeLogs::ptr logs() const noexcept;

        void start();
        void stop() noexcept;
        void wait() noexcept;
        bool query_sync(std::span<const unsigned char> packet, QueryEngine::ReadChainStore::ptr store) noexcept;

        template<typename Func>
        void run(schema_type schema, Func&& task) noexcept
        {
            for (decltype(auto) it : _threads)
                it.launch(schema, task);
        }
        template<typename Func>
        void run(std::size_t core, schema_type schema, Func&& task) noexcept
        {
            _threads[core]
            .launch(schema, task);
        }

        template<typename Schema, typename Func>
        void run(Func&& task) noexcept
        {
            for (decltype(auto) it : _threads)
                it.launch(Schema::ucode, task);
        }
        template<typename Schema, typename Func>
        void run(std::size_t core, Func&& task) noexcept
        {
            _threads[core]
            .launch(Schema::ucode, task);
        }

        Status status() const noexcept
        {
            std::lock_guard lock(_mtx);
            return _status;
        }

        Mount& operator=(const Mount&) = delete;
        Mount& operator=(Mount&&) = delete;
    };
}

#endif // RDB_MOUNT_HPP
