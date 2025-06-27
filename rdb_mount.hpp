#ifndef RDB_MOUNT_HPP
#define RDB_MOUNT_HPP

#include <memory_resource>
#include <condition_variable>
#include <future>
#include <memory>
#include <vector>
#include <functional>
#include <ConcurrentQueue/concurrentqueue.h>
#include <rdb_memory.hpp>
#include <rdb_root_config.hpp>
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

			std::unique_ptr<std::counting_semaphore<>> sem{};
			moodycamel::ConcurrentQueue<task> queue{};
			bool stop{ false };
			std::thread thread{};

			Thread() = default;
			Thread(Thread&&) = default;
			Thread(const Thread&) = delete;

			static auto make_sem()
			{
				return std::make_unique<std::counting_semaphore<>>(0);
			}

			void launch(task task) noexcept
			{
				queue.enqueue(std::move(task));
				sem->release();
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
				// Optimize for short queries

				for (int i = 0; i < 1000; ++i)
				{
					if (ref.load(std::memory_order::acquire) == 0)
						return;
					util::spinlock_yield();
				}

				// Optimize for longer queries

				auto expected = ref.load(std::memory_order::acquire);
				while (expected != 0)
				{
					ref.wait(expected, std::memory_order::acquire);
					expected = ref.load(std::memory_order::acquire);
				}
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
		std::unordered_map<
			std::size_t,
			QueryLogShard
		> _log_shards{};

		// Other stuff

		std::vector<Thread> _threads{};
		Config _cfg{};
		Status _status{ Status::Stopped };

		query_log_id _log_query(std::span<const unsigned char> packet) noexcept;
		void _resolve_query(query_log_id id) noexcept;
		void _replay_queries() noexcept;

		std::size_t _vcpu(std::uint32_t key) const noexcept;
		std::size_t _query_parse_operand(std::span<const unsigned char> packet, ParserState& state, ParserInfo info) noexcept;
		std::size_t _query_parse_schema_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort, schema_type schema, ParserState& state, ParserInfo& info) noexcept;
		std::size_t _query_parse_predicate_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort, schema_type schema, ParserState& state, ParserInfo& info) noexcept;
	public:
		friend class QueryEngine;
		QueryEngine& query{ *this };

		static auto make(Config cfg) noexcept
		{
			return std::make_shared<Mount>(cfg);
		}

		Mount() = default;
		Mount(Config cfg) : _cfg(cfg) {}
		Mount(const Mount&) = delete;
		Mount(Mount&&) = delete;
		~Mount()
		{
			stop();
		}

		std::size_t cores() const noexcept;

		void start() noexcept;
		void stop() noexcept;
		void wait() noexcept;
		bool query_sync(std::span<const unsigned char> packet, QueryEngine::ReadChainStore::ptr store) noexcept;

		template<typename Func>
		auto run(schema_type schema, Func&& task) noexcept
		{
			for (decltype(auto) it : _threads)
				it.launch(Thread::task{ schema, task });
		}
		template<typename Func>
		auto run(std::size_t core, schema_type schema, Func&& task) noexcept
		{
			_threads[core]
				.launch(Thread::task{ schema, task });
		}

		template<typename Schema, typename Func>
		auto run(Func&& task) noexcept
		{
			for (decltype(auto) it : _threads)
				it.launch(Thread::task{ Schema::ucode, task });
		}
		template<typename Schema, typename Func>
		auto run(std::size_t core, Func&& task) noexcept
		{
			_threads[core]
				.launch(Thread::task{ Schema::ucode, task });
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
