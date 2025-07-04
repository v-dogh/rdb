#include <rdb_dbg.hpp>
#include <rdb_mount.hpp>
#include <rdb_reflect.hpp>
#include <rdb_locale.hpp>
#include <limits>
#include <format>
#ifdef __unix__
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#else
#error(Unsupported Platform)
#endif

namespace rdb
{
	const Config& Mount::cfg() const noexcept
	{
		return _cfg;
	}
	Config& Mount::cfg() noexcept
	{
		return _cfg;
	}

	std::size_t Mount::cores() const noexcept
	{
		return _threads.size();
	}

	void Mount::start() noexcept
	{
		RDB_LOG("Attempting to start");
		std::lock_guard lock(_mtx);
		if (_status == Status::Running)
		{
			RDB_LOG("Attempting to stop");
			for (decltype(auto) it : _threads)
			{
				it.stop = true;
				it.sem->release();
				if (it.thread.joinable())
					it.thread.join();
			}
			_threads.clear();
			_status = Status::Stopped;
			RDB_LOG("Attempting to restart");
		}

		std::filesystem::create_directory(_cfg.root);
		if (!std::filesystem::exists(_cfg.root/"ntns"))
			std::filesystem::create_directory(_cfg.root/"ntns");

		_threads.resize(_cfg.mnt.cores);
		for (std::size_t i = 0; i < _cfg.mnt.cores; i++)
		{	
			_threads[i].sem = Thread::make_sem();
			_threads[i].thread = std::thread([this, i]() {
				RDB_FMT("Starting core{}", i);

				const auto path = _cfg.root/std::format("vcpu{}", i);
				if (!std::filesystem::exists(path))
				{
					std::filesystem::create_directory(path);
				}
				else if (_cfg.mnt.numa)
				{
#					ifdef __unix__
						cpu_set_t set;
						CPU_ZERO(&set);
						CPU_SET(i, &set);
						pthread_t thread = pthread_self();
						pthread_setaffinity_np(thread, sizeof(set), &set);
#					endif
				}

				ct::hash_map<schema_type, MemoryCache> schemas;

				// Replay all logs

				{
					for (decltype(auto) it : std::filesystem::directory_iterator(path))
					{
						const auto s = it.path().filename().string();
						const schema_type schema =
							uuid::decode(
								s.substr(1, s.size() - 2),
								uuid::table_alnum
							);
						schemas.emplace(
							std::piecewise_construct,
							std::forward_as_tuple(schema),
							std::forward_as_tuple(&_cfg, i, schema)
						);
					}
				}

				// Wait for requests

				while (true)
				{
					_threads[i].sem->acquire();
					std::pair<schema_type, std::function<void(MemoryCache*)>> task;
					if (_threads[i].queue.try_dequeue(task))
					{
						auto f = schemas.find(task.first);
						[[ unlikely ]] if (f == schemas.end())
						{
							f = schemas.emplace(
								std::piecewise_construct,
								std::forward_as_tuple(task.first),
								std::forward_as_tuple(&_cfg, i, task.first)
							).first;
						}
						task.second(&f->second);
					}
					if (_threads[i].stop)
					{
						break;
					}
				}
			});
		}

		_status = Status::Running;
	}
	void Mount::stop() noexcept
	{
		RDB_LOG("Attempting to stop");
		std::lock_guard lock(_mtx);
		for (decltype(auto) it : _threads)
		{
			it.stop = true;
			it.sem->release();
			if (it.thread.joinable())
				it.thread.join();
		}
		_threads.clear();
		_status = Status::Stopped;
	}
	void Mount::wait() noexcept
	{
		std::unique_lock lock(_mtx);
		_cv.wait(lock, [this]() {
			return _status != Status::Running;
		});
	}

	Mount::query_log_id Mount::_log_query(std::span<const unsigned char> packet) noexcept
	{
		std::lock_guard lock(_query_log_mtx);
		const auto req_size = packet.size() + sizeof(std::uint32_t) + 1;
		auto f = _log_shards.find(_shard_id);
		if (f == _log_shards.end())
		{
			const auto [ it, _ ] = _log_shards.try_emplace(_shard_id);
			f = it;
			f->second.data.map(_cfg.root/"ntns"/"s0");
			f->second.data.reserve(_cfg.logs.log_shard_size);
		}
		else if (f->second.offset + req_size > f->second.data.size())
		{
			const auto [ it, _ ] = _log_shards.try_emplace(++_shard_id);
			f = it;
			it->second.data.map(_cfg.root/"ntns"/std::format("s{}", _shard_id));
			it->second.data.reserve(_cfg.logs.log_shard_size);
		}
		auto& shard = f->second;
		const auto off = shard.offset;
		shard.data.memory()[shard.offset++]
			= char(QueryLogToken::Waiting);
		shard.offset += byte::swrite(
			shard.data.memory(),
			shard.offset,
			std::uint32_t(packet.size())
		);
		shard.offset += byte::swrite(
			shard.data.memory(),
			shard.offset,
			packet
		);
		return std::make_pair(_shard_id, off);
	}
	void Mount::_resolve_query(query_log_id id) noexcept
	{
		std::shared_lock lock(_query_log_mtx);
		auto& shard = _log_shards[id.first];
		shard.data.memory()[id.second]
			= char(QueryLogToken::Resolved);

		const auto value = shard.resolved.fetch_add(1, std::memory_order::relaxed) + 1;
		if (_shard_id - 1 != id.first &&
			value == shard.count)
		{
			shard.data.remove();
			if (_log_shards.size() > 12)
			{
				shard.data.close();
				lock.unlock();
				std::lock_guard lock(_query_log_mtx);
				for (auto it = _log_shards.begin(); it != _log_shards.end();)
				{
					auto cur = it++;
					if (cur->second.resolved == cur->second.count)
						_log_shards.erase(cur);
				}
				_log_shards.rehash(_log_shards.size());
			}
		}
	}
	void Mount::_replay_queries() noexcept
	{
		std::vector<std::size_t> shards;
		for (decltype(auto) it : std::filesystem::directory_iterator(_cfg.root/"ntns"))
		{
			shards.push_back(
				std::stoi(it
					.path()
					.filename()
					.string()
					.substr(1))
			);
			auto& shard = _log_shards[shards.back()];
			shard.data.open(it);
		}
		std::sort(shards.begin(), shards.end());
		for (decltype(auto) it : shards)
		{
			auto& shard = _log_shards[it];
			while (shard.data.memory()[shard.offset] !=
				   char(QueryLogToken::Invalid))
			{
				const auto type_offset = shard.offset;
				const auto type = QueryLogToken(shard.data.memory()[shard.offset++]);
				const auto size = byte::sread<std::uint32_t>(shard.data.memory(), shard.offset);
				const auto packet = std::span(
					shard.data.memory().subspan(shard.offset, size)
				);
				shard.offset += size;
				if (type == QueryLogToken::Resolved)
				{
					shard.resolved++;
				}
				else if (query_sync(packet, nullptr))
				{
					_resolve_query(query_log_id{ it, type_offset });
				}
				shard.count++;
			}
		}
		if (!shards.empty())
			_shard_id = shards.back();
	}

	std::size_t Mount::_vcpu(key_type key) const noexcept
	{
		return key % _cfg.mnt.cores;
	}

	bool Mount::query_sync(std::span<const unsigned char> packet, QueryEngine::ReadChainStore::ptr store) noexcept
	{
		thread_local std::aligned_storage_t<32, alignof(ParserState::fragment)> pool;
		std::pmr::monotonic_buffer_resource resource(&pool, sizeof(pool));
		ParserState state(&resource, std::move(store));
		ParserInfo inf{};

		std::size_t off = 0;
		while (packet.size() >= off + 2)
		{
			const auto flags = packet[off++];
			if (flags & QueryEngine::OperandFlags::Reads)
				inf.operand_idx++;
			off += _query_parse_operand(
				packet.subspan(off),
				state,
				inf
			);
		}
		state.wait();
		for (decltype(auto) it : state.response)
		{
			if (!it.second.empty())
			{
				state.store->handlers[
					it.first.operand_idx - 1
				](it.first.operator_idx, it.second);
			}
		}

		return true;
	}

	std::size_t Mount::_query_parse_operand(std::span<const unsigned char> packet, ParserState& state, ParserInfo inf) noexcept
	{
		const auto op = cmd::qOp(packet[0]);
		std::size_t off = sizeof(cmd::qOp);

		// Extract keys

		if (packet.size() < sizeof(cmd::qOp) + sizeof(key_type) + sizeof(schema_type))
			return packet.size();

		const auto schema = byte::sread<schema_type>(packet.data(), off);
		const auto* info = RuntimeSchemaReflection::fetch(schema);
		if (info == nullptr)
		{
			RDB_WARN("Unrecognized schema passed to query parser")
			return packet.size();
		}

		key_type key = 0x00;
		View pkey = nullptr;
		View sort = nullptr;
		// Get partition key
		{
			const auto size = info->partition_size(
				packet.subspan(off).data()
			);
			pkey = View::view(packet.subspan(off, size));
			off += size;
			key = info->hash_partition(pkey.data().data());
		}

		// Get sort keys
		if (info->skeys() &&
			op == cmd::qOp::Fetch || op == cmd::qOp::Check ||
			op == cmd::qOp::Remove ||
			op == cmd::qOp::PageFrom)
		{
			const auto size = byte::sread<std::uint32_t>(packet, off);
			sort = View::view(packet.subspan(off, size));
			off += size;
		}

		if (op == cmd::qOp::Fetch ||
			op == cmd::qOp::Check)
		{
			if (op == cmd::qOp::Fetch)
			{
				std::size_t coff = 0;
				while ((coff = _query_parse_schema_operator(
						packet.subspan(off),
						key, pkey, sort,
						schema,
						state,
						inf
					)) != ~0ull)
				{
					off += coff;
					if (off >= packet.size())
						break;
				}
			}
			else if (op == cmd::qOp::Check)
			{
				std::size_t coff = 0;
				while ((coff = _query_parse_predicate_operator(
						packet.subspan(off),
						key, pkey, sort,
						schema,
						state,
						inf
					)) != ~0ull)
				{
					off += coff;
					if (off >= packet.size())
						break;
				}
			}
		}
		else if (op == cmd::qOp::Create ||
				 op == cmd::qOp::Remove)
		{
			auto& core = _threads[_vcpu(key)];

			if (op == cmd::qOp::Create)
			{
				View data = nullptr;
				{
					data = View::view(
						packet.subspan(
							off,
							info->storage(packet.data() + off)
						)
					);
					off += data.size();
				}

				state.acquire();
				core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
					cache->write(
						WriteType::Table,
						key, pkey, nullptr,
						data
					);
					state.release();
				}));
			}
			else if (op == cmd::qOp::Remove)
			{
				state.acquire();
				core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
					cache->remove(key, sort);
					state.release();
				}));
			}
		}
		else if (op == cmd::qOp::PageFrom ||
				 op == cmd::qOp::Page)
		{
			auto& core = _threads[_vcpu(key)];
			const auto count = byte::sread<std::uint32_t>(packet, off);
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				if (op == cmd::qOp::Page)
				{
					state.push(cache->page(key, count), ParserInfo{
						.operand_idx = inf.operand_idx,
						.operator_idx = 0
					});
				}
				else if (op == cmd::qOp::PageFrom)
				{
					state.push(cache->page_from(key, sort, count), ParserInfo{
						.operand_idx = inf.operand_idx,
						.operator_idx = 0
					});
				}
				state.release();
			}));
		}
		return off;
	}
	std::size_t Mount::_query_parse_schema_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort, schema_type schema, ParserState& state, ParserInfo& info) noexcept
	{
		const auto op = cmd::qOp(packet[0]);
		auto& core = _threads[_vcpu(key)];
		std::size_t off = sizeof(cmd::qOp);
		if (op == cmd::qOp::Reset)
		{
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				cache->reset(key, partition, sort);
				state.release();
			}));
		}
		else if (op == cmd::qOp::Write)
		{
			const auto len = byte::sread<std::uint32_t>(packet.data(), off);
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				cache->write(
					WriteType::Field,
					key, partition, sort,
					packet.subspan(
						off,
						len + sizeof(std::uint8_t)
					)
				);
				state.release();
			}));
			off += len + sizeof(std::uint8_t);
		}
		else if (op == cmd::qOp::Read)
		{
			MemoryCache::field_bitmap fields{};
			std::array<unsigned short, std::numeric_limits<unsigned char>::max()> field_operator_map{};
			off--;
			do
			{
				off++;
				const auto field = packet[off++];
				fields.set(field);
				field_operator_map[field] = info.operator_idx++;
			} while (
				off < packet.size() &&
				packet[off] == char(cmd::qOp::Read)
			);

			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				std::size_t idx = 0;
				cache->read(key, sort, fields, [&](std::size_t field, View data) {
					state.push(View::copy(data), ParserInfo{
						.operand_idx = info.operand_idx,
						.operator_idx = field_operator_map[field]
					});
				});
				state.release();
			}));
		}
		else if (op == cmd::qOp::WProc)
		{
			const auto len = byte::sread<std::uint32_t>(packet.data(), off);
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				cache->write(
					WriteType::WProc,
					key, partition, sort,
					packet.subspan(
						off,
						len +
							sizeof(proc_opcode) +
							sizeof(std::uint8_t)
					)
				);
				state.release();
			}));
			off += len + sizeof(proc_opcode) + sizeof(std::uint8_t);
		}
		else if (op == cmd::qOp::RProc)
		{

		}
		else
		{
			return ~0ull;
		}
		return off;
	}
	std::size_t Mount::_query_parse_predicate_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort, schema_type schema, ParserState& state, ParserInfo& info) noexcept
	{
		const auto op = cmd::qOp(packet[0]);
		auto& core = _threads[_vcpu(key)];
		std::size_t off = sizeof(cmd::qOp);
		if (op == cmd::qOp::FilterExists)
		{
			const auto op_idx = info.operator_idx++;
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				const auto result = cache->exists(key, sort);
				auto v = View::copy(1);
				v.mutate()[0] = static_cast<unsigned char>(result);
				state.push(std::move(v), ParserInfo{
					.operand_idx = info.operand_idx,
					.operator_idx = op_idx
				});
				state.release();
			}));
		}
		else
		{
			return ~0ull;
		}
		return off;
	}
}
