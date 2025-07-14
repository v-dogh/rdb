#include <rdb_dbg.hpp>
#include <rdb_mount.hpp>
#include <rdb_reflect.hpp>
#include <rdb_locale.hpp>
#include <limits>
#include <format>

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
			_threads[i].thread = std::thread([this, i]() {
				RDB_FMT("Starting core{}", i);

				const auto path = _cfg.root/std::format("vcpu{}", i);
				if (!std::filesystem::exists(path))
				{
					std::filesystem::create_directory(path);
				}
				else if (_cfg.mnt.numa)
				{
					util::bind_thread(i);
				}

				ct::hash_map<schema_type, MemoryCache> schemas;

				// Replay all logs

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

				// Wait for requests

				constexpr auto spin_iters = 500;
				constexpr auto yield_iters = 10'000;

				std::size_t spin_ctr = 0;
				std::size_t yield_ctr = 0;

				while (true)
				{
					auto& t = _threads[i];
					Thread::task task;
					if (_cfg.mnt.cpu_profile == Config::Mount::CPUProfile::OptimizeUsage ?
							t.queue.dequeue(task) :
							t.queue.try_dequeue(task))
					{
						[[ unlikely ]] if (task.second == nullptr)
							break;

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

						spin_ctr = 0;
						yield_ctr = 0;

						continue;
					}

					if (++spin_ctr < spin_iters)
						util::spinlock_yield();
					else if (++yield_ctr < yield_iters)
						std::this_thread::yield();
					else if (_cfg.mnt.cpu_profile == Config::Mount::CPUProfile::OptimizeSpeed)
						std::this_thread::sleep_for(std::chrono::microseconds(50));
				}
			});
		}

		_status = Status::Running;
	}
	void Mount::stop() noexcept
	{
		RDB_LOG("Attempting to stop");
		{
			std::lock_guard lock(_mtx);
			for (decltype(auto) it : _threads)
			{
				it.stop = true;
				it.launch(0, nullptr);
				if (it.thread.joinable())
					it.thread.join();
			}
			_threads.clear();
			_status = Status::Stopped;
		}
		_cv.notify_all();
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

	std::tuple<std::size_t, schema_type, const RuntimeSchemaReflection::RTSI*> Mount::_query_parse_op_rtsi(std::span<const unsigned char> packet, ParserState& state, ParserInfo info) noexcept
	{
		std::size_t off = 0;
		const auto schema = byte::sread<schema_type>(packet, off);
		const auto* inf = RuntimeSchemaReflection::fetch(schema);
		if (inf == nullptr)
		{
			RDB_WARN("Unrecognized schema passed to query parser")
			return { packet.size(), 0, nullptr };
		}
		return { off, schema, inf };
	}
	std::tuple<std::size_t, View, key_type> Mount::_query_parse_op_pkey(std::span<const unsigned char> packet, const RuntimeSchemaReflection::RTSI& inf, ParserState& state, ParserInfo info) noexcept
	{
		std::tuple<std::size_t, View, key_type> result{};
		auto& [ off, val, key ] = result;

		const auto size = inf.partition_size(
			packet.subspan(off).data()
		);
		val = View::view(packet.subspan(off, size));
		off += size;
		key = inf.hash_partition(val.data().data());

		return result;
	}
	std::tuple<std::size_t, View> Mount::_query_parse_op_skey(std::span<const unsigned char> packet, const RuntimeSchemaReflection::RTSI& inf, ParserState& state, ParserInfo info) noexcept
	{
		std::tuple<std::size_t, View> result{};
		auto& [ off, sort ] = result;

		if (const auto cnt = inf.skeys(); cnt)
		{
			std::size_t size = 0;
			for (std::size_t i = 0; i < cnt; i++)
			{
				RuntimeInterfaceReflection::RTII& field = inf.reflect_skey(i);
				size += field.storage(packet.data() + off);
			}
			sort = View::view(packet.subspan(off, size));
			off += size;
		}

		return result;
	}

	std::size_t Mount::_query_parse_op_fetch(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet, state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;
		const auto [ off3, sort ] = _query_parse_op_skey(packet.subspan(off), *inf, state, info); off += off3;

		std::size_t coff = 0;
		while ((coff = _query_parse_schema_operator(
				packet.subspan(off),
				key, pkey, sort,
				schema,
				state,
				cfi,
				info
			)) != ~0ull)
		{
			off += coff;
			if (off >= packet.size())
				break;
		}

		return off;
	}
	std::size_t Mount::_query_parse_op_create(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet, state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;

		auto& core = _threads[_vcpu(key)];

		View data = View::view(
			packet.subspan(
				off,
				inf->storage(packet.data() + off)
			)
		);
		off += data.size();

		state.acquire();
		core.launch(schema, [=, ctx = MemoryCache::origin(), &state, this](MemoryCache* cache) {
			cache->write(
				WriteType::Table,
				key, pkey, nullptr,
				data, ctx
			);
			state.release();
		});

		return off;
	}
	std::size_t Mount::_query_parse_op_remove(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet, state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;
		const auto [ off3, sort ] = _query_parse_op_skey(packet.subspan(off), *inf, state, info); off += off3;

		auto& core = _threads[_vcpu(key)];

		state.acquire();
		core.launch(schema, [=, ctx = MemoryCache::origin(), &state, this](MemoryCache* cache) {
			cache->remove(key, sort, ctx);
			state.release();
		});

		return off;
	}
	std::size_t Mount::_query_parse_op_page(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet, state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;
		const auto [ off3, sort ] = _query_parse_op_skey(packet.subspan(off), *inf, state, info); off += off3;

		auto& core = _threads[_vcpu(key)];
		const auto count = byte::sread<std::uint32_t>(packet, off);
		state.acquire();
		core.launch(schema, [=, &state, this](MemoryCache* cache) {
			state.push(cache->page(key, count), ParserInfo{
				.operand_idx = info.operand_idx,
				.operator_idx = 0
			});
			state.release();
		});

		return off;
	}
	std::size_t Mount::_query_parse_op_page_from(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet, state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;
		const auto [ off3, sort ] = _query_parse_op_skey(packet.subspan(off), *inf, state, info); off += off3;

		auto& core = _threads[_vcpu(key)];
		const auto count = byte::sread<std::uint32_t>(packet, off);
		state.acquire();
		core.launch(schema, [=, &state, this](MemoryCache* cache) {
			state.push(cache->page_from(key, sort, count), ParserInfo{
				.operand_idx = info.operand_idx,
				.operator_idx = 0
			});
			state.release();
		});

		return off;
	}
	std::size_t Mount::_query_parse_op_check(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet, state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;
		const auto [ off3, sort ] = _query_parse_op_skey(packet.subspan(off), *inf, state, info); off += off3;

		std::size_t coff = 0;
		while ((coff = _query_parse_predicate_operator(
				packet.subspan(off),
				key, pkey, sort,
				schema,
				state,
				cfi,
				info
			)) != ~0ull)
		{
			off += coff;
			if (off >= packet.size())
				break;
		}

		return off;
	}
	std::size_t Mount::_query_parse_op_if(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo&, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		ControlFlowInfo cfi;
		cfi.set_chain(byte::sread<std::uint32_t>(packet, off));

		const auto total = cfi.get_chain() + sizeof(std::uint32_t);
		[[ likely ]] if (const auto op = packet[off++]; op < _op_parse_table.size())
		{
			const auto func = _op_parse_table[op];
			off += (this->*func)(packet.subspan(off), state, cfi, info);
		}
		if (cfi.get())
		{
			while (off != total)
			{
				const auto op = packet[off++];
				if (op < _op_parse_table.size())
				{
					const auto func = _op_parse_table[op];
					off += (this->*func)(packet.subspan(off), state, cfi, info);
				}
				else
				{
					return ~0ull;
				}
			}
			return total;
		}
		return total;
	}
	std::size_t Mount::_query_parse_op_atomic(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		cfi.set_chain(byte::sread<std::uint32_t>(packet, off));

		const auto total = cfi.get_chain() + sizeof(std::uint32_t);
		const auto qid = _log_query(packet.subspan(off, cfi.get_chain()));
		while (off != total)
		{
			const auto op = packet[off++];
			if (op < _op_parse_table.size())
			{
				const auto func = _op_parse_table[op];
				off += (this->*func)(packet.subspan(off), state, cfi, info);
			}
			else
			{
				return ~0ull;
			}
		}
		_resolve_query(qid);
		return off;
	}
	std::size_t Mount::_query_parse_op_lock(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo&, ParserInfo info) noexcept
	{
		std::size_t off = 0;

		ControlFlowInfo cfi;
		cfi.set_chain(byte::sread<std::uint32_t>(packet, off));

		const auto [ off1, schema, inf ] = _query_parse_op_rtsi(packet.subspan(off), state, info); off += off1;
		if (inf == nullptr) return off1;
		const auto [ off2, pkey, key ] = _query_parse_op_pkey(packet.subspan(off), *inf, state, info); off += off2;
		const auto [ off3, sort ] = _query_parse_op_skey(packet.subspan(off), *inf, state, info); off += off3;

		auto& core = _threads[_vcpu(key)];
		const auto op_idx = info.operator_idx++;

		core.launch(schema, [&, ctx = MemoryCache::origin(), order = cfi.order(), this](MemoryCache* cache) {
			auto lock = cache->lock(key, sort, ctx);
			auto v = View::copy(1);
			v.mutate()[0] = cfi.set(!lock.is_ready(), order);
			state.push(std::move(v), ParserInfo{
				.operand_idx = info.operand_idx,
				.operator_idx = op_idx
			});
		});

		const auto total = cfi.get_chain() + sizeof(std::uint32_t);
		if (cfi.get())
		{
			while (off != total)
			{
				const auto op = packet[off++];
				if (op < _op_parse_table.size())
				{
					const auto func = _op_parse_table[op];
					off += (this->*func)(packet.subspan(off), state, cfi, info);
				}
				else
					break;
			}
		}
		state.acquire();
		core.launch(schema, [=, ctx = MemoryCache::origin(), &state, this](MemoryCache* cache) {
			cache->unlock(key, sort, ctx);
			state.release();
		});

		return total;
	}
	std::size_t Mount::_query_parse_op_barrier(std::span<const unsigned char> packet, ParserState& state, ControlFlowInfo& cfi, ParserInfo info) noexcept
	{
		state.wait();
		return 0;
	}

	std::size_t Mount::_query_parse_operand(std::span<const unsigned char> packet, ParserState& state, ParserInfo info) noexcept
	{
		std::size_t off = 0;
		[[ likely ]] if (const auto op = packet[off++]; op < _op_parse_table.size())
		{
			ControlFlowInfo cfi;
			const auto func = _op_parse_table[op];
			return off + (this->*func)(packet.subspan(off), state, cfi, info);
		}
		return ~0ull;
	}
	std::size_t Mount::_query_parse_schema_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort, schema_type schema, ParserState& state, ControlFlowInfo& cfi, ParserInfo& info) noexcept
	{
		auto& core = _threads[_vcpu(key)];
		std::size_t off = 0;
		const auto op = cmd::qOp(packet[off++]);
		if (op == cmd::qOp::Reset)
		{
			state.acquire();
			core.launch(schema, [=, ctx = MemoryCache::origin(), &state, this](MemoryCache* cache) {
				cache->reset(key, partition, sort, ctx);
				state.release();
			});
		}
		else if (op == cmd::qOp::Write)
		{
			const auto len = byte::sread<std::uint32_t>(packet.data(), off);
			state.acquire();
			core.launch(schema, [=, ctx = MemoryCache::origin(), &state, this](MemoryCache* cache) {
				cache->write(
					WriteType::Field,
					key, partition, sort,
					packet.subspan(
						off,
						len + sizeof(std::uint8_t)
					),
					ctx
				);
				state.release();
			});
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
			core.launch(schema, [=, &state, this](MemoryCache* cache) {
				std::size_t idx = 0;
				cache->read(key, sort, fields, [&](std::size_t field, View data) {
					state.push(View::copy(data), ParserInfo{
						.operand_idx = info.operand_idx,
						.operator_idx = field_operator_map[field]
					});
				});
				state.release();
			});
		}
		else if (op == cmd::qOp::WProc)
		{
			const auto len = byte::sread<std::uint32_t>(packet.data(), off);
			state.acquire();
			core.launch(schema, [=, ctx = MemoryCache::origin(), &state, this](MemoryCache* cache) {
				cache->write(
					WriteType::WProc,
					key, partition, sort,
					packet.subspan(
						off,
						len +
							sizeof(proc_opcode) +
							sizeof(std::uint8_t)
					),
					ctx
				);
				state.release();
			});
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
	std::size_t Mount::_query_parse_predicate_operator(std::span<const unsigned char> packet, key_type key, View partition, View sort, schema_type schema, ParserState& state, ControlFlowInfo& cfi, ParserInfo& info) noexcept
	{
		auto& core = _threads[_vcpu(key)];
		std::size_t off = 0;
		const auto op = cmd::qOp(packet[off++]);
		if (op == cmd::qOp::FilterExists)
		{
			const auto op_idx = info.operator_idx++;
			state.acquire();
			core.launch(schema, [=, order = cfi.order(), &cfi, &state, this](MemoryCache* cache) {
				const auto result = cache->exists(key, sort);
				auto v = View::copy(1);
				v.mutate()[0] = cfi.set(result, order);
				state.push(std::move(v), ParserInfo{
					.operand_idx = info.operand_idx,
					.operator_idx = op_idx
				});
				state.release();
			});
		}
		else if (op == cmd::qOp::Invert)
		{
			cfi.set_filter([](bool o, bool n) {
				return !n;
			});
		}
		else
		{
			return ~0ull;
		}
		return off;
	}
}
