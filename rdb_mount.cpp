#include <rdb_mount.hpp>
#include <rdb_reflect.hpp>
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
	std::size_t Mount::cores() const noexcept
	{
		return _threads.size();
	}

	void Mount::start() noexcept
	{
		std::lock_guard lock(_mtx);
		if (_status == Status::Running)
			return;

		_threads.resize(_cfg.mnt.cores + 1);
		for (std::size_t i = 0; i < _cfg.mnt.cores + 1; i++)
		{	
			_threads[i].sem = Thread::make_sem();
			_threads[i].thread = std::thread([this, i]() {
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

				std::unordered_map<schema_type, MemoryCache> schemas;

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

	std::size_t Mount::_vcpu(std::uint32_t key) const noexcept
	{
		return key % _cfg.mnt.cores;
	}

	void Mount::query_sync(std::span<const unsigned char> packet, QueryEngine::ReadChainStore::ptr store) noexcept
	{
		thread_local std::aligned_storage_t<32, alignof(ParserState::fragment)> pool;
		std::pmr::monotonic_buffer_resource resource(&pool, sizeof(pool));
		ParserState state(&resource, std::move(store));
		ParserInfo inf{};

		std::size_t off = 0;
		while (packet.size() - off >= 2)
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
			state.store->handlers[
				it.first.operand_idx - 1
			](it.first.operator_idx - 1, it.second);
		}
	}
	std::future<void> Mount::query_async(std::span<const unsigned char> packet, QueryEngine::ReadChainStore::ptr store) noexcept
	{
		return std::future<void>();
	}

	std::size_t Mount::_query_parse_operand(std::span<const unsigned char> packet, ParserState& state, ParserInfo inf) noexcept
	{
		const auto op = cmd::qOp(packet[0]);
		std::size_t off = sizeof(cmd::qOp);
		if (op == cmd::qOp::Fetch)
		{
			if (packet.size() < sizeof(cmd::qOp) + sizeof(key_type) + sizeof(schema_type))
				return packet.size() - off;

			const auto key = byte::sread<key_type>(packet.data(), off);
			const auto schema = byte::sread<schema_type>(packet.data(), off);
			const auto* info = RuntimeSchemaReflection::fetch(schema);
			if (info == nullptr)
				return packet.size() - off;

			View sort = nullptr;
			if (info->skeys())
			{
				std::size_t size = 0;
				for (std::size_t i = 0; i < info->skeys(); i++)
				{
					size += info->reflect_skey(i).storage(
						packet.data() + off + size
					);
					if (size > packet.size())
						return ~0ull;
				}
				sort = View::view(packet.subspan(off, size));
				off += size;
			}

			std::size_t coff = 0;
			while ((coff = _query_parse_schema_operator(
					packet.subspan(off),
					key, sort,
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
		return off;
	}
	std::size_t Mount::_query_parse_schema_operator(std::span<const unsigned char> packet, key_type key, View sort, schema_type schema, ParserState& state, ParserInfo& info) noexcept
	{
		const auto op = cmd::qOp(packet[0]);
		auto& core = _threads[_vcpu(key)];
		std::size_t off = sizeof(cmd::qOp);
		if (op == cmd::qOp::Reset)
		{
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				cache->reset(key, sort);
				state.release();
			}));
		}
		else if (op == cmd::qOp::Remove)
		{

		}
		else if (op == cmd::qOp::Write)
		{
			off += sizeof(std::uint8_t);
			const auto len = byte::sread<std::uint32_t>(packet.data(), off);

			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				cache->write(
					WriteType::Field,
					key, sort,
					packet.subspan(
						off - sizeof(std::uint8_t),
						len + sizeof(std::uint8_t)
					)
				);
				state.release();
			}));
			off += len;
		}
		else if (op == cmd::qOp::Read)
		{
			info.operator_idx++;
			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				state.push(cache->read(key, sort, 1 << packet[1]), info);
				state.release();
			}));
		}
		else if (op == cmd::qOp::WProc)
		{
			off += sizeof(std::uint8_t) + sizeof(proc_opcode);
			const auto len = byte::sread<std::uint32_t>(packet.data(), off);
			off += len;

			state.acquire();
			core.launch(Thread::task(schema, [=, &state, this](MemoryCache* cache) {
				cache->write(
					WriteType::WProc,
					key, sort,
					packet.subspan(sizeof(cmd::qOp), sizeof(std::uint8_t) + sizeof(proc_opcode) + len)
				);
				state.release();
			}));
		}
		else if (op == cmd::qOp::RProc)
		{

		}
		return off;
	}
}
