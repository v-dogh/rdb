#include <rdb_log.hpp>
#include <rdb_locale.hpp>
#include <rdb_reflect.hpp>
#include <format>

namespace rdb
{
	void Log::_replay_shard(std::filesystem::path path, const std::function<void(WriteType, key_type, View, View)>& callback) noexcept
	{
		RuntimeSchemaReflection::RTSI& schema = RuntimeSchemaReflection::info(_schema);

		_current = _path/std::format("s{}", _shard++);
		_smap.open(_current.c_str());
		_smap.map(path);
		_smap.hint(Mapper::Access::Sequential);
		_shard_offset = 0;

		while (_shard_offset < _smap.size())
		{
			auto& off = _shard_offset;
			auto& shard = _smap;
			if (shard.memory()[off] == char(WriteType::Reserved))
			{
				break;
			}
			else if (shard.memory()[off] == char(WriteType::Remov) ||
					 shard.memory()[off] == char(WriteType::Reset))
			{
				const auto wtype = WriteType(shard.memory()[off]);
				off += sizeof(WriteType);
				const auto key = byte::sread<key_type>(shard.memory(), off);
				if (const auto keys = schema.skeys())
				{
					std::size_t size = 0;
					for (std::size_t i = 0; i < keys; i++)
					{
						RuntimeInterfaceReflection::RTII& info = schema.reflect_skey(0);
						size += info.storage(&shard.memory()[off + size]);
					}

					callback(
						wtype,
						key,
						View::view(shard.memory().subspan(
							off, size
						)),
						nullptr
					);
					off += size;
				}
				else
				{
					callback(
						wtype,
						key,
						nullptr,
						nullptr
					);
				}
			}
			else
			{
				const auto wtype = WriteType(shard.memory()[off]);
				off += sizeof(WriteType);
				const auto key = byte::sread<key_type>(shard.memory(), off);
				if (const auto keys = schema.skeys())
				{
					std::size_t size = 0;
					for (std::size_t i = 0; i < keys; i++)
					{
						RuntimeInterfaceReflection::RTII& info = schema.reflect_skey(0);
						size += info.storage(&shard.memory()[off + size]);
					}
					View skey = View::view(shard.memory().subspan(
						off, size
					));
					off += size;

					const auto length = byte::sread<std::uint32_t>(shard.memory(), off);
					callback(
						wtype,
						key,
						skey,
						View::view(shard.memory().subspan(
							off,
							length
						))
					);
					off += length;
				}
				else
				{
					const auto length = byte::sread<std::uint32_t>(shard.memory(), off);
					callback(
						wtype,
						key,
						nullptr,
						View::view(shard.memory().subspan(
							off,
							length
						))
					);
					off += length;
				}
			}
		}

		_smap.unmap();
	}

	void Log::snapshot(std::size_t id) noexcept
	{
		std::filesystem::path p = _path/std::format("snapshot{}", id);
		std::filesystem::create_directory(p);

		for (std::size_t i = 0; i < _shard; i++)
		{
			const auto name = std::format("s{}", i);
			std::filesystem::rename(_path/name, p/name);
		}

		_shard = 0;
		_shard_offset = 0;
		_current.clear();
	}
	void Log::mark(std::size_t id) noexcept
	{
		std::filesystem::remove_all(_path/std::format("snapshot{}", id));
	}
	void Log::log(WriteType type, key_type key, View sort, View data) noexcept
	{
		const auto req =
			(data != nullptr ? data.data().size() + sizeof(std::uint32_t) : 0)
			+ sizeof(type) + sizeof(key) + sort.size();
		if (_current.empty() ||
			req + _shard_offset > _smap.size())
		{
			if (_shard_offset < _smap.size())
			{
				_smap.write(
					_shard_offset,
					char(WriteType::Reserved)
				);
			}
			_current = _path/std::format("s{}", _shard++);
			_smap.open(_current.c_str());
			_smap.reserve(_cfg->logs.log_shard_size);
			_shard_offset = 0;
		}

		// Each log includes
		// 1. The type of the write
		// 2. The partition key
		// 3. The sort key
		// 4. The length of the data
		// 5. The data
		// Sort key lengths are derived using RTSI and RTII
		// In case a power failure occurs during a replay (or a flush) the new flush is discarded and the logs are still replayed
		// If we need a new shard but we still have bytes left, we append a WriteType::Reserved (since it is 0 we do not explicitly do that)

		const auto skey = byte::byteswap_for_storage<key_type>(key);
		if (data != nullptr)
		{
			const auto len = byte::byteswap_for_storage<std::uint32_t>(data.size());
			_smap.write(_shard_offset + 1, std::array{
				View::view(byte::tspan(skey)),
				sort,
				View::view(byte::tspan(len)),
				data
			});
		}
		else
		{
			_smap.write(_shard_offset + 1, std::array{
				View::view(byte::tspan(skey)),
				sort
			});
		}

		// Separate into two writes to make sure the logs don't get corrupted during power failure
		// By default logs are zero'ed (filled with WriteType::Reserved)
		// So if power fails this block is just skipped

		_smap.write(_shard_offset, char(type));
		_shard_offset += req;
	}
	void Log::replay(std::function<void(WriteType, key_type, View, View)> callback) noexcept
	{
		std::size_t max = 0;
		std::vector<std::pair<std::size_t, std::size_t>> snapshots;
		for (decltype(auto) it : std::filesystem::directory_iterator(_path))
		{
			if (it
					.path()
					.filename()
					.string()
					.starts_with("snapshot")
				)
			{
				constexpr auto ssize = std::string_view("snapshot").size();
				const auto id = std::stoul(it
					.path()
					.filename()
					.string()
					.substr(ssize));

				for (decltype(auto) s : std::filesystem::directory_iterator(it))
				{
					const auto shard = std::stoul(s
						.path()
						.filename()
						.string()
						.substr(1));
					snapshots.push_back({
						id,
						shard
					});
				}
			}
			else
			{
				max = std::max(std::stoul(it.path()
					.filename()
					.string()
					.substr(1)) + 1, max);
			}
		}

		// If we have snapshots we merge them with the root logs (and shift root logs at the end)
		// Else we just replay the root logs

		if (!snapshots.empty())
		{
			std::sort(snapshots.begin(), snapshots.end(), [](const auto& lhs, const auto& rhs) {
				const auto& [ id1, s1 ] = lhs;
				const auto& [ id2, s2 ] = rhs;
				if (id1 == id2)
					return s1 < s2;
				return id1 < id2;
			});

			for (std::size_t i = max; i != 0; i--)
			{
				const auto from = _path/std::format("s{}", i);
				const auto to = _path/std::format("s{}", snapshots.size() + i - 1);
				if (std::filesystem::exists(from))
					std::filesystem::rename(from, to);
			}

			for (std::size_t i = snapshots.size() - 1;; i--)
			{
				const auto snap = std::format("snapshot{}", snapshots[i].first);
				const auto from = _path/snap/std::format("s{}", snapshots[i].second);
				const auto to = _path/std::format("s{}", i);
				std::filesystem::rename(from, to);

				if (i == 0 || snapshots[i - 1].first != snapshots[i].first)
				{
					std::filesystem::remove_all(_path/snap);
				}
				if (i == 0)
					break;
			}

			for (std::size_t i = 0; i < snapshots.size() + max; i++)
			{
				_replay_shard(_path/std::format("s{}", i), callback);
			}
			_shard = max + snapshots.size();
		}
		else
		{
			for (std::size_t i = 0; i < max; i++)
			{
				_replay_shard(_path/std::format("s{}", i), callback);
			}
			_shard = max;
		}
	}
}
