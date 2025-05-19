#ifndef RDB_LOG_HPP
#define RDB_LOG_HPP

#include <filesystem>
#include <functional>
#include "rdb_root_config.hpp"
#include "rdb_utils.hpp"
#include "rdb_mapper.hpp"
#include "rdb_writetype.hpp"

namespace rdb
{
	// Lock-free
	// This class is responsible for logging any writes and replaying them to the memory cache after a runtime failure
	// Logs are append only and grow in blocks of constant size
	// When a flush occurs logs are snapshotted and then atomically removed when done
	// Snapshots are replayed in ascending order before the local shards
	// Each log contains a: byte (WriteType, Reserved indicates the end), uint32 (length), data
	class Log
	{
	private:
		schema_type _schema{ 0 };
		std::size_t _shard{ 0 };
		std::size_t _shard_offset{ 0 };
		Mapper _smap{};
		Config* _cfg{ nullptr };
		std::filesystem::path _path{};
		std::filesystem::path _current{};

		void _replay_shard(std::filesystem::path path, const std::function<void(WriteType, key_type, View, View)>& callback) noexcept;
	public:
		Log() = default;
		Log(Config* cfg, std::filesystem::path path, schema_type schema) :
			_cfg(cfg),
			_path(path),
			_schema(schema)
		{}
		Log(const Log&) = delete;
		Log(Log&&) = default;

		void snapshot(std::size_t id) noexcept;
		void mark(std::size_t id) noexcept;
		void log(WriteType type, key_type key, View sort, View data = nullptr) noexcept;
		void replay(std::function<void(WriteType, key_type, View, View)> callback) noexcept;

		Log& operator=(const Log&) = delete;
		Log& operator=(Log&&) = default;
	};
}

#endif // RDB_LOG_HPP
