#ifndef RDB_ROOT_CONFIG_HPP
#define RDB_ROOT_CONFIG_HPP

#include <rdb_memunits.hpp>
#include <filesystem>
#include <thread>

namespace rdb
{
	struct Config
	{
		// Root directory of the database
		std::filesystem::path root{ "/rdb/" };
		struct Logs
		{
			// The size of a single log shard (bytes)
			std::size_t log_shard_size{ 1024 * 1024 * 4 };
		} logs;
		struct Cache
		{
			// The block size during a flush
			std::size_t block_size{ mem::KiB(16) };
			// The space between indices inside blocks
			std::size_t sparse_index_ratio{ 10 };
			// Ammount of data in the memory cache that triggers a flush (bytes)
			std::size_t flush_pressure{ 1024 * 1024 * 8 };
			// Automatic compaction fold ratio determines how many flushes fold into a single flush
			std::size_t compaction_fold_ratio{ 4 };
			// How many flushes are allowed before forced compaction
			std::size_t compaction_pressure{ 32 };
			// Maximum open file descriptors
			std::size_t max_descriptors{ 4096 };
			// Maximum open mappings
			std::size_t max_mappings{ 8192 };
			// The ratio of compressed data to decompressed data below which we write a compressed block
			float compression_ratio{ 0.7f };
			// Whether to enable the query cache
			bool qcache{ true };
		} cache;
		struct Mount
		{
			// Number of cores for the database to distribute load
			std::size_t cores{ std::thread::hardware_concurrency() };
			// Whether to enable NUMA awareness
			bool numa{ false };
		} mnt;
	};
}

#endif // RDB_ROOT_CONFIG_HPP
