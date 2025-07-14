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
			enum class Type
			{
				ARC,
				LRU,
				LFU,
			};

			// The block size during a flush
			std::size_t block_size{ mem::KiB(64) };
			// The number entries to linearly scan after looking up a sparse index
			std::size_t block_sparse_index_ratio{ 64 };
			// The number entries to linearly scan after looking up a sparse index
			std::size_t partition_sparse_index_ratio{ 32 };
			// Ammount of data in the memory cache that triggers a flush (bytes)
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
			float partition_bloom_fp_rate{ 0.01f };
			// The average chance for a false positive in the intra-partition bloom filter
			float intra_partition_bloom_fp_rate{ 0.1f };
			// Query cache type (trigerred when a disk read is performed)
			Type cache_type{ Type::ARC };
			// Maximum allowed memory usage of the cache (bytes) (only considers data stored, not the total memory used by structures)
			std::size_t max_cache_volume{ mem::MiB(512) };
			// Whether page requests should be cached
			bool cache_page{ false };
		} cache;
		struct Mount
		{
			enum class CPUProfile
			{
				OptimizeSpeed,
				OptimizeUsage
			};

			// Number of cores for the database to distribute load
			std::size_t cores{ std::thread::hardware_concurrency() };
			// Whether to enable NUMA awareness
			bool numa{ true };
			// Optimizes for chosen qualities when it comes to CPU usage
			CPUProfile cpu_profile{ CPUProfile::OptimizeUsage };
		} mnt;
	};
}

#endif // RDB_ROOT_CONFIG_HPP
