#include <rdb_ctl.hpp>

namespace rdb
{
	CTL::CTL(Mount::ptr mnt) : _mnt(mnt)
	{
		expose_procedure<word, variadic>("var.set", std::function([this](const word& name, const variadic& value) -> std::string {
			const auto f = _variables.find(name);
			if (f == _variables.end())
				return std::format("Invalid variable: {}", name.substr());
			f->second.first(value.args);
			return "";
		}));
		expose_procedure<word>("var.get", std::function([this](const word& name) -> std::string {
			const auto f = _variables.find(name);
			if (f == _variables.end())
				return std::format("Invalid variable: {}", name.substr());
			return f->second.second();
		}));
		hook_mount();
		hook_memory_cache();
	}

	void CTL::hook_mount() noexcept
	{
		auto& cfg = mnt()->cfg();

		// Logs

		expose_variable("cfg.logs.shardSize", &cfg.logs.log_shard_size);

		// Memory cache

		expose_variable("cfg.cache.blockSize", &cfg.cache.block_size);
		expose_variable("cfg.cache.blockSparseIndexRatio", &cfg.cache.block_sparse_index_ratio);
		expose_variable("cfg.cache.partitionSparseIndexRatio", &cfg.cache.partition_sparse_index_ratio);
		expose_variable("cfg.cache.flushPressure", &cfg.cache.flush_pressure);
		expose_variable("cfg.cache.compactionFoldRatio", &cfg.cache.compaction_fold_ratio);
		expose_variable("cfg.cache.compactionPressure", &cfg.cache.compaction_pressure);
		expose_variable("cfg.cache.maxDescriptors", &cfg.cache.max_descriptors);
		expose_variable("cfg.cache.maxMappings", &cfg.cache.max_mappings);
		expose_variable("cfg.cache.compressionRatio", &cfg.cache.compression_ratio);
		expose_variable("cfg.cache.partitionBloomFP", &cfg.cache.partition_bloom_fp_rate);
		expose_variable("cfg.cache.intraPartitionBloomFP", &cfg.cache.intra_partition_bloom_fp_rate);
		// expose_variable("cfg.cache.cacheType", &cfg.cache.cache_type);
		expose_variable("cfg.cache.maxCacheVolume", &cfg.cache.max_cache_volume);

		// Mount

		expose_variable("cfg.mnt.cores", &cfg.mnt.cores);
		expose_variable("cfg.mnt.numa", &cfg.mnt.numa);

		// Controls

		expose_procedure("mnt.start", std::function([this]() {
			mnt()->start();
		}));
		expose_procedure("mnt.stop", std::function([this]() {
			mnt()->stop();
		}));
	}
	void CTL::hook_memory_cache() noexcept
	{
		expose_procedure<string>("cache.flush", std::function([this](const string& schema) {
			mnt()->run(uuid::hash<schema_type>(schema), [&](rdb::MemoryCache* ptr) {
				ptr->flush();
			});
		}));
		expose_procedure<string, integer>("cache.core.flush", std::function([this](const string& schema, const integer& core) {
			mnt()->run(uuid::hash<schema_type>(schema), core, [&](rdb::MemoryCache* ptr) {
				ptr->flush();
			});
		}));
		expose_procedure<string>("cache.handles", std::function([this](const string& schema) {
			std::vector<std::size_t> handles;
			std::string str;
			std::atomic<std::size_t> ctr;

			handles.resize(mnt()->cores());
			str.reserve(handles.size() * 28);

			mnt()->run(uuid::hash<schema_type>(schema), [&](rdb::MemoryCache* ptr) {
				handles[ptr->core()] = ptr->descriptors();
				++ctr;
			});

			while (ctr != handles.size()) util::spinlock_yield();

			for (std::size_t i = 0; i < handles.size(); i++)
				str += std::format("Core{}: {} handles\n", i, handles[i]);
			str += std::format(
				"Total: {} handles",
				std::accumulate(handles.begin(), handles.end(), 0)
			);

			return str;
		}));
		expose_procedure<string>("cache.pressure", std::function([this](const string& schema) {
			std::vector<std::size_t> pressures;
			std::string str;
			std::atomic<std::size_t> ctr;

			pressures.resize(mnt()->cores());
			str.reserve(pressures.size() * 28);

			mnt()->run(uuid::hash<schema_type>(schema), [&](rdb::MemoryCache* ptr) {
				pressures[ptr->core()] = ptr->pressure();
				++ctr;
			});

			while (ctr != pressures.size()) util::spinlock_yield();

			for (std::size_t i = 0; i < pressures.size(); i++)
				str += std::format("Core{}: {}b\n", i, pressures[i]);
			str += std::format(
				"Total: {}b",
				std::accumulate(pressures.begin(), pressures.end(), 0)
			);

			return str;
		}));
	}

	std::string CTL::eval(std::string_view str) noexcept
	{
		const auto f = str.find(' ');
		const auto off = f == std::string::npos ? str.size() : f;
		const auto ff = _procedures.find(std::string(
			str.begin(), str.begin() + off
		));
		if (ff == _procedures.end())
			return std::format("Invalid procedure: {}", str.substr(0, off));
		return ff->second(str.substr(off));
	}
}
