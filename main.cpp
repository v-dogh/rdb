#include <rdb_dbg.hpp>
#include <rdb_mount.hpp>
#include <rdb_types.hpp>
#include <iostream>
#include <random>

// READ PATH ISSUE
// eq_comparator (sort key wide partition reads) expects a full schema instance???
// but what if it's a field sequence or tombstone. Aren't we already preappending a sort key to each write
// no matter the type? Investigate (as it would be a significant overhead if we are writing full schemas which include a sort key)
// REWRITE QUERY OPERATION
// if we call rewrite we are allowed to construct our own table
// this may conflict with the key we pass to fetch (sort keys)
// solution 1. overwrite them no matter what, before sending
// solution 2. do some template stuff so that the sort key fields are constructed with the data from fetch
//             but the ::make function itself default initializes sort key fields and passes the args to the rest
// Handle wproc if not in cache, i.g. we would read apply and write to the memory cache
// Handle fragmented writes (i guess a new data type)
// FRAGMENTED DATA TYPES IN COMPOSABLE DATA TYPES
// Fail at compile time if a fragmented type is passed to a composable type
// I.e. buffers arrays tuples nullables etc.
// R/W OVERHEAD
// equality comparators seem to have a large overhead (rdb_reflect.cpp) due to the loops etc.
// mb just do it at compile time (in the schema) (i.e. generate the code with metaprogramming and pass to reflection)
// ISSWUES
// bruv, idk what is happening
// does _read_impl assume that the sort key is alongside each instance's data buffer (after the DataType)???
// Does _data_impl ensure that it is present in each of the instances on disk???

int main()
{
	// std::filesystem::remove_all("/tmp/RDB");

	using message = rdb::Schema<"Message",
		rdb::Topology<
			rdb::Field<"ServerID", rdbt::RandUUID>,
			rdb::Field<"ChannelID", rdbt::RandUUID>,
			rdb::Field<"DayBucket", rdbt::Timestamp>
		>,
		rdb::Topology<
			rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
			rdb::Field<"SenderID", rdbt::TimeUUID>,
			rdb::Field<"Flags", rdbt::Bitset<32>>,
			rdb::Field<"Message", rdbt::Binary>,
			rdb::Field<"Attachements", rdbt::Nullable<rdbt::Buffer<rdbt::TimeUUID>>>
		>
	>;
	rdb::require<message>();

	rdb::Mount::ptr mnt =
		rdb::Mount::make({
			.root = "/tmp/RDB",
			.mnt{
				.cores = std::thread::hardware_concurrency()
			}
		});
	mnt->start();

	auto send_message = [&](
		const std::string& sender,
		const rdb::uuid::uint128_t& server,
		const rdb::uuid::uint128_t& channel,
		const std::string& data)
	{
		const auto id = rdbt::TimeUUID::id();
		mnt->query
			<< rdb::create<message>(
				server,
				channel,
				rdbt::Timestamp::now(rdbt::Timestamp::Round::Day),
				id,
				rdb::uuid::uint128_t(),
				rdbt::Bitset<32>::list(),
				rdb::byte::sspan(data),
				rdbt::null
			)
			<< rdb::execute<>;
		return id;
	};

	if (false)
	{
		constexpr auto messages = 100'000ul;
		constexpr auto rounds = 50ul;

		std::vector<rdb::uuid::uint128_t> ids;
		std::random_device dev;
		std::mt19937 rng(dev());
		std::uniform_int_distribution<int> dist;

		ids.reserve(messages);
		auto pop_step = [&] {
			if (ids.size() < messages)
			{
				const auto dest = std::min(std::max(ids.size() * 2, 10ul), messages);
				while (ids.size() != dest)
					ids.emplace_back(send_message("", {}, {}, ""));
				dist = std::uniform_int_distribution<int>(0, ids.size() - 1);
			}
		};

		std::cout << "Beginning Test" << std::endl;

		bool stop = false;
		while (!stop)
		{
			pop_step();
			std::vector<std::chrono::high_resolution_clock::duration> times;
			times.reserve(rounds);
			for (std::size_t i = 0; i < rounds; i++)
			{
				const auto start = std::chrono::high_resolution_clock::now();
				for (std::size_t j = 0; j < ids.size(); j++)
				{
					message::interface_view<"Flags"> result;
					mnt->query
						<< rdb::compose(
							rdb::fetch<message>(
								rdb::uuid::uint128_t(), rdb::uuid::uint128_t(),
								rdbt::Timestamp::now(rdbt::Timestamp::Round::Day),
								ids[dist(rng)]
							)
							| rdb::read<"Flags">(&result)
						)
						<< rdb::execute<>;
				}
				const auto end = std::chrono::high_resolution_clock::now();
				const auto dur = end - start;
				times.emplace_back(dur);
			}
			const auto dur = std::accumulate(
				times.begin(), times.end(),
				std::chrono::high_resolution_clock::duration(0)
			);
			const auto avg = dur / rounds;

			std::cout
				<< "Ins[" << ids.size() << "m][" << rounds << "]: \n"
				<< "DUR/AVG - " << avg << '\n'
				<< "OP /AVG - " << (avg / ids.size()) << '\n'
				<< "IPS/AVG - " << std::size_t(ids.size() / std::chrono::duration_cast<std::chrono::duration<double>>(avg).count()) << "m/s"
				<< std::endl;

			stop = messages <= ids.size();
		}
		std::cout << "Done" << std::endl;
	}
	if (false)
	{
		send_message("A", {}, {}, "Hello World User B!");
		send_message("B", {}, {}, "Hello World User A!");
		send_message("A", {}, {}, "Such a good day User B!");
		send_message("B", {}, {}, "It is indeed User A!");
		send_message("A", {}, {}, "This message is for you only User B");
		send_message("B", {}, {}, "Much appreciated User A, I do believe in privacy");
		send_message("B", {}, {}, "Altough in the wake of digital technology it becomes harder and harder");

		rdb::TableList<message> li;
		mnt->query
			<< rdb::page<message>(
				&li, 10,
				rdb::uuid::uint128_t(), rdb::uuid::uint128_t(),
				rdbt::Timestamp::now(rdbt::Timestamp::Round::Day)
			)
			<< rdb::execute<>;

		for (decltype(auto) it : li)
			std::cout << it.print() << std::endl;

		mnt->run<message>([](rdb::MemoryCache* cache) {
			cache->flush();
		});
	}
	if (true)
	{
		message::interface_view<"Message"> result;
		mnt->query
			<< rdb::compose(
				rdb::fetch<message>(
					rdb::uuid::uint128_t(), rdb::uuid::uint128_t(),
					rdbt::Timestamp::now(rdbt::Timestamp::Round::Day),
					rdb::uuid::uint128_t{ .low = 0x00000e76b36b040b, .high = 0x003e341588495259,  },
					rdb::uuid::uint128_t()
				)
				| rdb::read<"Message">(&result)
			)
			<< rdb::execute<>;

		if (result == nullptr)
			std::cout << "Not Found" << std::endl;
		else
			std::cout << result->print() << std::endl;
	}

	mnt->wait();
}

// // {
// // 	using namespace std::chrono;

// // 	std::atomic<long long> total_ns{0};

// // 	constexpr std::size_t threads = 4;
// // 	constexpr std::size_t repeats_per_thread = 100;
// // 	constexpr std::size_t messages_per_run = 1000;

// // 	auto clear_cache = [&]() {
// // 		mnt->run<message>([](rdb::MemoryCache* cache) {
// // 			cache->clear();
// // 		});
// // 	};

// // 	// 🔁 Warm-up (single-threaded)
// // 	{
// // 		std::mt19937 rng{0x00};
// // 		std::uniform_int_distribution<std::size_t> dist(0, 10);
// // 		for (std::size_t i = 0; i < messages_per_run; i++) {
// // 			send_message("A", {}, {.high = dist(rng)}, "Hello World User B!");
// // 		}
// // 		clear_cache();
// // 	}

// // 	// 🧵 Worker function
// // 	auto worker = [&](int seed) {
// // 		std::mt19937 rng{ static_cast<unsigned long>(seed) };
// // 		std::uniform_int_distribution<std::size_t> dist(0, 10);

// // 		nanoseconds thread_time{0};

// // 		for (std::size_t i = 0; i < repeats_per_thread; i++) {
// // 			auto start = high_resolution_clock::now();
// // 			for (std::size_t j = 0; j < messages_per_run; j++) {
// // 				send_message("A", {}, {.high = dist(rng)}, "Hello World User B!");
// // 			}
// // 			auto end = high_resolution_clock::now();

// // 			thread_time += duration_cast<nanoseconds>(end - start);

// // 			clear_cache();
// // 		}

// // 		total_ns.fetch_add(thread_time.count(), std::memory_order_relaxed);
// // 	};

// // 	// 🚀 Launch threads
// // 	std::vector<std::thread> pool;
// // 	for (std::size_t i = 0; i < threads; i++) {
// // 		pool.emplace_back(worker, static_cast<int>(i * 1337)); // different seeds
// // 	}

// // 	for (auto& t : pool) t.join();

// // 	// 📊 Final stats
// // 	const std::size_t total_inserts = threads * repeats_per_thread * messages_per_run;
// // 	const auto avg_ns_per_insert = total_ns.load() / total_inserts;
// // 	const auto inserts_per_sec = (1'000'000'000ll) / avg_ns_per_insert;

// // 	std::cout << "Total inserts: " << total_inserts << "\n";
// // 	std::cout << "Average time per insert: " << avg_ns_per_insert << " ns\n";
// // 	std::cout << "Estimated inserts/sec: " << inserts_per_sec << "\n";
// // }
