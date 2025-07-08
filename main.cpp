#include <random>
#include <rdb_dbg.hpp>
#include <rdb_mount.hpp>
#include <rdb_types.hpp>
#include <rdb_ctl.hpp>
#include <iostream>

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
// ISSWUES
// bruv, idk what is happening
// does _read_impl assume that the sort key is alongside each instance's data buffer (after the DataType)???
// Does _data_impl ensure that it is present in each of the instances on disk???

int main()
{
	std::filesystem::remove_all("/tmp/RDB");

	// using message = rdb::Schema<"Message",
	// 	rdb::Topology<
	// 		rdb::Field<"ServerID", rdbt::RandUUID>,
	// 		rdb::Field<"ChannelID", rdbt::RandUUID>,
	// 		rdb::Field<"DayBucket", rdbt::Timestamp>
	// 	>,
	// 	rdb::Topology<
	// 		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort, rdb::Order::Descending>,
	// 		rdb::Field<"SenderID", rdbt::TimeUUID>,
	// 		rdb::Field<"Flags", rdbt::Bitset<32>>,
	// 		rdb::Field<"Message", rdbt::Binary>,
	// 		rdb::Field<"Resources", rdbt::Nullable<rdbt::Buffer<rdbt::TimeUUID>>>
	// 	>
	// >;
	// rdb::require<message>();

	using test = rdb::Schema<"Test",
		rdb::Topology<
			rdb::Field<"Key", rdbt::String>
		>,
		rdb::Topology<
			rdb::Field<"Val", rdbt::String, rdb::FieldType::Sort>,
			rdb::Field<"Val2", rdbt::Uint64>
		>
	>;
	rdb::require<test>();

	rdb::Mount::ptr mnt =
		rdb::Mount::make({
			.root = "/tmp/RDB",
			.mnt{
				.cores = 1 // std::thread::hardware_concurrency()
			}
		});
	mnt->start();

	mnt->query
		<< rdb::create<test>(std::string_view("Key"), std::string_view("SKey"), 1)
		<< rdb::execute<>;

	mnt->run<test>([](rdb::MemoryCache* cache) {
		cache->flush();
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));

	test::interface_view<"Val2"> result;
	mnt->query
		<< rdb::compose(
			rdb::fetch<test>(std::string_view("Key"), std::string_view("SKey"))
			| rdb::read<"Val2">(&result)
		)
		<< rdb::execute<>;

	std::cout
		<< (result == nullptr ? std::string("<null>") : result->print())
		<< std::endl;

	// rdb::CTL::ptr ctl = rdb::CTL::make(mnt);
	// {
	// 	std::thread([=]() {
	// 		std::string in;
	// 		while (std::getline(std::cin, in))
	// 			std::cout << " -> " << ctl->eval(in) << std::endl;
	// 	}).detach();
	// }

	// auto send_message = [&](
	// 	const std::string& sender,
	// 	const rdb::uuid::uint128_t& server,
	// 	const rdb::uuid::uint128_t& channel,
	// 	const std::string& data)
	// {
	// 	const auto id = rdbt::TimeUUID::id();
	// 	mnt->query
	// 		<< rdb::create<message>(
	// 			server,
	// 			channel,
	// 			rdbt::Timestamp::now(rdbt::Timestamp::Round::Day),
	// 			id,
	// 			rdb::uuid::uint128_t(),
	// 			rdbt::Bitset<32>::list(),
	// 			rdb::byte::sspan(data),
	// 			rdbt::null
	// 		)
	// 		<< rdb::execute<>;
	// 	return id;
	// };

	// // Some samples
	// rdb::uuid::uint128_t id;
	// {
	// 	id = send_message("A", rdb::uuid::uint128_t(), rdb::uuid::uint128_t(), "Hello World");
	// 	send_message("B", {}, {}, "This is a sample message");
	// 	send_message("A", {}, {}, "This is another sample message");
	// 	send_message("B", {}, {}, "This message is indeed yet another sample message");
	// }
	// // Disk reads
	// {
	// 	// Show the amount of data in the memory cache at this moment
	// 	std::cout << ctl->eval("cache.pressure 'Message'") << std::endl;
	// 	ctl->eval("cache.flush 'Message'");

	// 	// Wait to make sure that the flush is not running (since while the flush is running all data is accessible from memory)
	// 	std::this_thread::sleep_for(std::chrono::seconds(2));

	// 	message::interface_view<"Message"> result;
	// 	mnt->query
	// 		<< rdb::compose(
	// 			rdb::fetch<message>(
	// 				// Partition key
	// 				rdb::uuid::uint128_t(),
	// 				rdb::uuid::uint128_t(),
	// 				rdbt::Timestamp::now(rdbt::Timestamp::Round::Day),
	// 				// Sorting key
	// 				id
	// 			)
	// 			| rdb::read<"Message">(&result)
	// 		)
	// 		<< rdb::execute<>;

	// 	// Handles to disk flushes should be cached
	// 	std::cout << ctl->eval("cache.handles 'Message'") << std::endl;

	// 	if (result == nullptr)
	// 		std::cout << "Not Found" << std::endl;
	// 	else
	// 		std::cout << result->print() << std::endl;
	// }
	// // Send some more
	// {
	// 	send_message("A", {}, {}, "Some zeroes??? 000000000");
	// 	send_message("B", {}, {}, "Fr fr much appreciated");
	// 	send_message("B", {}, {}, "0000000000000000000000000000000000000000");
	// }
	// // Paging
	// {
	// 	rdb::TableList<message> li;
	// 	mnt->query
	// 		<< rdb::page<message>(
	// 			&li, 10,
	// 			rdb::uuid::uint128_t(), rdb::uuid::uint128_t(),
	// 			rdbt::Timestamp::now(rdbt::Timestamp::Round::Day)
	// 		)
	// 		<< rdb::execute<>;

	// 	for (decltype(auto) it : li)
	// 		std::cout << it.print() << std::endl;
	// }

	mnt->wait();
}

// Stress test <read>
// if (false)
// {
// 	constexpr auto messages = 100'000ul;
// 	constexpr auto rounds = 50ul;

// 	std::vector<rdb::uuid::uint128_t> ids;
// 	std::random_device dev;
// 	std::mt19937 rng(dev());
// 	std::uniform_int_distribution<int> dist;

// 	ids.reserve(messages);
// 	auto pop_step = [&] {
// 		if (ids.size() < messages)
// 		{
// 			const auto dest = messages; // std::min(std::max(ids.size() * 2, 10ul), messages);
// 			while (ids.size() != dest)
// 				ids.emplace_back(send_message(
// 					"", { .low = static_cast<std::uint64_t>(dist(rng)), .high = static_cast<std::uint64_t>(dist(rng)) }, {}, ""
// 				));
// 			dist = std::uniform_int_distribution<int>(0, ids.size() - 1);
// 		}
// 	};

// 	std::cout << "Beginning Test" << std::endl;

// 	bool stop = false;
// 	while (!stop)
// 	{
// 		pop_step();
// 		std::vector<std::chrono::high_resolution_clock::duration> times;
// 		times.reserve(rounds);
// 		for (std::size_t i = 0; i < rounds; i++)
// 		{
// 			const auto start = std::chrono::high_resolution_clock::now();
// 			for (std::size_t j = 0; j < ids.size(); j++)
// 			{
// 				// message::interface_view<"Flags"> result;
// 				// mnt->query
// 				// 	<< rdb::compose(
// 				// 		rdb::fetch<message>(
// 				// 			rdb::uuid::uint128_t(), rdb::uuid::uint128_t(),
// 				// 			decltype(std::chrono::high_resolution_clock::now().time_since_epoch().count())(), // rdbt::Timestamp::now(rdbt::Timestamp::Round::Day),
// 				// 			ids[dist(rng)]
// 				// 		)
// 				// 		| rdb::read<"Flags">(&result)
// 				// 	)
// 				// 	<< rdb::execute<>;
// 			}
// 			const auto end = std::chrono::high_resolution_clock::now();
// 			const auto dur = end - start;
// 			times.emplace_back(dur);
// 		}
// 		const auto dur = std::accumulate(
// 			times.begin(), times.end(),
// 			std::chrono::high_resolution_clock::duration(0)
// 		);
// 		const auto avg = dur / rounds;

// 		std::cout
// 			<< "Ins[" << ids.size() << "m][" << rounds << "]: \n"
// 			<< "DUR/AVG - " << avg << '\n'
// 			<< "OP /AVG - " << (avg / ids.size()) << '\n'
// 			<< "IPS/AVG - " << std::size_t(ids.size() / std::chrono::duration_cast<std::chrono::duration<double>>(avg).count()) << "m/s"
// 			<< std::endl;

// 		stop = messages <= ids.size();
// 	}
// 	std::cout << "Done" << std::endl;
// }
