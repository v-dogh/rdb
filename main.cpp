#include <iostream>
#include <rdb_mount.hpp>
#include <rdb_types.hpp>

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

int main()
{
	std::filesystem::remove_all("/tmp/RDB/vcpu0");

	using message = rdb::Schema<"Message",
		rdb::Topology<rdb::Field<"ChannelID", rdbt::VRandUUID>>,
		rdb::Topology<
			rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
			rdb::Field<"SenderID", rdbt::Hash>,
			rdb::Field<"Flags", rdbt::Bitset<32>>,
			rdb::Field<"Message", rdbt::Binary>
		>
	>;
	using schema = rdb::Schema<"Message",
		rdb::Topology<rdb::Field<"A", rdbt::Uint64>>,
		rdb::Topology<
			rdb::Field<"Q", rdbt::String>,
			rdb::Field<"B", rdbt::Uint32>,
			rdb::Field<"C", rdbt::Uint32, rdb::FieldType::Sort>
		>
	>;

	rdb::require<schema>();
	rdb::require<message>();

	rdb::Mount::ptr mnt =
		rdb::Mount::make({
			.root = "/tmp/RDB",
			.mnt{
				.cores = 1
			}
		});
	mnt->start();

	mnt->run<schema>([](rdb::MemoryCache* cache) {
		cache->clear();
	});

	auto send_message = [&](
		const std::string& sender,
		const rdb::uuid::uint128_t& channel,
		const std::string& data)
	{
		mnt->query
			<< rdb::compose(
				rdb::fetch<message>(rdbt::RandUUID::value_type(), rdbt::TimeUUID::value_type())
				| rdb::reset
			)
			<< rdb::execute<>;
	};

	// send_message("A", {}, "Hello World User B!");
	// send_message("B", {}, "Hello World User A!");

	// mnt->query
	// 	<< rdb::compose(
	// 		rdb::fetch<schema>(0, 0xFA)
	// 		| rdb::reset
	// 		| rdb::write<"B">(0xFB)
	// 		// | rdb::write<"C">(0xFA)
	// 		| rdb::write<"Q">((const char*)("abcd"))
	// 	)
	// 	<< rdb::execute<>;

	// rdb::TypedView<rdbt::String> result = nullptr;
	// mnt->query
	// 	<< rdb::compose(
	// 		rdb::fetch<schema>(0, 0xFA)
	// 		| rdb::read<"Q">(&result)
	// 	)
	// 	<< rdb::execute<>;
	// std::cout << result->print() << std::endl;

	mnt->run<schema>([](rdb::MemoryCache* cache) {
		cache->flush();
	});

	mnt->wait();
}
