#include <rdb_mount.hpp>
#include <rdb_types.hpp>
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

int main()
{
	std::filesystem::remove_all("/tmp/RDB/vcpu0");

	using user = rdb::Schema<"User",
		rdb::Topology<rdb::Field<"ID", rdbt::TimeUUID>>,
		rdb::Topology<
			rdb::Field<"Name", rdbt::String>,
			rdb::Field<"Flags", rdbt::Bitset<32>>,
			rdb::Field<"Address", rdbt::String>,
			rdb::Field<"Profile", rdbt::Uint64>,
			rdb::Field<"Nonce", rdbt::Uint32>,
			rdb::Field<"Password", rdbt::BinaryArray<32>>,
			rdb::Field<"Salt", rdbt::BinaryArray<16>>,
			rdb::Field<"Auth", rdbt::Nullable<rdbt::BinaryArray<32>>>
		>
	>;
	using auth_key = rdb::Schema<"UserAuth",
		rdb::Topology<rdb::Field<"ID", rdbt::TimeUUID>>,
		rdb::Topology<rdb::Field<"Key", rdbt::BinaryArray<32>>>
	>;
	using user_lookup = rdb::Schema<"UserLookup",
		rdb::Topology<rdb::Field<"Name", rdbt::String>>,
		rdb::Topology<rdb::Field<"ID", rdbt::TimeUUID>>
	>;

	using message = rdb::Schema<"Message",
		rdb::Topology<rdb::Field<"ChannelID", rdbt::RandUUID>>,
		rdb::Topology<
			rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
			rdb::Field<"SenderID", rdbt::TimeUUID>,
			rdb::Field<"Flags", rdbt::Bitset<32>>,
			rdb::Field<"Message", rdbt::Binary>
		>
	>;

	rdb::require<message>();
	rdb::require<user>();
	rdb::require<user_lookup>();
	rdb::require<auth_key>();

	rdb::Mount::ptr mnt =
		rdb::Mount::make({
			.root = "/tmp/RDB",
			.mnt{
				.cores = 1
			}
		});
	mnt->start();

	bool result = true;
	mnt->query
		<< rdb::compose(
			rdb::check<user>(&result, rdb::uuid::uint128_t())
			   < rdb::exists
		)
		<< rdb::execute<>;
	std::cout << result << std::endl;

	mnt->run<user>([](rdb::MemoryCache* cache) {
		std::cout << cache->exists(0x00, nullptr) << std::endl;
	});

	// mnt->run<message>([](rdb::MemoryCache* cache) {
	// 	cache->clear();
	// });

	// auto send_message = [&](
	// 	const std::string& sender,
	// 	const rdb::uuid::uint128_t& channel,
	// 	const std::string& data)
	// {
	// 	mnt->query
	// 		<< rdb::create<message>(
	// 			rdb::uuid::uint128_t(),
	// 			rdbt::TimeUUID::id(),
	// 			rdbt::TimeUUID::id(),
	// 			rdbt::Bitset<32>::list(),
	// 			rdb::byte::sspan(data)
	// 		)
	// 		<< rdb::execute<>;
	// };

	// send_message("A", {}, "Hello World User B!");
	// send_message("B", {}, "Hello World User A!");

	// mnt->run<message>([](rdb::MemoryCache* cache) {
	// 	cache->flush();
	// });

	mnt->wait();
}
