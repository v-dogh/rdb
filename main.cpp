#include <iostream>
#include <rdb_mount.hpp>
#include <rdb_types.hpp>

int main()
{
	// using schema = rdb::Schema<"Message",
	// 	rdb::Keyset<
	// 		rdb::Partition<"ChannelID">,
	// 		rdb::Sort<"ID">
	// 	>,
	// 	rdb::Topology<
	// 		rdb::Field<"ID", rdbt::TimeUUID>,
	// 		rdb::Field<"ChannelID", rdbt::VRandUUID>,
	// 		rdb::Field<"SenderID", rdbt::Hash>,
	// 		rdb::Field<"Flags", rdbt::Bitset<32>>,
	// 		rdb::Field<"Message", rdbt::Binary>
	// 	>
	// >;
	using schema = rdb::Schema<"Message",
		rdb::Keyset<rdb::Partition<"A">>,
		rdb::Topology<
			rdb::Field<"A", rdbt::Uint64>
		>
	>;
	rdb::require<schema>();

	rdb::Mount::ptr mnt =
		rdb::Mount::make({
			.root = "/tmp/RDB"
		});
	mnt->start();

	mnt->query
		<< rdb::compose(
			rdb::fetch<schema>(0)
			| rdb::reset
		)
		<< rdb::execute<>;

	mnt->run<schema>([](rdb::MemoryCache* cache) {
		cache->flush();
	});

	mnt->wait();
}
