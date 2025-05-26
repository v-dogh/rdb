#include <iostream>
#include <rdb_mount.hpp>
#include <rdb_types.hpp>

int main()
{
	// using schema = rdb::Schema<"Message",
	// 	rdb::Topology<
	// 		rdb::Field<"ID", rdbt::TimeUUID, rdb::FieldType::Sort>,
	// 		rdb::Field<"ChannelID", rdbt::VRandUUID, rdb::FieldType::Partition>,
	// 		rdb::Field<"SenderID", rdbt::Hash>,
	// 		rdb::Field<"Flags", rdbt::Bitset<32>>,
	// 		rdb::Field<"Message", rdbt::Binary>
	// 	>
	// >;
	using schema = rdb::Schema<"Message",
		rdb::Topology<
			rdb::Field<"A", rdbt::Uint64, rdb::FieldType::Partition>,
			rdb::Field<"Q", rdbt::String>,
			rdb::Field<"B", rdbt::Uint32>,
			rdb::Field<"C", rdbt::Uint32>
		>
	>;
	rdb::require<schema>();

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

	mnt->query
		<< rdb::compose(
			rdb::fetch<schema>(0)
			| rdb::reset
			// | rdb::write<"B">(1)
			// | rdb::write<"C">(2)
			// | rdb::write<"Q">((const char*)("abcd"))
		)
		<< rdb::execute<>;

	rdb::TypedView<rdbt::String> result = nullptr;
	mnt->query
		<< rdb::compose(
			rdb::fetch<schema>(0)
			| rdb::read<"Q">(&result)
		)
		<< rdb::execute<>;
	std::cout << result->print() << std::endl;

	mnt->run<schema>([](rdb::MemoryCache* cache) {
		cache->flush();
	});

	mnt->wait();
}
