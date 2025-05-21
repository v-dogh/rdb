#include <rdb_mount.hpp>
#include <Types/rdb_tuple.hpp>
#include <Types/rdb_scalar.hpp>
#include <iostream>

int main()
{
	namespace rdbt = rdb::type;
	using schema = rdb::Schema<"test",
		rdb::Keyset<rdb::Partition<"A">>,
		rdb::Topology<
			rdb::Field<"A", rdbt::Tuple<
				rdbt::Uint8,
				rdbt::Uint8
			>>,
			rdb::Field<"B", rdbt::Uint64>
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
			rdb::fetch<schema>(rdbt::MakeTuple(0, 0))
			| rdb::reset
		)
		<< rdb::execute<>;

	mnt->run<schema>([](rdb::MemoryCache* cache) {
		cache->flush();
	});

	mnt->wait();
}
