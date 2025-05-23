#include <rdb_mount.hpp>
#include <rdb_types.hpp>
#include <iostream>

int main()
{
	auto buffer = rdbt::String::make('h', 'e', 'l');
	std::cout << buffer->print() << std::endl;

	// using schema = rdb::Schema<"test",
	// 	rdb::Keyset<rdb::Partition<"A">>,
	// 	rdb::Topology<
	// 		rdb::Field<"A", rdbt::Tuple<
	// 			rdbt::Uint8,
	// 			rdbt::Uint8
	// 		>>,
	// 		rdb::Field<"B", rdbt::Uint64>
	// 	>
	// >;
	// rdb::require<schema>();

	// rdb::Mount::ptr mnt =
	// 	rdb::Mount::make({
	// 		.root = "/tmp/RDB"
	// 	});
	// mnt->start();

	// rdb::TypedView<schema::interface<"A">> value = nullptr;
	// mnt->query
	// 	<< rdb::compose(
	// 		rdb::fetch<schema>(rdbt::Make(0, 0))
	// 		| rdb::read<"A">(&value)
	// 	)
	// 	<< rdb::execute<>;
	// std::cout << value->print() << std::endl;

	// mnt->run<schema>([](rdb::MemoryCache* cache) {
	// 	cache->flush();
	// });

	// mnt->wait();
}
