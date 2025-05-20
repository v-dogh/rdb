// #include <rdb_mount.hpp>
// #include <Types/rdb_scalar.hpp>
// #include <iostream>

// int main()
// {
// 	using schema = rdb::Schema<"test",
// 		rdb::Keyset<rdb::Partition<"A">, rdb::Sort<"B">, rdb::Order::Ascending>,
// 		rdb::Topology<
// 			rdb::Field<"A", rdb::type::Uint64>,
// 			rdb::Field<"B", rdb::type::Uint32>,
// 			rdb::Field<"C", rdb::type::Byte>
// 		>
// 	>;
// 	rdb::require<schema>();

// 	rdb::Mount::ptr mnt =
// 		rdb::Mount::make({
// 			.root = "/tmp/RDB"
// 		});
// 	mnt->start();

// 	// mnt->query
// 	// 	<< (rdb::fetch<schema>(0, 1) | rdb::reset)
// 	// 	<< rdb::execute<>;
// 	// mnt->run<schema>([](rdb::MemoryCache* cache) {
// 	// 	cache->flush();
// 	// });

// 	rdb::TypedView<rdb::type::Byte> value;
// 	mnt->query
// 		<< (rdb::fetch<schema>(0, 1) | rdb::read<"C">(&value))
// 		<< rdb::execute<rdb::Policy::Sync>;
// 		// << (rdb::fetch<schema>(0, 2) | rdb::reset | rdb::write<"C">(value))
// 		// << rdb::execute<rdb::Policy::Sync>;

// 	// mnt->query
// 	// 	<< rdb::compose(
// 	// 		rdb::fetch<schema>(0, 1)
// 	// 		| rdb::reset
// 	// 	)
// 	// 	<< rdb::execute<rdb::Policy::Sync>;

// 	// std::cout << "Query Time: " << rdb::util::measure([&]() {
// 	// 	rdb::TypedView<rdb::type::Uint64> value;
// 	// 	mnt->query
// 	// 		<< rdb::compose(
// 	// 			rdb::fetch<schema>(0, 1)
// 	// 			// | rdb::reset
// 	// 			// | rdb::write<"A">(64)
// 	// 			// | rdb::write<"B">(0)
// 	// 			// | rdb::write<"C">(0)
// 	// 			| rdb::read<"A">(&value)
// 	// 		)
// 	// 		<< rdb::execute<rdb::Policy::Sync>;
// 	// }, 10000) << std::endl;
// 	// RDB_FMT("Value: {}", value->value())

// 	mnt->wait();
// }

#include <Types/rdb_tuple.hpp>
#include <Types/rdb_scalar.hpp>
#include <iostream>

int main()
{
	namespace rdbt = rdb::type;
	using type = rdbt::Tuple<
		rdbt::Uint64,
		rdbt::Uint32
	>;

	rdb::TypedView<type> value = type::make(123, 456);
	std::cout << value->uname << ": " << value->print() << std::endl;
}
