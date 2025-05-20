#ifndef RDB_TIME_HPP
#define RDB_TIME_HPP

#include <Types/rdb_scalar.hpp>

namespace rdb::type
{
	using Time = Scalar<"t64", std::chrono::system_clock::duration::rep>;
	using Timestamp = Scalar<"tp64", std::chrono::system_clock::time_point::rep>;
}

#endif // RDB_TIME_HPP
