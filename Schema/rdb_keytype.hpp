#ifndef RDB_KEYTYPE_H
#define RDB_KEYTYPE_H

#include <cstdint>

namespace rdb
{
	using version_type = std::uint16_t;
	using schema_type = std::uint32_t;
	using hash_type = std::uint64_t;
	using key_type = hash_type;
	using ucode_type = std::uint32_t;
}

#endif // RDB_KEYTYPE_H
