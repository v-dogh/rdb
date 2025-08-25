#ifndef RDB_MEMUNITS_HPP
#define RDB_MEMUNITS_HPP

#include <cstddef>

namespace rdb::mem
{
	constexpr std::size_t B(std::size_t cnt) noexcept
	{
		return cnt;
	}
	constexpr std::size_t KiB(std::size_t cnt) noexcept
	{
		return cnt * 1024ull;
	}
	constexpr std::size_t MiB(std::size_t cnt) noexcept
	{
		return cnt * 1024ull * 1024ull;
	}
	constexpr std::size_t GiB(std::size_t cnt) noexcept
	{
		return cnt * 1024ull * 1024ull * 1024ull;
	}
}

#endif // RDB_MEMUNITS_HPP
