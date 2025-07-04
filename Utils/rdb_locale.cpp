#include <rdb_locale.hpp>

namespace rdb::byte
{
	std::strong_ordering binary_compare(std::span<const unsigned char> lhs, std::span<const unsigned char> rhs) noexcept
	{
		static constexpr std::array table = {
			std::strong_ordering::less,
			std::strong_ordering::equal,
			std::strong_ordering::greater
		};
		const auto result = std::memcmp(
			lhs.data(), rhs.data(),
			std::min(lhs.size(), rhs.size())
		);
		if (result != 0)
			return table[(0 < result) - (result < 0) + 1];
		const auto lcmp = int(lhs.size()) - int(rhs.size());
		return table[(0 < lcmp) - (lcmp < 0) + 1];
	}
	bool binary_equal(std::span<const unsigned char> lhs, std::span<const unsigned char> rhs) noexcept
	{
		return
			lhs.size() == rhs.size() &&
			std::memcmp(lhs.data(), rhs.data(), rhs.size()) == 0;
	}
}
