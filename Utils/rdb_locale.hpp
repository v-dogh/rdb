#ifndef RDB_LOCALE_HPP
#define RDB_LOCALE_HPP

#include <cstring>
#include <cstdint>
#include <bit>
#include <optional>
#include <span>
#include <rdb_utils.hpp>
#include <type_traits>

namespace rdb::byte
{
	template<typename Type> requires std::is_trivial_v<Type>
	constexpr std::span<const unsigned char> tspan(const Type& value) noexcept
	{
		return std::span(
			reinterpret_cast<const unsigned char*>(&value),
			sizeof(Type)
		);
	}

	template<typename Type>
	constexpr std::span<const unsigned char> sspan(const Type& value) noexcept
	{
		return std::span(
			reinterpret_cast<const unsigned char*>(value.data()),
			value.size() * sizeof(value[0])
		);
	}

	constexpr bool is_storage_endian() noexcept
	{
		return std::endian::native == std::endian::little;
	}

	template<typename Type>
	constexpr Type misaligned_load(Type value) noexcept requires std::is_trivial_v<Type>
	{
#		ifdef RDB_ALIGNED_READ
		if (reinterpret_cast<std::uintptr_t>(&value) % alignof(Type) != 0)
		{
			std::remove_cvref_t<Type> result;
			std::memcpy(&result, &value, sizeof(Type));
			return result;
		}
		else
		{
			return value;
		}
#		else
		return value;
#		endif
	}

	template<typename Type>
	constexpr Type byteswap(Type value) noexcept requires std::is_trivial_v<Type>
	{
		if constexpr (sizeof(Type) == 1)
		{
			return value;
		}
		else if constexpr (sizeof(Type) == 2)
		{
#				if defined(__GNUC__) || defined(__clang__)
				return __builtin_bswap16(value);
#				elif defined(_MSC_VER)
				return _byteswap_ushort(value);
#				else
				return static_cast<Type>(
					((value & 0x00FF) << 8) |
					((value & 0xFF00) >> 8)
				);
#				endif
		}
		else if constexpr (sizeof(Type) == 4)
		{
#				if defined(__GNUC__) || defined(__clang__)
				return __builtin_bswap32(value);
#				elif defined(_MSC_VER)
				return _byteswap_ulong(value);
#				else
				return static_cast<Type>(
					((value & 0x000000FF) << 24) |
					((value & 0x0000FF00) << 8)  |
					((value & 0x00FF0000) >> 8)  |
					((value & 0xFF000000) >> 24)
				);
#				endif
		}
		else if constexpr (sizeof(Type) == 8)
		{
#				if defined(__GNUC__) || defined(__clang__)
				return __builtin_bswap64(value);
#				elif defined(_MSC_VER)
				return _byteswap_uint64(value);
#				else
				return static_cast<Type>(
					((value & 0x00000000000000FFull) << 56) |
					((value & 0x000000000000FF00ull) << 40) |
					((value & 0x0000000000FF0000ull) << 24) |
					((value & 0x00000000FF000000ull) << 8)  |
					((value & 0x000000FF00000000ull) >> 8)  |
					((value & 0x0000FF0000000000ull) >> 24) |
					((value & 0x00FF000000000000ull) >> 40) |
					((value & 0xFF00000000000000ull) >> 56)
				);
#				endif
		}
		else if constexpr (sizeof(Type) == 16)
		{
			std::uint64_t& low = reinterpret_cast<std::uint64_t*>(value)[0];
			std::uint64_t& high = reinterpret_cast<std::uint64_t*>(value)[1];
			const auto bs_low = byteswap(low);
			const auto bs_high = byteswap(high);
			low = bs_low;
			high = bs_high;
			return value;
		}
		else
		{
			for (std::size_t i = 0; i < sizeof(Type) / 2; i++)
				std::swap(
					reinterpret_cast<unsigned char*>(&value)[i],
					reinterpret_cast<unsigned char*>(&value)[sizeof(Type) - i - 1]
				);
			return value;
		}
	}

	template<typename Type>
	constexpr Type byteswap(const void* value) noexcept
	{
		std::remove_cvref_t<Type> aligned{};
		if (reinterpret_cast<std::uintptr_t>(value) % alignof(Type) != 0)
			std::memcpy(&aligned, value, sizeof(Type));
		else
			aligned = *static_cast<const Type*>(value);
		return byteswap(aligned);
	}

	// Byteswap from host-endian to storage-endian (this one also checks for misalignment if toggled)

	template<typename Type>
	constexpr Type byteswap_for_storage(Type value) noexcept
	{
		if constexpr (is_storage_endian())
			return misaligned_load(value);
		else
			return byteswap(misaligned_load(value));
	}

	template<typename Type>
	constexpr Type byteswap_for_storage(const void* value) noexcept
	{
		if constexpr (is_storage_endian())
		{
			std::remove_cvref_t<Type> aligned{};
			if (reinterpret_cast<std::uintptr_t>(value) % alignof(Type) != 0)
				std::memcpy(&aligned, value, sizeof(Type));
			else
				aligned = *static_cast<const Type*>(value);
			return aligned;
		}
		else
			return byteswap<Type>(value);
	}

	// Byteswap from storage-endian to sort-endian

	template<typename Type>
	constexpr Type byteswap_for_sort(Type value) noexcept
	{
		// To big-endian since ART requires big endian keys
		return byteswap(value);
	}

	template<typename Type>
	constexpr Type byteswap_for_sort(const void* value) noexcept
	{
		return byteswap<Type>(value);
	}

	template<typename Type>
	auto write(std::span<unsigned char> buffer, Type value) noexcept
	{
		if constexpr (std::is_trivial_v<Type>)
		{
			std::memcpy(buffer.data(), &value, sizeof(value));
			return sizeof(Type);
		}
		else
		{
			const auto s = std::span(value);
			if (!s.empty()) std::memcpy(buffer.data(), s.data(), s.size());
			return s.size();
		}

	}
	template<typename Type>
	auto write(std::span<unsigned char> buffer, std::size_t off, Type value) noexcept
	{
		return write(buffer.subspan(off), value);
	}
	template<typename Type>
	auto write(unsigned char* buffer, Type value) noexcept
	{
		return write(std::span(buffer, std::dynamic_extent), value);
	}

	template<typename Type>
	auto swrite(std::span<unsigned char> buffer, Type value) noexcept
	{
		if constexpr (std::is_trivial_v<Type>)
		{
			value = byte::byteswap_for_storage(value);
			std::memcpy(buffer.data(), &value, sizeof(value));
			return sizeof(Type);
		}
		else
		{
			const auto s = std::span(value);
			if (!s.empty()) std::memcpy(buffer.data(), s.data(), s.size());
			return s.size();
		}
	}
	template<typename Type>
	auto swrite(std::span<unsigned char> buffer, std::size_t off, Type value) noexcept
	{
		return swrite(buffer.subspan(off), value);
	}
	template<typename Type>
	auto swrite(unsigned char* buffer, Type value) noexcept
	{
		return swrite(std::span(buffer, std::dynamic_extent), value);
	}

	template<typename Type>
	auto read(std::span<const unsigned char> buffer, std::size_t& ctr) noexcept
	{
		std::remove_cvref_t<Type> aligned{};
		if (reinterpret_cast<std::uintptr_t>(buffer.data()) % alignof(Type) != 0)
			std::memcpy(&aligned, buffer.data() + ctr, sizeof(Type));
		else
			aligned = *reinterpret_cast<const Type*>(buffer.data() + ctr);
		ctr += sizeof(Type);
		return aligned;
	}
	template<typename Type>
	auto read(const unsigned char* buffer, std::size_t& ctr) noexcept
	{
		return read<Type>(std::span(buffer, std::dynamic_extent), ctr);
	}

	template<typename Type>
	auto sread(std::span<const unsigned char> buffer, std::size_t& ctr) noexcept
	{
		auto v = byte::byteswap_for_storage<Type>(buffer.data() + ctr);
		ctr += sizeof(Type);
		return v;
	}
	template<typename Type>
	auto sread(const unsigned char* buffer, std::size_t& ctr) noexcept
	{
		return sread<Type>(std::span(buffer, std::dynamic_extent), ctr);
	}

	template<typename Type>
	auto read(std::span<const unsigned char> buffer) noexcept
	{
		std::size_t ctr = 0;
		return read<Type>(buffer, ctr);
	}
	template<typename Type>
	auto read(const unsigned char* buffer) noexcept
	{
		std::size_t ctr = 0;
		return read<Type>(buffer, ctr);
	}

	template<typename Type>
	auto sread(std::span<const unsigned char> buffer) noexcept
	{
		std::size_t ctr = 0;
		return sread<Type>(buffer, ctr);
	}
	template<typename Type>
	auto sread(const unsigned char* buffer) noexcept
	{
		std::size_t ctr = 0;
		return sread<Type>(buffer, ctr);
	}

	template<typename Key, typename Value>
	std::optional<Value> search_partition(const Key& key, std::span<const unsigned char> data, std::size_t cells, bool closest = false) noexcept
	{
		std::size_t off = 0;
		std::size_t partition_left = 0;
		std::size_t partition_right = cells - 1;
		std::size_t optimal = ~0ull;
		bool has_optimal = false;

		do
		{
			const auto idx = (
				partition_left + (
					(partition_right - partition_left) / 2
				)
			);

			const auto v = byte::sread<Key>(
				data.subspan(off + idx *
					(sizeof(Key) + sizeof(Value))
				)
			);

			if (v > key)
			{
				partition_left = idx + 1;
			}
			else if (v < key)
			{
				partition_right = idx - 1;
				optimal = idx;
				has_optimal = true;
			}
			else
			{
				return byte::sread<Value>(
					data.subspan(idx *
						(sizeof(Key) + sizeof(Value)) +
						sizeof(Key)
					)
				);
			}
		} while (partition_left <= partition_right);

		if (closest)
		{
			if (!has_optimal)
				return std::nullopt;
			return byte::sread<Value>(
				data.subspan(optimal *
					(sizeof(Key) + sizeof(Value)) +
					sizeof(Key)
				)
			);
		}
		else
			return std::nullopt;
	}

	template<typename Value>
	std::optional<Value> search_partition_binary(std::span<const unsigned char> key, std::span<const unsigned char> data, std::size_t cells, auto&& comparator, bool closest = false) noexcept
	{
		std::size_t off = 0;
		std::size_t partition_left = 0;
		std::size_t partition_right = cells - 1;
		std::size_t optimal = ~0ull;
		bool has_optimal = false;

		do
		{
			const auto idx = (
				partition_left + (
					(partition_right - partition_left) / 2
				)
			);

			const auto v = data.subspan(off + idx *
				(key.size() + sizeof(Value))
			);

			const auto result = comparator(View::view(v), View::view(key));
			if (result > 0)
			{
				partition_left = idx + 1;
			}
			else if (result < 0)
			{
				partition_right = idx - 1;
				optimal = idx;
				has_optimal = true;
			}
			else
			{
				return byte::sread<Value>(
					data.subspan(idx *
						(key.size() + sizeof(Value)) +
						key.size()
					)
				);
			}
		} while (partition_left <= partition_right);

		if (closest)
		{
			if (!has_optimal)
				return std::nullopt;
			return byte::sread<Value>(
				data.subspan(optimal *
					(key.size() + sizeof(Value)) +
					key.size()
				)
			);
		}
		else
			return std::nullopt;
	}
}

#endif // RDB_LOCALE_HPP
