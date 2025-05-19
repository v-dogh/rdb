#include <rdb_utils.hpp>
#include <rdb_locale.hpp>
#include <XXHash/xxhash.hpp>
#include <cmath>
#include <random>
#include <thread>
#include <atomic>
#include <chrono>
#ifdef __unix__
#include <ifaddrs.h>
#include <net/if.h>
#include <netpacket/packet.h>
#else
#error(Unsupported platform)
#endif

namespace rdb
{
	namespace uuid
	{
		std::uint64_t random_machine() noexcept
		{
			static const std::uint64_t machine = []() {
				std::random_device dev;
				std::mt19937 rng(
					xxhash_combine({
						dev(),
						std::hash<std::thread::id>()(std::this_thread::get_id()),
						std::hash<std::chrono::high_resolution_clock::rep>()(
							std::chrono::high_resolution_clock::now().time_since_epoch().count()
						)
					})
				);
				std::uniform_int_distribution<std::uint64_t> dist{};
				return dist(rng);
			}();
			return machine;
		}
		std::uint64_t stable_machine() noexcept
		{
			static const std::uint64_t machine = []() {
				ifaddrs* ifaddr = nullptr;
				if (getifaddrs(&ifaddr) == -1)
					return random_machine();

				std::uint64_t id = random_machine();
				for (const auto* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
				{
					if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_PACKET)
						continue;

					const sockaddr_ll* ptr = reinterpret_cast<const sockaddr_ll*>(ifa->ifa_addr);
					if (ptr->sll_halen == 8 && (ifa->ifa_flags & IFF_LOOPBACK) == 0)
					{
						std::memcpy(&id, ptr->sll_addr, 8);
						break;
					}
				}
				freeifaddrs(ifaddr);

				return id;
			}();
			return machine;
		}

		std::array<unsigned char, 16> ugen(std::uint64_t machine) noexcept
		{
			using time_ratio = std::chrono::duration<int64_t, std::ratio<1, 10'000'000>>;

			static std::atomic<std::uint16_t> idx = 0;
			static std::atomic<std::chrono::system_clock::time_point::rep> prev{};

			std::array<unsigned char, 16> result{};

			const auto time = std::chrono::duration_cast<time_ratio>(
				std::chrono::system_clock::now().time_since_epoch()
			).count();
			const auto ptime = byte::byteswap_for_storage(time);
			std::uint16_t clock = 0;
			if (prev == time)
			{
				clock = idx.fetch_add(1, std::memory_order::relaxed);
			}
			else
			{
				idx.store(1, std::memory_order::relaxed);
			}

			std::size_t off = 0;

			// 64-bits - 8 bytes - time since epoch

			result[off++] = static_cast<std::uint8_t>(ptime & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 8) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 16) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 24) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 32) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 40) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 48) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((ptime >> 56) & 0xFF);

			// 48-bits - 6 bytes - machine identifier (64-bits)

			result[off++] = static_cast<std::uint8_t>(machine & 0xFF);
			result[off++] = static_cast<std::uint8_t>((machine >> 8) & 0xFF) ^ static_cast<std::uint8_t>((machine >> 48) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((machine >> 16) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((machine >> 24) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((machine >> 32) & 0xFF);
			result[off++] = static_cast<std::uint8_t>((machine >> 40) & 0xFF) ^ static_cast<std::uint8_t>((machine >> 56) & 0xFF);

			// 16-bits - 2 bytes - clock sequence

			result[off++] = static_cast<std::uint8_t>(clock & 0xFF);
			result[off++] = static_cast<std::uint8_t>((clock >> 8) & 0xFF);

			return result;
		}

		std::size_t decode(const std::string& uuid, std::string_view table) noexcept
		{
			const std::size_t base = table.size();

			std::size_t id = 0;
			for (std::size_t i = uuid.size(); i-- > 0; )
			{
				auto c = uuid[i];
				auto pos = table.find(c);
				id = id * base + pos;
			}

			return id;
		}
		std::string encode(std::size_t id, std::string_view table) noexcept
		{
			const std::size_t base = table.size();

			std::string buffer{};
			[[ unlikely ]] if (id == 0) buffer.reserve(1);
			else buffer.reserve(std::size_t(std::log(id) / std::log(table.size())) + 1);

			auto id_cpy = id;
			do
			{
				buffer.push_back(table[id_cpy % base]);
				id_cpy /= base;
			}
			while (id_cpy > 0);

			return buffer;
		}

		key_type xxhash(std::span<const unsigned char> data, key_type seed) noexcept
		{
			return xxh::xxhash<64>(data.data(), data.size(), seed);
		}
		key_type xxhash(std::initializer_list<std::span<const unsigned char>> data, key_type seed) noexcept
		{
			xxh::hash_state_t<64> state{ seed };
			for (decltype(auto) it : data)
				state.update(it.begin(), it.end());
			return state.digest();
		}
		key_type xxhash_combine(key_type a, key_type b, key_type seed) noexcept
		{
			xxh::hash_state_t<64> state{ seed };
			state.update(&a, sizeof(a));
			state.update(&b, sizeof(b));
			return state.digest();
		}
		key_type xxhash_combine(std::initializer_list<key_type> li, key_type seed) noexcept
		{
			xxh::hash_state_t<64> state{ seed };
			for (decltype(auto) it : li)
				state.update(&it, sizeof(it));
			return state.digest();
		}
	}
}
