#include <rdb_utils.hpp>
#include <rdb_locale.hpp>
#include <XXHash/xxhash.hpp>
#include <cmath>
#include <random>
#include <thread>
#include <atomic>
#include <chrono>
#include <sstream>
#include <iomanip>
#ifdef __unix__
#include <ifaddrs.h>
#include <net/if.h>
#include <netpacket/packet.h>
#include <sys/random.h>
#else
#error(Unsupported platform)
#endif

namespace rdb
{
	namespace uuid
	{
		std::string uint128_t::to_string() const noexcept
		{
			std::stringstream out;
			out << "0x"
				<< std::hex << std::setw(16) << std::setfill('0') << low
				<< std::hex << std::setw(16) << std::setfill('0') << high;
			return out.str();
		}

		std::span<const unsigned char> uint128_t::view() const noexcept
		{
			return std::span(
				reinterpret_cast<const unsigned char*>(&low),
				sizeof(std::uint64_t) * 2
			);
		}
		std::span<unsigned char> uint128_t::view() noexcept
		{
			return std::span(
				reinterpret_cast<unsigned char*>(&low),
				sizeof(std::uint64_t) * 2
			);
		}

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
#				ifdef __unix__
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
#				endif
			}();
			return machine;
		}

		uint128_t ugen_order_invert(uint128_t id) noexcept
		{
			return {
				.low = ~id.low,
				.high =
					(~id.high & 0xFFFF000000000000ull) |
					(id.high & 0x0000FFFFFFFFFFFFull)
			};
		}
		uint128_t ugen_time(std::uint64_t machine, bool ascending) noexcept
		{
			using time_ratio = std::chrono::duration<int64_t, std::ratio<1, 10'000'000>>;

			static std::atomic<std::int64_t> last{0};
			static std::atomic<std::uint32_t> sequence{0};

			uint128_t result;

			const auto time = std::chrono::duration_cast<time_ratio>(
				std::chrono::system_clock::now().time_since_epoch()
			).count();

			std::uint16_t clock_seq;

			auto prev = last.load(std::memory_order::relaxed);
			if (prev == time)
			{
				clock_seq = static_cast<std::uint16_t>(sequence.fetch_add(1, std::memory_order::relaxed));
				if (clock_seq == 0)
				{
					std::int64_t now;
					do
					{
						now = std::chrono::duration_cast<time_ratio>(
							std::chrono::system_clock::now().time_since_epoch()
						).count();
					} while (now == time);
					last.store(now, std::memory_order::relaxed);
				}
			}
			else
			{
				while (prev < time &&
					   !last.compare_exchange_weak(prev, time, std::memory_order::relaxed));
				sequence.store(1, std::memory_order::relaxed);
				clock_seq = 0;
			}

			// Byteswapped for big-endian (so it is trivially lexicographically comparable)

			const auto clock = byte::byteswap_for_sort(clock_seq);
			const auto ptime = byte::byteswap_for_sort(time);

			// 64-bits - 8 bytes - time since epoch

			result.low = ascending ? ptime : ~ptime;

			// 16-bits - 2 bytes - clock sequence
			// 48-bits - 6 bytes - machine identifier (64-bits) - truncated

			result.high =
				(static_cast<std::uint64_t>(ascending ? clock : ~clock) << 48) |
				(machine & 0x0000FFFFFFFFFFFFull);

			return result;
		}
		uint128_t ugen_random() noexcept
		{
			uint128_t result;
			getrandom(&result, sizeof(result), 0x00);
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
		key_type xxhash_combine(std::span<const key_type> li, key_type seed) noexcept
		{
			xxh::hash_state_t<64> state{ seed };
			for (decltype(auto) it : li)
				state.update(&it, sizeof(it));
			return state.digest();
		}
	}
	namespace util
	{
		void spinlock_yield() noexcept
		{
			// Probably an overkill
#			if defined(_MSC_VER)
				_mm_pause();
#			elif defined(__GNUG__)
				__builtin_ia32_pause();
#			else
				std::this_thread::yield();
#			endif
		}
	}
}
