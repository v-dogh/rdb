#ifndef RDB_TIME_HPP
#define RDB_TIME_HPP

#include <Types/rdb_scalar.hpp>
#include <iostream>
#include <rdb_utils.hpp>

namespace rdb::type
{
	using Time = Scalar<"t64", std::chrono::system_clock::duration::rep>;
	class Timestamp :
		public ScalarBase<Timestamp, std::chrono::system_clock::time_point::rep>,
		public Interface<
			Timestamp, "tp64",
			InterfaceProperty::sortable | InterfaceProperty::trivial | InterfaceProperty::static_prefix
		>
	{
	public:
		enum class Round
		{
			Year,
			Month,
			Day,
			Hour,
			Minute
		};
	public:
		static auto minline(std::span<unsigned char> view, const std::chrono::system_clock::time_point::rep& value) noexcept
		{
			return ScalarBase::minline(view, value);
		}
		static auto minline(std::span<unsigned char> view) noexcept
		{
			return ScalarBase::minline(view,
				std::chrono::system_clock::now().time_since_epoch().count()
			);
		}
		static auto now() noexcept
		{
			return std::chrono::system_clock::now().time_since_epoch().count();
		}
		static auto now(Round rval, std::size_t to = 0) noexcept
		{
			using namespace std::chrono;
			auto value = std::chrono::system_clock::now();
			switch (rval)
			{
			case Round::Year: value = round<years>(value) + years(to); break;
			case Round::Month: value = round<months>(value) + months(to); break;
			case Round::Day: value = round<days>(value) + days(to); break;
			case Round::Hour: value = round<hours>(value) + hours(to); break;
			case Round::Minute: value = round<minutes>(value) + minutes(to); break;
			}
			return value.time_since_epoch().count();
		}
	};
	class TimeUUID :
		public ScalarBase<TimeUUID, uuid::uint128_t>,
		public Interface<
			TimeUUID, "tuuid",
			InterfaceProperty::sortable | InterfaceProperty::trivial | InterfaceProperty::static_prefix
		>
	{
	public:
		static auto mpinline(std::span<unsigned char> view, rdb::Order order, const uuid::uint128_t& value = {}) noexcept
		{
			return TimeUUID(value).prefix(
				View::view(view.subspan(0, sizeof(uuid::uint128_t))),
				order
			);
		}

		static auto id() noexcept
		{
			return uuid::ugen_time(uuid::stable_machine());
		}

		std::size_t prefix(View buffer, rdb::Order order) const noexcept
		{
			const auto val =
				order == rdb::Order::Ascending ?
					value() :
					uuid::ugen_order_invert(value());
			const auto len = std::min(sizeof(val), buffer.size());
			std::memcpy(
				buffer.mutate().data(),
				&val,
				len
			);
			return len;
		}
	};
	class RandUUID :
		public ScalarBase<RandUUID, uuid::uint128_t>,
		public Interface<
			RandUUID, "ruuid",
			InterfaceProperty::sortable | InterfaceProperty::trivial | InterfaceProperty::static_prefix
		>
	{
	public:
		static auto minline(std::span<unsigned char> view, const uuid::uint128_t& value) noexcept
		{
			return ScalarBase::minline(view, value);
		}
		static auto minline(std::span<unsigned char> view) noexcept
		{
			return ScalarBase::minline(view,
				uuid::ugen_random()
			);
		}
		static auto id() noexcept
		{
			return uuid::ugen_random();
		}
	};
}

#endif // RDB_TIME_HPP
