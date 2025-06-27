#ifndef RDB_TIME_HPP
#define RDB_TIME_HPP

#include <Types/rdb_scalar.hpp>
#include <rdb_utils.hpp>

namespace rdb::type
{
	using Time = Scalar<"t64", std::chrono::system_clock::duration::rep>;
	class Timestamp :
		public ScalarBase<Timestamp, std::chrono::system_clock::time_point::rep>,
		public Interface<Timestamp, "tp64">
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
			ScalarBase::minline(view,
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
			case Round::Year: value = round<years>(value) + years(to);
			case Round::Month: value = round<months>(value) + months(to);
			case Round::Day: value = round<days>(value) + days(to);
			case Round::Hour: value = round<hours>(value) + hours(to);
			case Round::Minute: value = round<minutes>(value) + minutes(to);
			}
			return value.time_since_epoch().count();
		}
	};
	class TimeUUID :
		public ScalarBase<TimeUUID, uuid::uint128_t>,
		public Interface<TimeUUID, "tuuid">
	{
	public:
		static auto minline(std::span<unsigned char> view, const uuid::uint128_t& value) noexcept
		{
			return ScalarBase::minline(view, value);
		}
		static auto minline(std::span<unsigned char> view) noexcept
		{
			return ScalarBase::minline(view,
				uuid::ugen(uuid::stable_machine())
			);
		}
		static auto id() noexcept
		{
			return uuid::ugen(uuid::stable_machine());
		}
	};
	class RandUUID :
		public ScalarBase<RandUUID, uuid::uint128_t>,
		public Interface<RandUUID, "ruuid">
	{
	public:
		static auto minline(std::span<unsigned char> view, const uuid::uint128_t& value) noexcept
		{
			return ScalarBase::minline(view, value);
		}
		static auto minline(std::span<unsigned char> view) noexcept
		{
			return ScalarBase::minline(view,
				uuid::ugen()
			);
		}
		static auto id() noexcept
		{
			return uuid::ugen();
		}
	};
}

#endif // RDB_TIME_HPP
