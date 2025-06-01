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
