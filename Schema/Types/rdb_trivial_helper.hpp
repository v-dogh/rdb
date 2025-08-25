#ifndef RDB_TRIVIAL_HELPER_HPP
#define RDB_TRIVIAL_HELPER_HPP

namespace rdb::type::impl
{
	template<bool, typename> struct TrivialTypeImpl;
	template<typename Type> struct TrivialTypeImpl<true, Type>
	{
		using value_type = typename Type::value_type;
		static constexpr auto storage = Type::static_storage();
	};
	template<typename Type> struct TrivialTypeImpl<false, Type>
	{
		using value_type = void*;
		static constexpr auto storage = 1ull;
	};

	template<typename Type>
	using TrivialInterface = TrivialTypeImpl<
		Type::uproperty.is(Type::uproperty.trivial),
		Type
	>;
}

#endif // RDB_TRIVIAL_HELPER_HPP
