#ifndef RDB_SCHEMA_HPP
#define RDB_SCHEMA_HPP

#include <rdb_reflect.hpp>
#include <rdb_utils.hpp>
#include <rdb_keytype.hpp>
#include <rdb_locale.hpp>
#include <cstring>
#include <string_view>
#include <span>
#include <sstream>

namespace rdb
{
	namespace type
	{
		template<typename... Argv>
		struct Make
		{
			std::tuple<Argv...> data;
			Make(Argv&&... args)
				: data(std::forward_as_tuple(std::forward<Argv>(args)...)) {}
		};
	}

	// <Interfaces>
	// An interface is the most basic unit of data
	// Topologies of schemas are composed of fields of which each field is a unique named interface
	// There are two types of interfaces
	// Logical and Trivial
	// <Trivial Interfaces>
	// Trivial interfaces do not support <write procedures>
	// I.e. a write sequence requires a full read and then full write
	// <Logical Interfaces>
	// Reads and writes occur through <write procedures>
	// A write procedure is defined by an opcode (byte) and parameters passed after the opcode

	template<typename Type>
	concept InterfaceType = requires (Type& v)
	{
		// Returns the ammount of storage required by the type
		// E.g. a string might return it's length + the header
		{ v.storage() } -> std::same_as<std::size_t>;
		// Returns a hash of the data
		{ v.hash() } -> std::same_as<key_type>;
		// Returns a view to the entire object
		{ v.view() } -> std::same_as<View>;
		// Write and read procedures are used for performing any kind of operation on an object
		// Filtering procedures are used for comparison
		// They are meant to be trivially serializable so they can be easily transferred over the network and/or logged
		// The write procedure takes an opcode and parameters and returns a QueryResult which provides trivial serialization
		{ v.wproc(std::declval<proc_opcode>(), std::declval<proc_param>(), std::declval<wproc_query>()) }
			-> std::same_as<wproc_query_result>;
		{ v.rproc(std::declval<proc_opcode>(), std::declval<proc_param>()) }
			-> std::same_as<rproc_result>;
		{ v.fproc(std::declval<proc_opcode>(), std::declval<proc_param>()) }
			-> std::same_as<bool>;
		// These are meant to provide type safe access to interfaces from the user
		{ Type::uname } -> std::convertible_to<std::string_view>;
		{ Type::ucode } -> std::convertible_to<std::size_t>;
		{ Type::uproperty };
		// { Type::WritePair<Opcode>::param }
		// { Type::ReadPair<Opcode>::param Type::ReadPair<Opcode>::result }
	};

	enum class FieldType
	{
		Data,
		Sort
	};

	template<typename Base>
	class InterfaceHelper
	{
	protected:
		const void* _dynamic_field(std::size_t idx = sizeof(Base)) const noexcept
		{
			return reinterpret_cast<const unsigned char*>(this) + idx;
		}
		void* _dynamic_field(std::size_t idx = sizeof(Base)) noexcept
		{
			return reinterpret_cast<unsigned char*>(this) + idx;
		}
	};

	template<typename Base>
	struct InterfaceMake
	{
		template<typename... Argv>
		static auto make(Argv&&... args) noexcept
		{
			auto view = TypedView<Base>::copy(Base::mstorage(args...));
			Base::minline(view.mutate(), std::forward<Argv>(args)...);
			return view;
		}

		template<typename... Argv>
		static auto make(type::Make<Argv...>&& args) noexcept
		{
			return std::apply([](auto&&... args) {
				auto view = TypedView<Base>::copy(Base::mstorage(args...));
				Base::minline(view.mutate(), std::forward<Argv>(args)...);
				return view;
			}, args.data);
		}
	};

	struct InterfaceProperty
	{
		static constexpr std::uint64_t trivial = 1 << 0;
		static constexpr std::uint64_t dynamic = 1 << 1;
		static constexpr std::uint64_t fragmented = 1 << 2;
		std::uint64_t value{ trivial };

		constexpr InterfaceProperty() = default;
		constexpr InterfaceProperty(std::uint64_t value) : value(value) {}

		constexpr bool is(std::uint64_t property) const noexcept
		{
			return (value & property) == property;
		}
	};

	namespace impl
	{
		template<std::size_t Idx, typename... Args>
		struct FindNth
		{
			using type = void;
		};
		template<typename Arg, typename... Argv>
		struct FindNth<0, Arg, Argv...>
		{
			using type = Arg;
		};
		template<std::size_t Idx, typename Arg, typename... Argv>
		struct FindNth<Idx, Arg, Argv...>
		{
			using type = typename FindNth<Idx - 1, Argv...>::type;
		};
	}

	template<typename First, typename Second> struct ReadPair { using param = First; using result = Second; };
	template<typename... Argv> struct DeclWrite
	{
		static constexpr auto wcount = sizeof...(Argv);
		template<proc_opcode Opcode>
		using FindType = impl::FindNth<Opcode, Argv...>::type;
	};
	template<typename... Argv> struct DeclRead
	{
		static constexpr auto rcount = sizeof...(Argv);
		template<proc_opcode Opcode>
		using FindType = impl::FindNth<Opcode, Argv...>::type;
	};
	template<typename... Argv> struct DeclFilter
	{
		static constexpr auto fcount = sizeof...(Argv);
		template<proc_opcode Opcode>
		using FindType = impl::FindNth<Opcode, Argv...>::type;
	};

	namespace impl
	{
		struct EmptyProcBase
		{
			struct Op
			{
			protected:
				using write_type = DeclWrite<>;
				using read_type = DeclRead<>;
				using filter_type = DeclFilter<>;
			public:
				static constexpr auto wbase = 0;
				static constexpr auto rbase = 0;
				static constexpr auto fbase = 0;

				enum w : proc_opcode {};
				enum r : proc_opcode {};
				enum f : proc_opcode {};
			};
		};
	}

	template<typename Base, typename Write, typename Filter, typename Read>
	class InterfaceDeclProc : public Base::Op
	{
	protected:
		using write_type = Write;
		using read_type = Read;
		using filter_type = Filter;
	public:
		using BaseOp = Base::Op;
		using typename Base::Op::r;
		using typename Base::Op::w;
		using typename Base::Op::f;

		static constexpr auto wbase =
			BaseOp::wbase + BaseOp::write_type::wcount;
		static constexpr auto rbase =
			BaseOp::rbase + BaseOp::read_type::rcount;
		static constexpr auto fbase =
			BaseOp::fbase + BaseOp::filter_type::fcount;

		template<proc_opcode Opcode> using wtype =
			std::conditional_t<Opcode >= wbase, InterfaceDeclProc, BaseOp>
				::write_type::template FindType<Opcode - (wbase * (Opcode >= wbase))>;
		template<proc_opcode Opcode> using rtype =
			std::conditional_t<Opcode >= rbase, InterfaceDeclProc, BaseOp>
				::read_type::template FindType<Opcode - (rbase * (Opcode >= rbase))>;
		template<proc_opcode Opcode> using ftype =
			std::conditional_t<Opcode >= fbase, InterfaceDeclProc, BaseOp>
				::filter_type::template FindType<Opcode - (fbase * (Opcode >= fbase))>;
	};
	template<typename Write, typename Filter, typename Read>
	using InterfaceDeclProcPrimary = InterfaceDeclProc<impl::EmptyProcBase, Write, Filter, Read>;

	template<typename Base, cmp::ConstString UniqueName, InterfaceProperty Property = InterfaceProperty(), typename Accumulator = void, typename Compressor = void>
	struct Interface
	{
	public:
		static constexpr auto uproperty = Property;
		static constexpr auto cuname = UniqueName;
		static constexpr auto uname = *UniqueName;
		static inline auto ucode = ucode_type(std::hash<std::string_view>()(uname));
		static void require()
		{
			RuntimeInterfaceReflection::reg(ucode, RuntimeInterfaceReflection::RTII{
				[]() { return uproperty.is(uproperty.dynamic); },
				[]() { return alignof(Base); },
				[](const void* ptr) { return static_cast<const Base*>(ptr)->storage(); },
				[]() { return sizeof(Base); },
				[](const void* ptr) { return static_cast<const Base*>(ptr)->hash(); },
				[](void* ptr, proc_opcode o, const proc_param& p, wproc_query q) { return static_cast<Base*>(ptr)->wproc(o, p, q); },
				[](const void* ptr, proc_opcode o, const proc_param& p) { return static_cast<const Base*>(ptr)->rproc(o, p); },
				[](const void* ptr, proc_opcode o, const proc_param& p) { return static_cast<const Base*>(ptr)->fproc(o, p); },
				[]() { return uproperty.is(uproperty.fragmented); },
				[]() { return AccumulatorHandle::make<Accumulator>(); },
				[]() { return CompressorHandle::make<Compressor>(); }
			});
		}
	public:
		Interface() = default;
		Interface(const Interface&) = delete;
		Interface(Interface&&) = delete;

		View view() const noexcept
		{
			return View::view(std::span(
				reinterpret_cast<const unsigned char*>(this),
				static_cast<const Base*>(this)->storage()
			));
		}
		View view() noexcept
		{
			return View::view(std::span(
				reinterpret_cast<unsigned char*>(this),
				static_cast<Base*>(this)->storage()
			));
		}
	};

	template<cmp::ConstString Name, InterfaceType Inf, FieldType Type = FieldType::Data, Order Ordering = Order::Ascending>
	struct Field
	{
		static constexpr auto name = *Name;
		static constexpr auto cname = Name;
		static constexpr auto type = Type;
		static constexpr auto order = Ordering;
		using interface = Inf;
	};

	template<typename... Fields>
	class Topology
	{
	public:
		using value = AlignedTypedView<
			Topology,
			std::max({ alignof(Fields)... })
		>;
		static constexpr auto fields = sizeof...(Fields);
	private:
		template<typename Field, typename... Rest>
		struct AreStaticImpl
		{
			static constexpr auto value =
				!Field::interface::uproperty.is(InterfaceProperty::dynamic) &&
				AreStaticImpl<Rest...>::value;
		};
		template<typename Last>
		struct AreStaticImpl<Last>
		{
			static constexpr auto value = true;
		};
		struct AreStatic
		{
			static constexpr auto value = AreStaticImpl<Fields...>::value;
		};

		template<cmp::ConstString Name, typename Field, typename... Rest>
		struct SearchFieldImpl
		{
			static_assert(sizeof...(Rest), "Invalid field");
			using type = SearchFieldImpl<Name, Rest...>;
			using interface = type::interface;
			using field = type::field;
			static constexpr auto offset = sizeof(typename Field::interface) + type::offset;
			static constexpr auto idx = type::idx;
		};
		template<cmp::ConstString Name, typename Field, typename... Rest> requires (Name.view() == Field::name)
		struct SearchFieldImpl<Name, Field, Rest...>
		{
			using interface = Field::interface;
			using field = Field;
			static constexpr auto offset = 0;
			static constexpr auto idx = sizeof...(Fields) - sizeof...(Rest) - 1;
		};
		template<cmp::ConstString Name>
		struct SearchField
		{
			using type = SearchFieldImpl<Name, Fields...>;
			using interface = type::interface;
			using field = type::field;
			static constexpr auto offset = type::offset;
			static constexpr auto idx = type::idx;
		};
	public:
		template<cmp::ConstString Name>
		using interface = SearchField<Name>::interface;
		template<cmp::ConstString Name>
		using interface_view = TypedView<typename SearchField<Name>::interface>;
		template<cmp::ConstString Name>
		using field_type = SearchField<Name>::field;
	private:
		template<cmp::ConstString Key, cmp::ConstString... Rest>
		struct SortCountImpl
		{
			static constexpr auto count =
				SortCountImpl<Rest...>::count +
				(field_type<Key>::type == FieldType::Sort);
		};
		template<cmp::ConstString Key>
		struct SortCountImpl<Key>
		{
			static constexpr auto count =
				(field_type<Key>::type == FieldType::Sort);
		};

		template<FieldType Type, std::size_t Idx, typename Field, typename... Rest>
		struct SearchKeyImpl
		{
			static_assert(sizeof...(Rest), "Invalid key");
			using field = typename SearchKeyImpl<
				Type,
				Idx - (Field::type == Type ? 1 : 0),
				Rest...
			>::field;
		};
		template<FieldType Type, std::size_t Idx, typename Field, typename... Rest>
			requires (Idx == 0 && Field::type == Type)
		struct SearchKeyImpl<Type, Idx, Field, Rest...>
		{
			using field = Field;
		};
		template<std::size_t Idx>
		struct SearchSortKey
		{
			using field = SearchKeyImpl<FieldType::Sort, Idx, Fields...>::field;
		};
	public:
		static constexpr auto sort_count = SortCountImpl<Fields::cname...>::count;
		template<std::size_t Idx> static constexpr auto sort = SearchSortKey<Idx>::field::cname;
		template<std::size_t Idx> static constexpr auto sort_order = SearchSortKey<Idx>::field::order;
		template<cmp::ConstString Key> static constexpr auto has_sort_key =
			(field_type<Key>::type == FieldType::Sort);
		template<cmp::ConstString Field> static constexpr auto has =
			!std::is_same_v<void, typename SearchField<Field>::type>;
	private:
		template<typename Func>
		static constexpr void _for_each(Func&& callback, auto&&... args) noexcept
		{
			if constexpr (sizeof...(args))
			{
				([&]<typename Field>()
				{
					std::forward<Func>(callback).template operator()<Field>(
						std::forward<decltype(args)>(args)
					);
				}.template operator()<Fields>(), ...);
			}
			else
			{
				([&]<typename Field>()
				{
					std::forward<Func>(callback).template operator()<Field>();
				}.template operator()<Fields>(), ...);
			}
		}

		template<typename Interface>
		const auto* _at(std::size_t off) const noexcept
		{
			return reinterpret_cast<const Interface*>(
				reinterpret_cast<const unsigned char*>(this) + off
			);
		}
		template<typename Interface>
		auto* _at(std::size_t off) noexcept
		{
			return reinterpret_cast<Interface*>(
				reinterpret_cast<unsigned char*>(this) + off
			);
		}

		template<cmp::ConstString Name>
		auto _field_impl() const noexcept
		{
			using field = SearchField<Name>;
			if constexpr (AreStatic::value)
			{
				return TypedView<typename field::interface>::view(
					std::span(
						reinterpret_cast<const unsigned char*>(this) + field::offset,
						sizeof(typename field::interface)
					)
				);
			}
			else
			{
				std::size_t off = 0;
				std::size_t len = 0;
				([&]() {
					if constexpr (Fields::name == Name.view())
					{
						len = _at<typename Fields::interface>(off)->storage();
						return true;
					}
					else
					{
						off += _at<typename Fields::interface>(off)->storage();
						return false;
					}
				}() || ...);
				return TypedView<typename field::interface>::view(
					std::span(
						reinterpret_cast<const unsigned char*>(this) + off,
						len
					)
				);
			}
		}
		template<cmp::ConstString Name>
		auto _field_impl() noexcept
		{
			using field = SearchField<Name>;
			if constexpr (AreStatic::value)
			{
				return TypedView<typename field::interface>::view(
					std::span(
						reinterpret_cast<unsigned char*>(this) + field::offset,
						sizeof(typename field::interface)
					)
				);
			}
			else
			{
				std::size_t off = 0;
				std::size_t len = 0;
				([&]() {
					if constexpr (Fields::name == Name.view())
					{
						len = _at<typename Fields::interface>(off)->storage();
						return true;
					}
					else
					{
						off += _at<typename Fields::interface>(off)->storage();
						return false;
					}
				}() || ...);
				return TypedView<typename field::interface>::view(
					std::span(
						reinterpret_cast<unsigned char*>(this) + off,
						len
					)
				);
			}
		}

		template<std::size_t... Count>
		View _runtime_field_impl(std::size_t idx, std::index_sequence<Count...>) const noexcept
		{
			static auto table = std::array{
				+[](const Topology* ptr) -> View {
					auto field = ptr->_field_impl<Fields::cname>();
					return View::view(std::span{
						field.data().data(),
						field.size()
					});
				}...
			};
			return table[idx](this);
		}
		template<std::size_t... Count>
		View _runtime_field_impl(std::size_t idx, std::index_sequence<Count...> s) noexcept
		{
			View view = const_cast<const Topology*>(this)->_runtime_field_impl(idx, s);
			return View::view(std::span(
				const_cast<unsigned char*>(view.data().data()),
				view.size()
			));
		}

		template<std::size_t... Count>
		View _runtime_sort_field_impl(std::size_t idx, std::index_sequence<Count...>) const noexcept
		{
			if constexpr (sort_count)
			{
				static auto table = std::array{
					+[](const Topology* ptr) -> View {
						auto field = ptr->_field_impl<sort<Count>>();
						return View::view(std::span{
							field.data().data(),
							field.size()
						});
					}...
				};
				return table[idx](this);
			}
			else
			{
				std::terminate();
			}
		}
		template<std::size_t... Count>
		View _runtime_sort_field_impl(std::size_t idx, std::index_sequence<Count...> s) noexcept
		{
			View view = const_cast<const Topology*>(this)->_runtime_sort_field_impl(idx, s);
			return View::view(std::span(
				const_cast<unsigned char*>(view.data().data()),
				view.size()
			));
		}

		template<std::size_t... Count>
		static RuntimeInterfaceReflection::RTII& _runtime_reflect_impl(std::size_t idx, std::index_sequence<Count...>) noexcept
		{
			static auto table = std::array{ Fields::interface::ucode... };
			return RuntimeInterfaceReflection::info(table[idx]);
		}
	protected:
		static void require_fields() noexcept
		{
			(Fields::interface::require(), ...);
		};
	public:
		static bool sort_keys_equal(View lhs, View rhs) noexcept
		{
			const auto* ldata = lhs.data().data();
			const auto* rdata = rhs.data().data();
			std::size_t off1 = 0;
			std::size_t off2 = 0;

			return ([&]() -> bool {
				if constexpr (Fields::type == FieldType::Sort)
				{
					const auto* v1 = reinterpret_cast<const Fields::interface*>(ldata + off1);
					const auto* v2 = reinterpret_cast<const Fields::interface*>(rdata + off2);
					const auto v2p = View::view(std::span(rdata + off2, std::dynamic_extent));
					if (v1->fproc(proc_opcode(SortFilterOp::Equal), v2p))
						return true;
					off1 += v1->storage();
					off2 += v2->storage();
					return false;
				}
				return true;
			}() && ...);
		}
		static bool sort_keys_order(View lhs, View rhs) noexcept
		{
			const auto* ldata = lhs.data().data();
			const auto* rdata = rhs.data().data();
			std::size_t off1 = 0;
			std::size_t off2 = 0;

			return ([&]() -> bool {
				if constexpr (Fields::type == FieldType::Sort)
				{
					const auto ordering = Fields::order;
					const auto* v1 = reinterpret_cast<const Fields::interface*>(ldata + off1);
					const auto* v2 = reinterpret_cast<const Fields::interface*>(rdata + off2);
					const auto v2p = View::view(std::span(rdata + off2, std::dynamic_extent));
					if (v1->fproc(proc_opcode(SortFilterOp::Smaller), v2p))
						return ordering == Order::Ascending;
					if (v1->fproc(proc_opcode(SortFilterOp::Larger), v2p))
						return ordering != Order::Ascending;
					off1 += v1->storage();
					off2 += v2->storage();
					return false;
				}
				return false;
			}() || ...);
		}
		static int sort_keys_compare(View lhs, View rhs) noexcept
		{
			const auto* ldata = lhs.data().data();
			const auto* rdata = rhs.data().data();
			std::size_t off1 = 0;
			std::size_t off2 = 0;
			int result = 0;

			([&]() -> bool {
				if constexpr (Fields::type == FieldType::Sort)
				{
					const auto* v1 = reinterpret_cast<const Fields::interface*>(ldata + off1);
					const auto* v2 = reinterpret_cast<const Fields::interface*>(rdata + off2);
					const auto v2p = View::view(std::span(rdata + off2, std::dynamic_extent));
					if (v1->fproc(proc_opcode(SortFilterOp::Smaller), v2p))
					{
						result = -1;
						return true;
					}
					if (v1->fproc(proc_opcode(SortFilterOp::Larger), v2p))
					{
						result = 1;
						return true;
					}
					off1 += v1->storage();
					off2 += v2->storage();
				}
				return false;
			}() || ...);

			return result;
		}

		template<typename... Argv>
		static constexpr auto make(Argv&&... args) noexcept
		{
			auto data = value::copy(mstorage(args...));
			minline(data.mutate(), std::forward<Argv>(args)...);
			return data;
		}
		static constexpr version_type topology(std::size_t cutoff = ~0ull) noexcept
		{
			constexpr auto fnv_offset = 0x811C;
			constexpr auto fnv_prime = 0x0101;

			auto fnv = [](auto hash, auto code) {
				for (std::size_t i = 0; i < 4; i++)
				{
					unsigned char byte = (code >> (i * 8)) & 0xFF;
					hash ^= byte;
					hash *= fnv_prime;
				}
				return hash;
			};

			version_type hash{ fnv_offset };
			std::size_t idx = fnv_prime;
			_for_each([&]<typename Field>() {
				  hash = fnv(hash, Field::interface::ucode ^ fnv(hash, idx++));
				  return --cutoff;
			});

			return hash;
		}
		static constexpr std::size_t align() noexcept
		{
			return std::max({ alignof(Fields)... });
		}
		template<typename... Argv>
		static constexpr auto mstorage(Argv&&... args) noexcept
		{
			std::size_t size = 0;
			if constexpr (sizeof...(args))
			{
				([&]() {
					size += Fields::interface::mstorage(args);
				}(), ...);
			}
			else
			{
				([&]() {
					size += Fields::interface::mstorage();
				}(), ...);
			}
			return size;
		}
		template<typename... Argv>
		static auto minline(std::span<unsigned char> data, Argv&&... args) noexcept
		{
			std::size_t off = 0;
			if constexpr (sizeof...(args))
			{
				([&]() {
					off += Fields::interface::minline(
						data.subspan(off),
						std::forward<Argv>(args)
					);
				}(), ...);
			}
			else
			{
				([&]() {
					off += Fields::interface::minline(
						data.subspan(off)
					);
				}(), ...);
			}
			return off;
		}
		static constexpr auto mstorage_init_keys(View view) noexcept
		{
			std::size_t off = 0;
			std::size_t skey_off = 0;
			_for_each([&]<typename Field>() {
				if constexpr (Field::type == FieldType::Sort)
				{
					const auto size = TypedView<typename Field::interface>
						::view(view.data().subspan(skey_off))->storage();
					skey_off += size;
					off += size;
				}
				else
				{
					off += Field::interface::mstorage();
				}
			});
			return off;
		}
		static auto minline_init_keys(std::span<unsigned char> data, View view) noexcept
		{
			std::size_t off = 0;
			std::size_t skey_off = 0;
			_for_each([&]<typename Field>() {
				if constexpr (Field::type == FieldType::Sort)
				{
					const auto size = TypedView<typename Field::interface>
						::view(view.data().subspan(skey_off))->storage();
					std::memcpy(
						data.data() + off,
						view.data().data() + skey_off,
						size
					);
					skey_off += size;
					off += size;
				}
				else
				{
					off += Field::interface::minline(data.subspan(off));
				}
			});
			return off;
		}

		template<cmp::ConstString Name>
		static constexpr std::size_t index_of() noexcept
		{
			return SearchField<Name>::idx;
		}
		static RuntimeInterfaceReflection::RTII& reflect(std::size_t idx) noexcept
		{
			return _runtime_reflect_impl(
				idx, std::make_index_sequence<sizeof...(Fields)>()
			);
		}
		static std::string show() noexcept
		{
			std::stringstream out;

			out << "<";
			std::size_t off = 0;
			_for_each([&]<typename Field>() {
				out << "\n"
					<< "\t'"
					<< Field::name
					<< "' @"
					<< Field::interface::uname;
			});
			out << "\n>";

			return out.str();
		}

		Topology() = default;
		Topology(const Topology&) = default;
		Topology(Topology&&) = default;

		constexpr std::size_t storage() const noexcept
		{
			std::size_t size = 0;
			_for_each([&]<typename Field>() {
				size += _at<typename Field::interface>(size)->storage();
			});
			return size;
		}

		hash_type hash() const noexcept
		{
			return uuid::xxhash_combine({
				field<Fields::cname>()->hash()...
			});
		}

		std::string print() const noexcept
		{
			std::stringstream out;

			out << "[";
			std::size_t off = 0;
			_for_each([&]<typename Field>() {
				const auto* ptr = _at<interface<Field::cname>>(off);
				out << "\n"
					<< "\t'"
					<< Field::name
					<< "' @"
					<< Field::interface::uname
					<< ": "
					<< ptr->print();
				off += ptr->storage();
			});
			out << "\n]";

			return out.str();
		}

		std::size_t apply_field_write(std::size_t idx, View data, std::size_t buffer) noexcept
		{
			View f = field(idx);
			const auto size = storage();
			if (buffer != ~0ull)
			{
				const auto req =
					size +
					(int(data.data().size()) - f.data().size());
				if (buffer < req)
					return req;
			}

			const auto dest = f.mutate();
			if (idx != sizeof...(Fields) - 1 && data.size() != f.size())
			{
				const auto src = dest.data() + dest.size();
				const auto diff = int(data.data().size()) - dest.size();
				const auto back =
					size - (dest.data() - reinterpret_cast<unsigned char*>(this));
				std::memmove(src + diff, src, back);
			}
			std::memcpy(dest.data(), data.data().data(), data.data().size());

			return 0;
		}
		std::size_t apply_write(std::size_t idx, proc_opcode op, proc_param data, std::size_t buffer) noexcept
		{
			RuntimeInterfaceReflection::RTII& info = reflect(idx);
			View f = field(idx);
			if (buffer != ~0ull)
			{
				const auto type = info.wproc(
					f.mutate().data(),
					op,
					View::view(data),
					wproc_query::Type
				);
				if (type == wproc_type::Dynamic)
				{
					if (const auto req = info.wproc(
							f.mutate().data(),
							op,
							View::view(data),
							wproc_query::Storage
						); req > buffer)
					{
						return req;
					}
				}
			}
			info.wproc(
				f.mutate().data(),
				op,
				View::view(data),
				wproc_query::Commit
			);
			return 0;
		}

		template<cmp::ConstString Name>
		std::size_t apply_field_write(TypedView<interface<Name>> data, std::size_t buffer) noexcept
		{
			return apply_field_write(
				index_of<Name>(),
				View::view(data.data()),
				buffer
			);
		}
		template<cmp::ConstString Name, interface<Name>::Op::w Op>
		std::size_t apply_write(TypedView<typename interface<Name>::Op::template wtype<Op>::param> data, std::size_t buffer) noexcept
		{
			return apply_write(
				index_of<Name>(),
				proc_opcode(Op),
				View::view(data.data()),
				buffer
			);
		}
		template<cmp::ConstString Name, interface<Name>::Op::w Op, typename... Argv>
		std::size_t apply_write(std::size_t buffer, Argv&&... args) noexcept
		{
			const auto data = interface<Name>
				::Op::template wtype<Op>
				::param::make(std::forward<Argv>(args)...);
			return apply_write(
				index_of<Name>(),
				proc_opcode(Op),
				View::view(data.data()),
				buffer
			);
		}

		template<cmp::ConstString Name>
		auto field() const noexcept
		{
			return _field_impl<Name>();
		}
		template<cmp::ConstString Name>
		auto field() noexcept
		{
			return _field_impl<Name>();
		}

		View field(std::size_t idx) const noexcept
		{
			return _runtime_field_impl(
				idx, std::make_index_sequence<sizeof...(Fields)>()
			);
		}
		View field(std::size_t idx) noexcept
		{
			return _runtime_field_impl(
				idx, std::make_index_sequence<sizeof...(Fields)>()
			);
		}
		View sort_field(std::size_t idx) const noexcept
		{
			return _runtime_sort_field_impl(
				idx, std::make_index_sequence<sort_count>()
			);
		}
		View sort_field(std::size_t idx) noexcept
		{
			return _runtime_sort_field_impl(
				idx, std::make_index_sequence<sort_count>()
			);
		}
	};

	template<cmp::ConstString Name, typename Partition, typename Topology, typename... Parsers>
	class Schema : public Topology
	{
	private:
		template<std::size_t... Idx>
		static RuntimeInterfaceReflection::RTII& _reflect_pkey_impl(std::size_t idx, std::index_sequence<Idx...>) noexcept
		{
			static std::array table{
				+[]() -> RuntimeInterfaceReflection::RTII& {
					return
						RuntimeInterfaceReflection::info(
							Topology::template interface<
								Topology::template partition<Idx>
							>::ucode
						);
				}...
			};
			return table[idx]();
		}
		template<std::size_t... Idx>
		static RuntimeInterfaceReflection::RTII& _reflect_skey_impl(std::size_t idx, std::index_sequence<Idx...>) noexcept
		{
			if constexpr (Topology::sort_count)
			{
				static std::array table{
					+[]() -> RuntimeInterfaceReflection::RTII& {
						return
							RuntimeInterfaceReflection::info(
								Topology::template interface<
									Topology::template sort<Idx>
								>::ucode
							);
					}...
				};
				return table[idx]();
			}
			else
			{
				std::terminate();
			}
		}
		template<std::size_t... Idx>
		static Order _skey_order(std::size_t idx, std::index_sequence<Idx...>) noexcept
		{
			if constexpr (Topology::sort_count)
			{
				static std::array table{
					+[]() -> Order {
						return Topology::template sort_order<Idx>;
					}...
				};
				return table[idx]();
			}
			else
			{
				std::terminate();
			}
		}
	public:
		using topology = Topology;
		using partition = Partition;
		using data = Topology;
		static constexpr schema_type ucode = uuid::hash<schema_type>(*Name);

		static View transcode(version_type version, View data) noexcept
		{
			return View();
		}
		static RuntimeInterfaceReflection::RTII& reflect_skey(std::size_t idx) noexcept
		{
			return _reflect_skey_impl(
				idx, std::make_index_sequence<Topology::sort_count>()
			);
		}
		static Order skey_order(std::size_t idx) noexcept
		{
			return _skey_order(
				idx, std::make_index_sequence<Topology::sort_count>()
			);
		}

		static void require() noexcept
		{
			Topology::require_fields();
			RuntimeSchemaReflection::reg(ucode, RuntimeSchemaReflection::RTSI{
				[](void* ptr, const View& sort) { Topology::minline_init_keys(std::span(static_cast<unsigned char*>(ptr), std::dynamic_extent), sort); },
				[](const View& sort) { return Topology::mstorage_init_keys(sort); },
				[]() { return Topology::align(); },
				[](const void* ptr) { return static_cast<const Topology*>(ptr)->storage(); },

				[](void* ptr, std::size_t i, const View& v, std::size_t b) { return static_cast<Topology*>(ptr)->apply_field_write(i, View::view(v), b); },
				[](void* ptr, std::size_t i, proc_opcode o, const proc_param& p, std::size_t b) { return static_cast<Topology*>(ptr)->apply_write(i, o, p, b); },

				[](const void* ptr, std::size_t i) { return static_cast<const Topology*>(ptr)->field(i); },
				[](void* ptr, std::size_t i) { return static_cast<Topology*>(ptr)->field(i); },
				[](const void* ptr, std::size_t i) { return static_cast<const Topology*>(ptr)->sort_field(i); },
				[](version_type v, const View& d) { return transcode(v, View::view(d)); },

				[](const void* ptr) { return static_cast<const Partition*>(ptr)->hash(); },
				[](const void* ptr) { return static_cast<const Partition*>(ptr)->storage(); },

				[](std::size_t c) { return Topology::topology(c); },
				[]() -> std::size_t { return Topology::fields; },
				[]() -> std::size_t { return Topology::sort_count; },
				[](std::size_t c) { return skey_order(c); },

				[](std::size_t idx) -> RuntimeInterfaceReflection::RTII& { return Topology::reflect(idx); },
				[](std::size_t idx) -> RuntimeInterfaceReflection::RTII& { return Partition::reflect(idx); },
				[](std::size_t idx) -> RuntimeInterfaceReflection::RTII& { return reflect_skey(idx); },

				[](const void* ptr) -> std::string { return static_cast<const Topology*>(ptr)->print(); },
				[](const void* ptr) -> std::string { return static_cast<const Partition*>(ptr)->print(); },
				[]() -> std::string { return Topology::show(); },
				[]() -> std::string { return Partition::show(); },

				[](const View& a, const View& b) -> bool { return Topology::sort_keys_equal(a, b); },
				[](const View& a, const View& b) -> bool { return Topology::sort_keys_order(a, b); },
				[](const View& a, const View& b) -> int { return Topology::sort_keys_compare(a, b); },
			});
		}
	};

	template<typename... Schemas>
	constexpr void require() noexcept
	{
		(Schemas::require(), ...);
	}
}

#endif // RDB_SCHEMA_HPP
