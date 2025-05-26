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
		{ typename Type::rOp() } -> std::convertible_to<proc_opcode>;
		{ typename Type::wOp() } -> std::convertible_to<proc_opcode>;
		{ typename Type::fOp() } -> std::convertible_to<proc_opcode>;
		// { Type::WritePair<Opcode>::param }
		// { Type::ReadPair<Opcode>::param Type::ReadPair<Opcode>::result }
	};

	enum class FieldType
	{
		Data,
		Partition,
		Sort
	};

	template<typename Base>
	class InterfaceHelper
	{
	protected:
		const void* _dynamic_field() const noexcept
		{
			return static_cast<const Base*>(this + 1);
		}
		void* _dynamic_field() noexcept
		{
			return static_cast<Base*>(this + 1);
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
		static constexpr std::uint64_t dynamic = 1 << 0;
		static constexpr std::uint64_t fragmented = 1 << 0;
		std::uint64_t value{ trivial };

		constexpr InterfaceProperty() = default;
		constexpr InterfaceProperty(std::uint64_t value) : value(value) {}

		constexpr bool is(std::uint64_t property) const noexcept
		{
			return (value & property) == property;
		}
	};

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
				[](void* ptr, proc_opcode o, proc_param p, wproc_query q) { return static_cast<Base*>(ptr)->wproc(o, proc_param::view(p), q); },
				[](const void* ptr, proc_opcode o, proc_param p) { return static_cast<const Base*>(ptr)->rproc(o, proc_param::view(p)); },
				[](const void* ptr, proc_opcode o, proc_param p) { return static_cast<const Base*>(ptr)->fproc(o, proc_param::view(p)); },
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
		static constexpr auto fields = sizeof...(Fields);
	private:
		template<typename Field, typename... Rest>
		struct AreStaticImpl
		{
			enum
			{
				Value =
					!Field::interface::uproperty.is(InterfaceProperty::dynamic) &&
					AreStaticImpl<Rest...>::Value
			};
		};
		template<typename Last>
		struct AreStaticImpl<Last>
		{
			enum
			{
				Value = true
			};
		};
		struct AreStatic
		{
			enum
			{
				Value = AreStaticImpl<Fields...>::Value
			};
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
		using field_type = SearchField<Name>::field;
	private:
		template<cmp::ConstString Key, cmp::ConstString... Rest>
		struct PartitionCountImpl
		{
			static constexpr auto count =
				PartitionCountImpl<Rest...>::count +
				(field_type<Key>::type == FieldType::Partition);
		};
		template<cmp::ConstString Key>
		struct PartitionCountImpl<Key>
		{
			static constexpr auto count =
				(field_type<Key>::type == FieldType::Partition);
		};

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
		struct SearchPartitionKey
		{
			using field = SearchKeyImpl<FieldType::Partition, Idx, Fields...>::field;
		};
		template<std::size_t Idx>
		struct SearchSortKey
		{
			using field = SearchKeyImpl<FieldType::Sort, Idx, Fields...>::field;
		};
	public:
		static constexpr auto partition_count = PartitionCountImpl<Fields::cname...>::count;
		static constexpr auto sort_count = SortCountImpl<Fields::cname...>::count;
		template<std::size_t Idx> static constexpr auto partition = SearchPartitionKey<Idx>::field::cname;
		template<std::size_t Idx> static constexpr auto sort = SearchSortKey<Idx>::field::cname;
		template<std::size_t Idx> static constexpr auto sort_order = SearchSortKey<Idx>::field::order;
		template<cmp::ConstString Key> static constexpr auto has =
			(field_type<Key>::type == FieldType::Partition) ||
			(field_type<Key>::type == FieldType::Sort);
	private:
		template<typename Func>
		static constexpr void _for_each(Func&& callback, auto&&... args) noexcept
		{
			if constexpr (sizeof...(args))
			{
				([&]<typename Field>()
				{
					if constexpr (Field::type != FieldType::Partition)
					{
						std::forward<Func>(callback).template operator()<Field>(
							std::forward<decltype(args)>(args)
						);
					}
				}.template operator()<Fields>(), ...);
			}
			else
			{
				([&]<typename Field>()
				{
					if constexpr (Field::type != FieldType::Partition)
						std::forward<Func>(callback).template operator()<Field>();
				}.template operator()<Fields>(), ...);
			}
		}
		template<typename Func>
		static constexpr auto _for_each_table(Func&& callback) noexcept
		{
			return std::array{ [&]<typename Field>()
			{
				if constexpr (Field::type != FieldType::Partition)
					std::forward<Func>(callback).template operator()<Field>();
			}.template operator()<Fields>()... };
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
			if constexpr (AreStatic::Value)
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
				_for_each([&]<typename Field>() {
					if constexpr (Field::interface::uname != Name.view())
					{
						len = _at<typename Field::interface>(off)->storage();
						return true;
					}
					else
					{
						off += _at<Field>(off)->storage();
						return false;
					}
				});
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
			if constexpr (AreStatic::Value)
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
				_for_each([&]<typename Field>() {
					if constexpr (Field::uname != Name.view())
					{
						len = _at<Field>(off)->storage();
						return true;
					}
					else
					{
						off += _at<Field>(off)->storage();
						return false;
					}
				});
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
		template<typename... Argv>
		static constexpr auto make(Argv&&... args) noexcept
		{
			std::size_t size = 0;
			std::size_t off = 0;
			_for_each([&]<typename Field>() {
				size += Field::interface::mstorage();
			});
			auto data = AlignedTypedView<Topology, align()>::copy(size);
			if constexpr (sizeof...(Argv))
			{
				_for_each([&]<typename Field, typename Arg>(Arg&& arg) {
					off += Field::interface::minline(
						data.mutate().subspan(off),
						std::forward<Arg>(arg)
					);
				}, std::forward<Argv>(args)...);
			}
			else
			{
				_for_each([&]<typename Field>() {
					off += Field::interface::minline(
						data.mutate().subspan(off)
					);
				}, std::forward<Argv>(args)...);
			}
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
			std::size_t m = 0;
			_for_each([&]<typename Field>() {
				m = std::max(alignof(typename Field::interface), m);
			});
			return m;
		}
		static constexpr auto mstorage() noexcept
		{
			std::size_t size = 0;
			_for_each([&]<typename Field>() {
				size += Field::interface::mstorage();
			});
			return size;
		}
		static auto minline(std::span<unsigned char> data) noexcept
		{
			std::size_t off = 0;
			_for_each([&]<typename Field>() {
				off += Field::interface::minline(data.subspan(off));
			});
		}
		static constexpr auto mstorage_init_keys(View view) noexcept
		{
			std::size_t size = 0;
			std::size_t skey_off = 0;
			_for_each([&]<typename Field>() {
				if constexpr (Field::type == FieldType::Sort)
				{
					const auto size = TypedView<typename Field::interface>(
						view.data()
					)->storage();
					skey_off += size;
					size += size;
				}
				else
				{
					size += Field::interface::mstorage();
				}
			});
			return size;
		}
		static auto minline_init_keys(std::span<unsigned char> data, View view) noexcept
		{
			std::size_t off = 0;
			std::size_t skey_off = 0;
			_for_each([&]<typename Field>() {
				if constexpr (Field::type == FieldType::Sort)
				{
					const auto size = TypedView<typename Field::interface>(
						view.data()
					)->storage();
					std::memcpy(
						data.data() + off,
						view.data().data() + skey_off,
						size
					);
					skey_off += size;
					size += size;
				}
				else
				{
					off += Field::interface::minline(data.subspan(off));
				}
			});
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

		std::string print() const noexcept
		{
			std::stringstream out;

			out << "[";
			std::size_t off = 0;
			_for_each([&]<typename Field>() {
				const auto* ptr = _at<interface<Field::cname>>(off);
				out << "\n"
					<< "\t"
					<< Field::name
					<< "<"
					<< Field::interface::uname
					<< ">: "
					<< ptr->print();
				off += ptr->storage();
			});
			out << "\n]";

			return out.str();
		}

		std::size_t fw_apply(std::size_t idx, View data, std::size_t buffer) noexcept
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
		std::size_t wp_apply(std::size_t idx, proc_opcode op, proc_param data, std::size_t buffer) noexcept
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
		auto field() const noexcept
		{
			return _field_impl<Name>();
		}

		View field(std::size_t idx) const noexcept
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
	};

	template<cmp::ConstString Name, typename Topology, typename... Parsers>
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
		static key_type _hash_partition(View view) noexcept
		{
			std::array<key_type, Topology::partition_count> keys{};
			std::size_t idx = 0;
			std::size_t off = 0;
			while (idx < Topology::partition_count &&
				   off < view.size())
			{
				RuntimeInterfaceReflection::RTII& key
					= reflect_pkey(idx);
				const auto size = key.storage(view.data().data() + off);
				keys[idx] = key.hash(view.data().data() + off);
				off += size;
				idx++;
			}
			return uuid::xxhash_combine(
				std::span(keys.begin(), keys.end())
			);
		}
		static std::size_t _partition_size(View view) noexcept
		{
			std::size_t idx = 0;
			std::size_t off = 0;
			while (idx < Topology::partition_count &&
				   off < view.size())
			{
				RuntimeInterfaceReflection::RTII& key
					= reflect_pkey(idx);
				off += key.storage(view.data().data() + off);
				idx++;
			}
			return off;
		}
	public:
		using topology = Topology;
		static constexpr schema_type ucode = uuid::hash<schema_type>(*Name);

		static View transcode(version_type version, View data) noexcept
		{
			return View();
		}
		static RuntimeInterfaceReflection::RTII& reflect_pkey(std::size_t idx) noexcept
		{
			return _reflect_pkey_impl(
				idx, std::make_index_sequence<Topology::partition_count>()
			);
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
				[](void* ptr, View sort) { Topology::minline_init_keys(std::span(static_cast<unsigned char*>(ptr), std::dynamic_extent), sort); },
				[](View sort) { return Topology::mstorage_init_keys(sort); },
				[]() { return Topology::align(); },
				[](const void* ptr) { return static_cast<const Topology*>(ptr)->storage(); },

				[](void* ptr, std::size_t i, View v, std::size_t b) { return static_cast<Topology*>(ptr)->fw_apply(i, View::view(v), b); },
				[](void* ptr, std::size_t i, proc_opcode o, proc_param p, std::size_t b) { return static_cast<Topology*>(ptr)->wp_apply(i, o, proc_param::view(p), b); },

				[](const void* ptr, std::size_t i) { return static_cast<const Topology*>(ptr)->field(i); },
				[](void* ptr, std::size_t i) { return static_cast<Topology*>(ptr)->field(i); },
				[](const void* ptr, std::size_t i) { return static_cast<const Topology*>(ptr)->sort_field(i); },
				[](version_type v, View d) { return transcode(v, View::view(d)); },

				[](View v) { return _hash_partition(View::view(v)); },
				[](View v) { return _partition_size(View::view(v)); },

				[](std::size_t c) { return Topology::topology(c); },
				[]() -> std::size_t { return Topology::fields; },
				[]() -> std::size_t { return Topology::sort_count; },
				[](std::size_t c) { return skey_order(c); },

				[](std::size_t idx) -> RuntimeInterfaceReflection::RTII& { return Topology::reflect(idx); },
				[](std::size_t idx) -> RuntimeInterfaceReflection::RTII& { return reflect_pkey(idx); },
				[](std::size_t idx) -> RuntimeInterfaceReflection::RTII& { return reflect_skey(idx); },
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
