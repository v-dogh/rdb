#ifndef RDB_TUPLE_HPP
#define RDB_TUPLE_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>
#include <sstream>

namespace rdb::type
{
	template<typename... Ts>
	class alignas(std::max({ alignof(Ts)... })) Tuple :
		public InterfaceMake<Tuple<Ts...>>,
		public Interface<
			Tuple<Ts...>,
			cmp::concat_const_string<"t<", Ts::cuname..., ">">(),
			((Ts::uproperty.is(Ts::uproperty.dynamic) || ...) ? InterfaceProperty::dynamic : 0x00) |
			((Ts::uproperty.is(Ts::uproperty.trivial) && ...) ? InterfaceProperty::trivial : 0x00)
		>
	{
	private:
		template<typename Type>
		const auto* _at(std::size_t off) const noexcept
		{
			return reinterpret_cast<const Type*>(
				reinterpret_cast<const unsigned char*>(this) + off
			);
		}
		template<typename Type>
		auto* _at(std::size_t off) noexcept
		{
			return reinterpret_cast<Type*>(
				reinterpret_cast<unsigned char*>(this) + off
			);
		}

		template<typename Type>
		std::span<const unsigned char> _at_view(std::size_t off) const noexcept
		{
			const auto* begin = reinterpret_cast<const unsigned char*>(this) + off;
			const auto* ptr = reinterpret_cast<const Type*>(begin);
			return std::span(begin, begin + ptr->storage());
		}
		template<typename Type>
		std::span<unsigned char> _at_view(std::size_t off) noexcept
		{
			auto* begin = reinterpret_cast<unsigned char*>(this) + off;
			auto* ptr = reinterpret_cast<Type*>(begin);
			return std::span(begin, begin + ptr->storage());
		}

		template<std::size_t Idx, typename Type, typename... Rest>
		struct TypeAtImpl
		{
			using type = TypeAtImpl<Idx - 1, Rest...>::type;
		};
		template<typename Type, typename... Rest>
		struct TypeAtImpl<0, Type, Rest...>
		{
			using type = Type;
		};

		template<std::size_t Idx>
		struct TypeAt
		{
			using type = TypeAtImpl<Idx, Ts...>::type;
		};

		template<std::size_t Idx, std::size_t Off, typename Type, typename... Rest>
		struct StaticOffsetImpl
		{
			static constexpr auto offset = StaticOffsetImpl<
				Idx - 1, Off + Type::static_storage(), Rest...
			>::offset;
		};
		template<std::size_t Off, typename Type, typename... Rest>
		struct StaticOffsetImpl<0, Off, Type, Rest...>
		{
			static constexpr auto offset = Off;
		};

		template<std::size_t Idx>
		struct StaticOffset
		{
			static constexpr auto offset = StaticOffsetImpl<Idx, 0, Ts...>::offset;
		};

		template<std::size_t... Idv>
		View _field_impl(std::size_t idx, std::index_sequence<Idv...>) const noexcept
		{
			static std::array table{
				+[](const Tuple* tuple) {
					return tuple->_at_view<typename TypeAt<Idv>::type>(
						tuple->_offset_of<Idv>()
					);
				}...
			};
			return View::view(table[idx](this));
		}
		template<std::size_t... Idv>
		View _field_impl(std::size_t idx, std::index_sequence<Idv...>) noexcept
		{
			static std::array table{
				+[](Tuple* tuple) {
					return tuple->_at_view<typename TypeAt<Idv>::type>(
						tuple->_offset_of<Idv>()
					);
				}...
			};
			return View::view(table[idx](this));
		}

		template<std::size_t Idx>
		constexpr std::size_t _offset_of() const noexcept
		{
			if constexpr (Tuple::uproperty.is(Tuple::uproperty.dynamic))
			{
				std::size_t off = 0;
				[&]<std::size_t... Idv>(std::index_sequence<Idv...>) {
					((off += _at<typename TypeAt<Idv>::type>(off)->storage()), ...);
				}(std::make_index_sequence<Idx>());
				return off;
			}
			else
			{
				return StaticOffset<Idx>::offset;
			}
		}
	public:
		template<typename... Argv>
		static auto mstorage(const Argv&... args) noexcept
		{
			static_assert(sizeof...(Argv) == sizeof...(Ts), "Tuple must be either default-initialized or have all elements initialized");
			return (Ts::mstorage(args) + ...);
		}
		template<typename... Argv>
		static auto mstorage(const Make<Argv...>& args) noexcept
		{
			return std::apply([](const auto&... args) {
				return (Ts::mstorage(args) + ...);
			}, args.data);
		}
		static auto mstorage() noexcept
		{
			return (Ts::mstorage() + ...);
		}

		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Argv&&... args) noexcept
		{
			new (view.data()) Tuple{ std::forward<Argv>(args)... };
			return mstorage(args...);
		}
		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Make<Argv...> args) noexcept
		{
			return std::apply([&](auto&&... args) {
				return minline(view, std::forward<Argv>(args)...);
			}, args.data);
		}

		Tuple()
		{
			std::size_t off = 0;
			([&]() {
				off += Ts::minline(_at_view<Ts>(off));
			}(), ...);
		}
		template<typename... Argv>
		Tuple(Argv&&... args)
		{
			std::size_t off = 0;
			([&]() {
				off += Ts::minline(_at_view<Ts>(off), args);
			}(), ...);
		}

		enum rOp : proc_opcode
		{
			// Get + N is equivalent to a read of tuple.field<N>()
			Get = 0
		};
		enum wOp : proc_opcode
		{
			// Set + N is equivalent to a write to tuple.field<N>()
			Set = 0
		};
		enum fOp : proc_opcode
		{
			Smaller = proc_opcode(SortFilterOp::Smaller),
			Larger = proc_opcode(SortFilterOp::Larger),
			Equal = proc_opcode(SortFilterOp::Equal),
		};

		template<rOp Op>
		struct ReadPair
		{
			using param = void;
			using result = TypeAt<std::size_t(Op)>;
		};
		template<wOp Op>
		struct WritePair
		{
			using param = TypeAt<std::size_t(Op)>;
		};
		template<fOp Op>
		struct FilterPair
		{
			using param = Tuple;
		};

		key_type hash() const noexcept
		{
			std::size_t off = 0;
			return uuid::xxhash_combine({
				[&]() -> hash_type {
					const auto h = _at<Ts>(off)->hash();
					off += _at<Ts>(off)->storage();
					return h;
				}()...
			});
		}

		template<std::size_t Idx>
		auto field() const noexcept
		{
			static_assert(Idx < sizeof...(Ts), "Invalid index");
			using type = TypeAt<Idx>::type;
			return TypedView<type>::view(
				_at_view<type>(_offset_of<Idx>())
			);
		}
		template<std::size_t Idx>
		auto field() noexcept
		{
			static_assert(Idx < sizeof...(Ts), "Invalid index");
			using type = TypeAt<Idx>::type;
			return TypedView<type>::view(
				_at_view<type>(_offset_of<Idx>())
			);
		}

		View field(std::size_t idx) const noexcept
		{
			return _field_impl(idx, std::make_index_sequence<sizeof...(Ts)>());
		}
		View field(std::size_t idx) noexcept
		{
			return _field_impl(idx, std::make_index_sequence<sizeof...(Ts)>());
		}

		static constexpr std::size_t static_storage() noexcept
		{
			if constexpr (!(Ts::uproperty.is(Ts::uproperty.dynamic) || ...))
				return (Ts::static_storage() + ...);
			else
				static_assert(false, "Type is dynamic");
		}
		constexpr std::size_t storage() const noexcept
		{
			if constexpr (Tuple::udynamic)
			{
				std::size_t off = 0;
				return ((off += _at<Ts>(off)->storage()), ...);
			}
			else
			{
				return (Ts::static_storage() + ...);
			}
		}
		std::string print() const noexcept
		{
			std::stringstream out;

			out << "[ ";

			std::size_t off = 0;
			std::size_t idx = 0;
			([&]() {
				if (off != 0)
					out << ", ";
				out << "<";
				out << idx++;
				out << ">: '";
				out << _at<Ts>(off)->print();
				out << '\'';
				off += _at<Ts>(off)->storage();
			}(), ...);

			out << " ]";

			return out.str();
		}

		wproc_query_result wproc(proc_opcode opcode, proc_param arguments, wproc_query query) noexcept
		{

		}
		rproc_result rproc(proc_opcode opcode, proc_param) const noexcept
		{
			if (opcode > rOp::Get)
			{
				return field(opcode - rOp::Get);
			}
		}
		bool fproc(proc_opcode opcode, proc_param arguments) const noexcept
		{

		}
	};
}

#endif // RDB_TUPLE_HPP
