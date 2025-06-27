#ifndef RDB_SCALAR_HPP
#define RDB_SCALAR_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>

namespace rdb::type
{
	template<typename Base, typename Type> requires std::is_trivial_v<Type>
	class ScalarBase : public InterfaceMake<Base>
	{
	public:
		using value_type = Type;
	private:
		Type _value{ 0 };
	public:
		static auto mstorage(const Type& value = Type{}) noexcept
		{
			return sizeof(ScalarBase);
		}
		static auto minline(std::span<unsigned char> view, const Type& value = {}) noexcept
		{
			new (view.data()) ScalarBase(value);
			return sizeof(ScalarBase);
		}

		ScalarBase() = default;
		explicit ScalarBase(const Type& value)
			: _value(byte::byteswap_for_storage(value)) {}

		struct Op : InterfaceDeclProcPrimary<
			DeclWrite<ScalarBase, ScalarBase, ScalarBase>,
			DeclFilter<ScalarBase, ScalarBase, ScalarBase>,
			DeclRead<>
		>
		{
			enum w : proc_opcode
			{
				Add,
				Mul,
				Div
			};
			enum r : proc_opcode {};
			enum f : proc_opcode
			{
				Smaller = proc_opcode(SortFilterOp::Smaller),
				Larger = proc_opcode(SortFilterOp::Larger),
				Equal = proc_opcode(SortFilterOp::Equal),
			};
		};

		const Type* underlying() const noexcept
		{
			return &_value;
		}
		Type* underlying() noexcept
		{
			return &_value;
		}
		Type value() const noexcept
		{
			return byte::byteswap_for_storage(_value);
		}

		key_type hash() const noexcept
		{
			return uuid::xxhash(std::span(
				reinterpret_cast<const unsigned char*>(&_value),
				sizeof(_value)
			));
		}

		static constexpr std::size_t static_storage() noexcept
		{
			return sizeof(Type);
		}
		constexpr std::size_t storage() const noexcept
		{
			return sizeof(Type);
		}
		std::string print() const noexcept
		{
			return util::to_string(value());
		}

		wproc_query_result wproc(proc_opcode opcode, const proc_param& arguments, wproc_query query) noexcept
		{
			if (query == wproc_query::Commit)
			{
				if constexpr (std::is_arithmetic_v<Type>)
				{
					const auto& arg = byte::byteswap_for_storage(
						TypedView<ScalarBase>::view(arguments.data())->_value
					);
					Type result;
					if (opcode == Op::Add) result = value() + arg;
					else if (opcode == Op::Mul) result = value() * arg;
					else if (opcode == Op::Div) result = value() / arg;
					_value = byte::byteswap_for_storage(result);
					return wproc_status::Ok;
				}
				else
				{
					return wproc_status::Error;
				}
			}
			return wproc_type::Static;
		}
		rproc_result rproc(proc_opcode, const proc_param&) const noexcept { return View(); }
		bool fproc(proc_opcode opcode, const proc_param& arguments) const noexcept
		{
			const auto arg = byte::byteswap_for_storage(
				TypedView<ScalarBase>::view(arguments.data())->_value
			);
			if (opcode == Op::Larger) return value() > arg;
			else if (opcode == Op::Smaller) return value() < arg;
			else if (opcode == Op::Equal) return value() == arg;
			return false;
		}
	};

	template<cmp::ConstString UniqueName, typename Type> requires std::is_trivial_v<Type>
	class Scalar :
		public ScalarBase<Scalar<UniqueName, Type>, Type>,
		public Interface<Scalar<UniqueName, Type>, UniqueName>
	{ };

	using Uint8 = Scalar<"u8", std::uint8_t>;
	using Uint16 = Scalar<"u16", std::uint16_t>;
	using Uint32 = Scalar<"u32", std::uint32_t>;
	using Uint64 = Scalar<"u64", std::uint64_t>;

	using Int8 = Scalar<"i8", std::uint8_t>;
	using Int16 = Scalar<"i16", std::uint16_t>;
	using Int32 = Scalar<"i32", std::uint32_t>;
	using Int64 = Scalar<"i64", std::uint64_t>;

	using Byte = Uint8;
	using Hash = Uint64;
	using Boolean = Scalar<"bool", bool>;
	using Character = Scalar<"char", char>;
	using U8Character = Scalar<"char8", char8_t>;
	using U16Character = Scalar<"char16", char16_t>;
	using U32Character = Scalar<"char32", char32_t>;
}

#endif // RDB_SCALAR_HPP
