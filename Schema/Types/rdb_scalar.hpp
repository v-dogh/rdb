#ifndef RDB_SCALAR_HPP
#define RDB_SCALAR_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>

namespace rdb::type
{
	template<cmp::ConstString UniqueName, typename Type> requires std::is_trivial_v<Type>
	class Scalar : public Interface<Scalar<UniqueName, Type>, UniqueName>
	{
	private:
		Type _value{ 0 };
	public:
		static auto mstorage(const Type& value = Type{}) noexcept
		{
			return sizeof(Scalar);
		}
		static auto minline(std::span<unsigned char> view, Type value = {}) noexcept
		{
			new (view.data()) Scalar(value);
			return sizeof(Scalar);
		}
		static auto make(Type value) noexcept
		{
			auto view = TypedView<Scalar>::copy(sizeof(Scalar));
			new (view.mutate().data()) Scalar(value);
			return view;
		}

		Scalar() = default;
		explicit Scalar(Type value) : _value(value) {}

		enum rOp : proc_opcode { };
		enum wOp : proc_opcode
		{
			Add = 'a',
			Mul = 'm',
			Div = 'd'
		};
		enum fOp : proc_opcode
		{
			Smaller = proc_opcode(SortFilterOp::Smaller),
			Larger = proc_opcode(SortFilterOp::Larger),
			Equal = proc_opcode(SortFilterOp::Equal),
		};

		template<wOp Op>
		struct WritePair
		{
			using param = Scalar;
		};
		template<fOp Op>
		struct FilterPair
		{
			using param = Scalar;
		};

		Type& value() noexcept
		{
			return _value;
		}

		void view(View view) const noexcept
		{
			const auto* beg = reinterpret_cast<const unsigned char*>(this);
			const auto* end = beg + storage();
			const auto tv = TypedView<Scalar>::view(view.mutate());
			if constexpr (byte::is_storage_endian())
			{
				tv->_value = _value;
			}
			else
			{
				tv = byte::byteswap(_value);
			}
		}
		key_type hash() const noexcept
		{
			const auto v = byte::byteswap_for_storage(_value);
			return uuid::xxhash(std::span(
				reinterpret_cast<const unsigned char*>(&v),
				sizeof(v)
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
			return std::to_string(_value);
		}

		wproc_query_result wproc(proc_opcode opcode, proc_param arguments, wproc_query query) noexcept
		{
			const auto& arg = TypedView<Scalar>::view(arguments.data())->_value;
			if (opcode == wOp::Add) _value += arg;
			else if (opcode == wOp::Mul) _value *= arg;
			else if (opcode == wOp::Div) _value /= arg;
			return wproc_type::Static;
		}
		rproc_result rproc(proc_opcode, proc_param) const noexcept { return View(); }
		bool fproc(proc_opcode opcode, proc_param arguments) const noexcept
		{
			const auto arg = TypedView<Scalar>::view(arguments.data())->_value;
			const auto val = _value;
			if (opcode == fOp::Larger) return _value > arg;
			else if (opcode == fOp::Smaller) return _value < arg;
			else if (opcode == fOp::Equal) return _value == arg;
			return false;
		}
	};

	using Uint8 = Scalar<"u8", std::uint8_t>;
	using Uint16 = Scalar<"u16", std::uint16_t>;
	using Uint32 = Scalar<"u32", std::uint32_t>;
	using Uint64 = Scalar<"u64", std::uint64_t>;

	using Int8 = Scalar<"i8", std::uint8_t>;
	using Int16 = Scalar<"i16", std::uint16_t>;
	using Int32 = Scalar<"i32", std::uint32_t>;
	using Int64 = Scalar<"i64", std::uint64_t>;

	using Byte = Uint8;
}

#endif // RDB_SCALAR_HPP
