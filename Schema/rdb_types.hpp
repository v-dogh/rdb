#ifndef RDB_TYPES_HPP
#define RDB_TYPES_HPP

#include <rdb_locale.hpp>
#include <rdb_schema.hpp>

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
		static auto minline(std::span<unsigned char> view) noexcept
		{
			new (view.data()) Scalar{};
			return sizeof(Scalar);
		}
		static auto minline(Type value, std::span<unsigned char> view) noexcept
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

		View view() const noexcept
		{
			const auto beg = reinterpret_cast<const unsigned char*>(this);
			const auto end = beg + storage();
			if constexpr (byte::is_storage_endian())
			{
				return View::view(std::span(beg, end));
			}
			else
			{
				return make(byte::byteswap(_value));
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
			return wproc_type::Default;
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

	template<typename... Ts>
	class Tuple : public Interface<
		Tuple<Ts...>,
		cmp::concat_const_string<"t", Ts::uname...>(),
		(Ts::udynamic || ...)
	>
	{
	private:
		alignas(std::max({ alignof(Ts)... }))
			std::array<unsigned char, (Tuple::udynamic ? 0 : (sizeof(Ts) + ...))> buffer;
	public:
		View view() const noexcept
		{

		}
		key_type hash() const noexcept
		{

		}

		constexpr std::size_t storage() const noexcept
		{
			if constexpr (Tuple::udynamic)
			{

			}
			else
			{
				return (sizeof(Ts) + ...);
			}
		}
		std::string print() const noexcept
		{
			return "";
		}

		wproc_query_result wproc(proc_opcode opcode, proc_param arguments, wproc_query query) noexcept
		{

		}
		rproc_result rproc(proc_opcode, proc_param) const noexcept
		{

		}
		bool fproc(proc_opcode opcode, proc_param arguments) const noexcept
		{

		}
	};

	// template<cmp::ConstString UniqueName, typename Type> requires std::is_trivial_v<Type>
	// class Buffer : public Interface<Buffer<UniqueName, Type>, UniqueName, true>
	// {
	// private:
	// 	std::uint32_t _length{ 0 };

	// 	void _insert(std::size_t offset, std::span<const Type> data) noexcept
	// 	{
	// 		Type* buffer = this->_dynamic_field();
	// 		std::copy(
	// 			data.begin(), data.end(),
	// 			buffer + offset
	// 		);
	// 	}
	// public:
	// 	static auto mstorage(const Type& value = Type{}) noexcept
	// 	{
	// 		return sizeof(Scalar);
	// 	}
	// 	static auto minline(std::span<unsigned char> view) noexcept
	// 	{
	// 		new (view.data()) Scalar{};
	// 		return sizeof(Scalar);
	// 	}
	// 	static auto minline(Type value, std::span<unsigned char> view) noexcept
	// 	{
	// 		new (view.data()) Scalar(value);
	// 		return sizeof(Scalar);
	// 	}
	// 	static auto make(Type value) noexcept
	// 	{
	// 		auto view = TypedView<Buffer>::copy(sizeof(Scalar));
	// 		new (view.mutate().data()) Scalar(value);
	// 		return view;
	// 	}

	// 	Buffer() = default;
	// 	explicit Buffer(std::uint32_t value) : _length(value) {}
	// 	explicit Buffer(std::span<const Type> data) : _length(data.size())
	// 	{
	// 		_insert(0, data);
	// 	}
	// 	explicit Buffer(std::string_view data) requires std::is_same_v<Type, char>
	// 		: Buffer(std::span(
	// 			reinterpret_cast<const Type*>(data.data()),
	// 			data.size()
	// 		))
	// 	{}
	// 	explicit Buffer(View data) requires (
	// 		std::is_same_v<Type, char> ||
	// 		std::is_same_v<Type, unsigned char>
	// 		) : Buffer(std::span(
	// 			reinterpret_cast<const Type*>(data.data().data()),
	// 			data.data().size()
	// 		))
	// 	{}

	// 	enum rOp : proc_opcode
	// 	{
	// 		Range = 'r',
	// 		Get = 'g'
	// 	};
	// 	enum wOp : proc_opcode
	// 	{
	// 		Insert = 'i',
	// 		Set = 's',
	// 		Reserve = 'r'
	// 	};
	// 	enum fOp : proc_opcode
	// 	{
	// 		Equal = 'e',
	// 		EqualCaseInsensitive = 'E'
	// 	};

	// 	template<wOp Op>
	// 	struct WritePair
	// 	{
	// 		using param = Scalar;
	// 	};
	// 	template<fOp Op>
	// 	struct FilterPair
	// 	{
	// 		using param = Scalar;
	// 	};

	// 	std::span<const Type> data() const noexcept
	// 	{
	// 		return std::span(
	// 			static_cast<const Type*>(this->_dynamic_field()),
	// 			_length
	// 		);
	// 	}
	// 	std::span<Type> data() noexcept
	// 	{
	// 		return std::span(
	// 			static_cast<Type*>(this->_dynamic_field()),
	// 			_length
	// 		);
	// 	}

	// 	View view() const noexcept
	// 	{
	// 		const auto beg = reinterpret_cast<const unsigned char*>(this);
	// 		const auto end = beg + storage();
	// 		if constexpr (byte::is_storage_endian())
	// 		{
	// 			return View::view(std::span(beg, end));
	// 		}
	// 		else
	// 		{
	// 			return make(byte::byteswap(_value));
	// 		}
	// 	}
	// 	key_type hash() const noexcept
	// 	{
	// 		return RuntimeInterfaceReflection::hash(std::span(
	// 			static_cast<const unsigned char*>(this->_dynamic_field()),
	// 			sizeof(Type) * _length
	// 		));
	// 	}

	// 	constexpr std::size_t storage() const noexcept
	// 	{
	// 		return sizeof(Type);
	// 	}
	// 	std::string print() const noexcept
	// 	{
	// 		return std::to_string(_value);
	// 	}

	// 	wproc_query_result wproc(proc_opcode opcode, proc_param arguments, wproc_query query) noexcept
	// 	{
	// 		if (query == wproc_query::Commit)
	// 		{

	// 		}
	// 		else if (query == wproc_query::Storage)
	// 		{

	// 		}
	// 		return wproc_type::Dynamic;
	// 	}
	// 	rproc_result rproc(proc_opcode, proc_param) const noexcept
	// 	{

	// 	}
	// 	bool fproc(proc_opcode opcode, proc_param arguments) const noexcept
	// 	{

	// 	}
	// };
}

#endif // RDB_TYPES_HPP
