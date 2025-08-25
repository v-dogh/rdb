#ifndef RDB_NULLABLE_HPP
#define RDB_NULLABLE_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>

namespace rdb::type
{
	struct Null { } inline null;

	template<typename Type>
	class Nullable :
		public InterfaceMake<Nullable<Type>>,
		public InterfaceHelper<Nullable<Type>>,
		public Interface<
			Nullable<Type>,
			cmp::concat_const_string<"opt<", Type::cuname, ">">(),
			InterfaceProperty::dynamic
		>
	{
	private:
		bool _null{ false };

		const Type* _value() const noexcept
		{
			return static_cast<const Type*>(this->_dynamic_field());
		}
		Type* _value() noexcept
		{
			return static_cast<Type*>(this->_dynamic_field());
		}
	public:
		template<typename... Argv>
		static auto mstorage(const Argv&... args) noexcept
		{
			return sizeof(Nullable) + Type::mstorage(args...);
		}
		static auto mstorage(Null) noexcept
		{
			return sizeof(Nullable);
		}

		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Argv&&... args) noexcept
		{
			new (view.data()) Nullable();
			return
				sizeof(Nullable) +
				Type::minline(
					view.subspan(sizeof(Nullable)),
					std::forward<Argv>(args)...
				);
		}
		static auto minline(std::span<unsigned char> view, Null) noexcept
		{
			new (view.data()) Nullable(null);
			return sizeof(Nullable);
		}

		explicit Nullable() = default;
		explicit Nullable(Null) : _null(true) {}

		struct Op : InterfaceDeclProc<Type,
			DeclWrite<>,
			DeclFilter<void>,
			DeclRead<>
		>
		{
			enum w : proc_opcode {};
			enum r : proc_opcode {};
			enum f : proc_opcode
			{
				IsNull = Op::wbase
			};
		};

		const Type* value() const noexcept
		{
			if (_null)
				return nullptr;
			return _value();
		}
		Type* value() noexcept
		{
			if (_null)
				return nullptr;
			return _value();
		}

		key_type hash() const noexcept
		{
			if (_null)
			{
				return uuid::xxhash_combine({ key_type(Interface<
					Nullable<Type>,
					cmp::concat_const_string<"opt<", Type::cuname, ">">(),
					InterfaceProperty::dynamic |
					(InterfaceProperty::fragmented * Type::uproperty.is(InterfaceProperty::fragmented))
				>::ucode) });
			}
			return _value()->hash();
		}

		constexpr std::size_t storage() const noexcept
		{
			if (_null)
				return 1;
			return _value()->storage() + 1;
		}
		std::string print() const noexcept
		{
			if (_null)
			{
				return "<null>";
			}
			else
			{
				return _value()->print();
			}
		}

		wproc_query_result wproc(proc_opcode opcode, const proc_param& arguments, wproc_query query) noexcept
		{
			return _value()->wproc(opcode, arguments, query);
		}
		rproc_result rproc(proc_opcode opcode, const proc_param& arguments) const noexcept
		{
			return _value()->rproc(opcode, arguments);
		}
		bool fproc(proc_opcode opcode, const proc_param& arguments) const noexcept
		{
			if (opcode == Op::IsNull)
				return _null;
			return _value()->fproc(opcode, arguments);
		}
	};
}

#endif // RDB_NULLABLE_HPP
