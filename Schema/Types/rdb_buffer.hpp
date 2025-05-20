#ifndef RDB_BUFFER_HPP
#define RDB_BUFFER_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>

namespace rdb::type
{
	template<typename Type>
	class alignas(Type) Buffer : public Interface<
		Buffer<Type>,
		cmp::concat_const_string<"buf<", Type::cuname, ">">(),
		true
	>
	{
	private:

	private:
		std::uint32_t _length{ 0 };
	public:
		template<typename... Argv>
		static auto mstorage(const Argv&... args) noexcept
		{
			static_assert(sizeof...(Argv) == sizeof...(Ts), "Tuple must be either default-initialized or have all elements initialized");
			return (Ts::mstorage(args) + ...);
		}
		static auto mstorage() noexcept
		{
			return (Ts::mstorage() + ...);
		}
		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Argv&&... args) noexcept
		{
			static_assert(sizeof...(Argv) == sizeof...(Ts), "Tuple must be either default-initialized or have all elements initialized");
			new (view.data()) Tuple{ std::forward<Argv>(args)... };
			return mstorage(args...);
		}
		template<typename... Argv>
		static auto make(Argv&&... args) noexcept
		{
			static_assert(sizeof...(Argv) == sizeof...(Ts), "Tuple must be either default-initialized or have all elements initialized");
			auto view = TypedView<Tuple>::copy(mstorage(args...));
			new (view.mutate().data()) Tuple(std::forward<Argv>(args)...);
			return view;
		}

		Buffer() = default;

		enum rOp : proc_opcode
		{
			Range = 'r'
		};
		enum wOp : proc_opcode
		{
			Insert = 'i'
		};
		enum fOp : proc_opcode
		{
			Smaller = proc_opcode(SortFilterOp::Smaller),
			Larger = proc_opcode(SortFilterOp::Larger),
			Equal = proc_opcode(SortFilterOp::Equal),
		};

		template<wOp Op>
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

		void view(View view) const noexcept
		{
			if constexpr (byte::is_storage_endian())
			{
				std::memcpy(view.mutate().data(), this, storage());
			}
			else
			{

			}
		}
		key_type hash() const noexcept
		{

		}

		std::size_t storage() const noexcept
		{

		}
		std::string print() const noexcept
		{

		}

		wproc_query_result wproc(proc_opcode opcode, proc_param arguments, wproc_query query) noexcept
		{

		}
		rproc_result rproc(proc_opcode opcode, proc_param) const noexcept
		{

		}
		bool fproc(proc_opcode opcode, proc_param arguments) const noexcept
		{

		}
	};
}

#endif // RDB_BUFFER_HPP
