#ifndef RDB_buffer_HPP
#define RDB_buffer_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>
#include <Types/rdb_tuple.hpp>
#include <Types/rdb_scalar.hpp>
#include <Types/rdb_array_iterator.hpp>
#include <Types/rdb_trivial_helper.hpp>
#include <sstream>
#include <numeric>

namespace rdb::type
{
	template<std::size_t Size, typename Type>
	class alignas(std::max(alignof(Type), alignof(std::uint64_t)))
		ArrayBase :
			public InterfaceMake<ArrayBase<Size, Type>>,
			public InterfaceHelper<ArrayBase<Size, Type>>
	{
	private:
		static constexpr auto _is_dynamic = Type::uproperty.is(Type::uproperty.dynamic);
		static constexpr auto _is_trivial = Type::uproperty.is(Type::uproperty.trivial);
	public:
		using trivial_type = typename impl::TrivialInterface<Type>::value_type;
		using view_list = std::span<const TypedView<Type>>;
		using list = std::initializer_list<TypedView<Type>>;
		using trivial_list = std::span<const trivial_type>;
		using iterator = impl::ArrayIterator<Type, false, !_is_dynamic>;
		using const_iterator = impl::ArrayIterator<Type, true, !_is_dynamic>;
	private:
		std::span<const unsigned char> _binary_buffer(std::size_t off = 0) const noexcept
		{
			return std::span(static_cast<const unsigned char*>(
				this->_dynamic_field(0)
			) + off, std::dynamic_extent);
		}
		std::span<unsigned char> _binary_buffer(std::size_t off = 0) noexcept
		{
			return std::span(
				const_cast<unsigned char*>(
					const_cast<const ArrayBase*>(this)->_binary_buffer(off).data()
				),
				std::dynamic_extent
			);
		}

		const Type* _buffer(std::size_t off = 0) const noexcept
		{
			return reinterpret_cast<const Type*>(static_cast<const unsigned char*>(
				this->_dynamic_field(0)
			) + off);
		}
		Type* _buffer(std::size_t off = 0) noexcept
		{
			return const_cast<Type*>(
				const_cast<const ArrayBase*>(this)->_buffer(off)
			);
		}

		const Type* _at_impl(std::size_t idx) const noexcept
		{
			if constexpr (_is_dynamic)
			{
				std::size_t off = 0;
				while (--idx != 0)
				{
					auto ptr = _buffer(off);
					off += ptr->storage();
				}
				return _buffer(off);
			}
			else
			{
				return _buffer()[idx];
			}
		}
		Type* _at_impl(std::size_t idx) noexcept
		{
			return const_cast<Type*>(
				const_cast<const ArrayBase*>(this)->_at_impl(idx)
			);
		}

		std::size_t _volume() const noexcept
		{
			if constexpr (_is_trivial)
			{
				return Size * Type::static_storage();
			}
			else
			{
				std::size_t s = 0;
				for (decltype(auto) it : *this)
					s += it.storage();
				return s;
			}
		}
	public:
		template<typename... Argv>
		static auto mstorage(const Argv&... args) noexcept
		{
			static_assert(sizeof...(Argv) == Size || sizeof...(Argv) == 0);
			if constexpr (sizeof...(Argv))
			{
				if constexpr (_is_dynamic)
				{
					return (Type::mstorage(args) + ...);
				}
				else
				{
					return (Type::static_storage() * Size);
				}
			}
			else
			{
				return Type::mstorage() * Size;
			}
		}

		static auto mstorage(view_list cpy) noexcept
		{
			std::size_t size = 0;
			if constexpr (_is_dynamic)
			{
				const auto m = std::min(cpy.size(), Size);
				size = std::accumulate(
					cpy.begin(),
					cpy.begin() + m,
					std::size_t{ 0 },
					[](std::size_t ctr, const auto& value) {
						return ctr + value->storage();
					}
				);
				if (cpy.size() < Size)
				{
					size += (Size - cpy.size()) * Type::mstorage();
				}
			}
			else
			{
				size = Size * Type::static_storage();
			}
			return size;
		}
		static auto mstorage(list cpy) noexcept
		{
			return mstorage(view_list(cpy.begin(), cpy.end()));
		}

		static auto mstorage(trivial_list cpy) noexcept
			requires _is_trivial
		{
			return Size * Type::static_storage();
		}

		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Argv&&... args) noexcept
		{
			const auto s = mstorage(args...);
			new (view.data()) ArrayBase{ std::forward<Argv>(args)... };
			return s;
		}

		ArrayBase()
		{
			std::size_t off = 0;
			for (std::size_t i = 0; i < Size; i++)
			{
				const auto beg = off;
				off += Type::mstorage();
				Type::minline(_binary_buffer(beg));
			}
		}
		template<typename Arg, typename... Argv>
		explicit ArrayBase(Arg&& arg, Argv&&... args)
			requires (
				!std::is_same_v<std::decay_t<Arg>, list> &&
				!std::is_same_v<std::decay_t<Arg>, view_list>
			)
		{
			std::size_t off = 0;
			auto write = [&]<typename Value>(Value&& arg) {
				const auto beg = off;
				off += Type::mstorage(arg);
				Type::minline(
					_binary_buffer(beg),
					std::forward<Value>(arg)
				);
			};
			(write(std::forward<Arg>(arg)));
			(write(std::forward<Argv>(args)), ...);
		}
		explicit ArrayBase(list li) :
			ArrayBase(std::span(li.begin(), li.end()))
		{}
		explicit ArrayBase(view_list li)
		{
			std::size_t off = 0;
			for (auto it = li.begin(); it != li.begin() + std::min(Size, li.size()); ++it)
			{
				const auto beg = off;
				off += it->size();
				std::memcpy(
					_binary_buffer(beg).data(),
					it->data().data(),
					it->size()
				);
			}
			if (li.size() < Size)
			{
				for (std::size_t i = 0; i < Size - li.size(); i++)
				{
					const auto beg = off;
					off += Type::mstorage();
					Type::minline(_binary_buffer(beg));
				}
			}
		}
		explicit ArrayBase(trivial_list li)
			requires _is_trivial
		{
			std::size_t off = std::min(Size, li.size()) * sizeof(trivial_type);
			std::memcpy(
				_buffer(),
				li.data(),
				off
			);
			if (li.size() < Size)
			{
				for (std::size_t i = 0; i < Size - li.size(); i++)
				{
					const auto beg = off;
					off += Type::mstorage();
					Type::minline(_binary_buffer(beg));
				}
			}
		}

		struct Op : InterfaceDeclProcPrimary<
			DeclWrite<
				Tuple<Uint64, ArrayBase>,
				Tuple<Uint64, Type>
			>,
			DeclFilter<>,
			DeclRead<
				rdb::ReadPair<Tuple<Uint64, Uint64>, ArrayBase>,
				rdb::ReadPair<Uint64, Type>
			>
		>
		{
			enum w : proc_opcode
			{
				// [ Offset, Values ]
				Overwrite,
				// [ Offset, Value ]
				Write
			};
			enum r : proc_opcode
			{
				Range,
				Read
			};
			enum f : proc_opcode
			{
				// [ Position ] -> [ Value ]
				Test
			};
		};


		std::size_t size() const noexcept
		{
			return Size;
		}

		const Type& at(std::size_t idx) const noexcept
		{
			return *_at_impl(idx);
		}
		Type& at(std::size_t idx) noexcept
		{
			return *_at_impl(idx);
		}

		const trivial_type* data() const noexcept
			requires _is_trivial
		{
			return _buffer()->underlying();
		}
		trivial_type* data() noexcept
			requires _is_trivial
		{
			return _buffer()->underlying();
		}

		iterator begin() noexcept
		{
			return iterator(
				0,
				_volume(),
				_buffer()
			);
		}
		iterator end() noexcept
		{
			const auto vol = _volume();
			return iterator(
				vol,
				vol,
				_buffer()
			);
		}

		const_iterator begin() const noexcept
		{
			return const_iterator(
				0,
				_volume(),
				_buffer()
			);
		}
		const_iterator end() const noexcept
		{
			const auto vol = _volume();
			return const_iterator(
				vol,
				vol,
				_buffer()
			);
		}

		key_type hash() const noexcept
		{
			if constexpr (_is_trivial)
			{
				return uuid::xxhash(
					_binary_buffer().subspan(0, _volume())
				);
			}
			else
			{
				key_type result = 0x00;
				for (decltype(auto) it : *this)
				{
					result = uuid::xxhash_combine(
						result,
						it.hash()
					);
				}
				return result;
			}
		}

		std::size_t storage() const noexcept
		{
			return _volume();
		}
		std::string print() const noexcept
		{
			std::stringstream out;

			out << "[ ";
			bool first = true;
			for (decltype(auto) it : *this)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					out << ", ";
				}

				out
					<< "'"
					<< it.print()
					<< "'";
			}
			out << " ]";

			return out.str();
		}

		wproc_query_result wproc(proc_opcode opcode, const proc_param& arguments, wproc_query query) noexcept
		{

		}
		rproc_result rproc(proc_opcode opcode, const proc_param&) const noexcept
		{

		}
		bool fproc(proc_opcode opcode, const proc_param& arguments) const noexcept { }
	};

	template<std::size_t Size, typename Type>
	class Array :
		public ArrayBase<Size, Type>,
		public Interface<
			Array<Size, Type>,
			cmp::concat_const_string<"arr", cmp::int_to_const_string<Size>(), "<", Type::cuname, ">">(),
			Type::uproperty
		>
	{ };

	template<std::size_t Size>
	using BinaryArray = Array<Size, Byte>;
}

#endif // RDB_buffer_HPP
