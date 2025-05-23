#ifndef RDB_BUFFER_HPP
#define RDB_BUFFER_HPP

#include <rdb_schema.hpp>
#include <rdb_locale.hpp>
#include <Types/rdb_tuple.hpp>
#include <Types/rdb_scalar.hpp>
#include <sstream>
#include <numeric>

namespace rdb::type
{
	namespace impl
	{
		template<typename Type, bool Const, bool Trivial>
		class BufferIterator;

		template<typename Type, bool Const>
		class BufferIterator<Type, Const, true>
		{
		public:
			using value_type = std::conditional_t<Const, const Type, Type>;
			using pointer = value_type*;
			using reference = value_type&;
			using difference_type = std::ptrdiff_t;
			using iterator_category = std::random_access_iterator_tag;
		private:
			pointer _ptr{ nullptr };
			std::size_t _size{};
			std::size_t _idx{};
		public:
			explicit BufferIterator(
				std::size_t idx = 0,
				std::size_t size = 0,
				pointer ptr = nullptr
			) : _ptr(ptr), _size(size), _idx(idx) {}

			reference operator*() const noexcept { return *_ptr; }
			pointer operator->() const noexcept { return _ptr; }

			reference operator[](difference_type n) const noexcept { return _ptr[n]; }

			BufferIterator& operator++() noexcept { ++_ptr; return *this; }
			BufferIterator& operator--() noexcept { --_ptr; return *this; }

			BufferIterator operator++(int) noexcept { BufferIterator tmp = *this; ++(*this); return tmp; }
			BufferIterator operator--(int) noexcept { BufferIterator tmp = *this; --(*this); return tmp; }

			BufferIterator operator+(difference_type n) const noexcept { return BufferIterator(_ptr + n); }
			BufferIterator operator-(difference_type n) const noexcept { return BufferIterator(_ptr - n); }

			BufferIterator& operator+=(difference_type n) noexcept { _ptr += n; return *this; }
			BufferIterator& operator-=(difference_type n) noexcept { _ptr -= n; return *this; }

			difference_type operator-(const BufferIterator& other) const noexcept { return _ptr - other._ptr; }

			bool operator==(const BufferIterator& other) const noexcept { return _ptr == other._ptr; }
			bool operator!=(const BufferIterator& other) const noexcept { return _ptr != other._ptr; }
			bool operator<(const BufferIterator& other) const noexcept { return _ptr < other._ptr; }
			bool operator<=(const BufferIterator& other) const noexcept { return _ptr <= other._ptr; }
			bool operator>(const BufferIterator& other) const noexcept { return _ptr > other._ptr; }
			bool operator>=(const BufferIterator& other) const noexcept { return _ptr >= other._ptr; }
		};

		template<typename Type, bool Const>
		class BufferIterator<Type, Const, false>
		{
		public:
			using value_type = std::conditional_t<Const, const Type, Type>;
			using pointer = value_type*;
			using reference = value_type&;
			using difference_type = std::ptrdiff_t;
			using iterator_category = std::random_access_iterator_tag;
		private:
			pointer _ptr{ nullptr };
			std::size_t _size{};
			std::size_t _idx{};
		public:
			explicit BufferIterator(
				std::size_t idx = 0,
				std::size_t size = 0,
				pointer ptr = nullptr
			) : _ptr(ptr), _size(size), _idx(idx) {}

			reference operator*() const noexcept { return *_ptr; }
			pointer operator->() const noexcept { return _ptr; }

			reference operator[](difference_type n) const noexcept { return _ptr[n]; }

			BufferIterator& operator++() noexcept { ++_ptr; return *this; }
			BufferIterator& operator--() noexcept { --_ptr; return *this; }

			BufferIterator operator++(int) noexcept { BufferIterator tmp = *this; ++(*this); return tmp; }
			BufferIterator operator--(int) noexcept { BufferIterator tmp = *this; --(*this); return tmp; }

			BufferIterator operator+(difference_type n) const noexcept { return BufferIterator(_ptr + n); }
			BufferIterator operator-(difference_type n) const noexcept { return BufferIterator(_ptr - n); }

			BufferIterator& operator+=(difference_type n) noexcept { _ptr += n; return *this; }
			BufferIterator& operator-=(difference_type n) noexcept { _ptr -= n; return *this; }

			difference_type operator-(const BufferIterator& other) const noexcept { return _ptr - other._ptr; }

			bool operator==(const BufferIterator& other) const noexcept { return _ptr == other._ptr; }
			bool operator!=(const BufferIterator& other) const noexcept { return _ptr != other._ptr; }
			bool operator<(const BufferIterator& other) const noexcept { return _ptr < other._ptr; }
			bool operator<=(const BufferIterator& other) const noexcept { return _ptr <= other._ptr; }
			bool operator>(const BufferIterator& other) const noexcept { return _ptr > other._ptr; }
			bool operator>=(const BufferIterator& other) const noexcept { return _ptr >= other._ptr; }
		};
	}

	template<typename Type, bool Fragmented>
	class alignas(std::max(alignof(Type), alignof(std::uint64_t)))
		BufferBase :
			public InterfaceMake<BufferBase<Type, Fragmented>>,
			public InterfaceHelper<BufferBase<Type, Fragmented>>
	{
	private:
		static constexpr auto _is_dynamic = Type::uproperty.is(Type::uproperty.dynamic);
		static constexpr auto _is_trivial = Type::uproperty.is(Type::uproperty.trivial);
	public:
		using list = std::initializer_list<TypedView<Type>>;
		using Iterator = impl::BufferIterator<Type, false, !_is_dynamic>;
		using ConstIterator = impl::BufferIterator<Type, true, !_is_dynamic>;

		struct Reserve
		{
			std::size_t size{};
		};
	private:
		static constexpr auto _sbo_max =
			(sizeof(std::uint64_t) * 2 - 1) / Type::static_storage();
		static constexpr auto _volume_mask =
			std::uint64_t(
				byte::byteswap_for_storage<std::uint64_t>(
					~std::uint64_t(0) << 1
				)
			);
		static constexpr auto _sbo_tag =
			std::uint8_t(0b00000001);
		static constexpr auto _sbo_mask =
			std::uint8_t(0b11111110);

		union
		{
			struct
			{
				std::uint64_t length;
				std::uint64_t volume;
			} _std;
			struct
			{
				std::conditional_t<(_sbo_max > 0),
					std::array<Type, _sbo_max>,
					std::array<unsigned char, sizeof(std::uint64_t) * 2 - 1>
				> buffer;
				std::uint8_t length;
			} _sbo;
		};

		bool _has_sbo() const noexcept
		{
			if constexpr (_sbo_max)
				return (_sbo.length & _sbo_tag) == _sbo_tag;
			else
				return false;
		}
		bool _has_sbo(std::size_t size) const noexcept
		{
			return size < _sbo_max;
		}

		std::size_t _total_volume() const noexcept
		{
			if (_has_sbo())
				return sizeof(BufferBase);
			return
				sizeof(BufferBase) +
				byte::byteswap_for_storage(_std.volume & _volume_mask);
		}
		std::size_t _volume() const noexcept
		{
			if (_has_sbo())
			{
				const auto len = _size();
				if constexpr (_is_dynamic)
				{
					std::size_t off = 0;
					while (off < len)
					{
						auto ptr = _buffer(off);
						off += ptr->storage();
					}
					return off;
				}
				else
				{
					return len * Type::static_storage();
				}
			}
			return byte::byteswap_for_storage(_std.volume & _volume_mask);
		}
		std::size_t _size() const noexcept
		{
			if constexpr (_sbo_max)
			{
				if (_has_sbo())
					return byte::byteswap_for_storage(_sbo.length & _sbo_mask);
			}
			return byte::byteswap_for_storage(_std.length);
		}

		void _set_size(std::size_t size) noexcept
		{
			if (_has_sbo(size))
			{
				_sbo.length = static_cast<std::uint8_t>(size);
				_sbo.length |= _sbo_tag;
			}
			else
				_std.length = byte::byteswap_for_storage(size);
		}
		void _set_volume(std::size_t vol, std::size_t size = ~0ull) noexcept
		{
			if (size > _sbo_max)
			{
				_std.volume = byte::byteswap_for_storage(vol);
			}
		}
		void _set_dims(std::size_t size, std::size_t vol) noexcept
		{
			_set_size(size);
			_set_volume(vol, size);
		}

		std::span<const unsigned char> _binary_buffer(std::size_t off = 0) const noexcept
		{
			if constexpr (_sbo_max)
			{
				if (_has_sbo())
				{
					return std::span(reinterpret_cast<const unsigned char*>(
						_sbo.buffer.data()
					) + off, std::dynamic_extent);
				}
			}
			return std::span(static_cast<const unsigned char*>(
				this->_dynamic_field()
			) + off, std::dynamic_extent);
		}
		std::span<unsigned char> _binary_buffer(std::size_t off = 0) noexcept
		{
			return std::span(
				const_cast<unsigned char*>(
					const_cast<const BufferBase*>(this)->_binary_buffer(off).data()
				),
				std::dynamic_extent
			);
		}

		const Type* _buffer(std::size_t off = 0) const noexcept
		{
			if constexpr (_sbo_max)
			{
				if (_has_sbo())
				{
					return reinterpret_cast<const Type*>(reinterpret_cast<const unsigned char*>(
						_sbo.buffer.data()
					) + off);
				}
			}
			return reinterpret_cast<const Type*>(static_cast<const unsigned char*>(
				this->_dynamic_field()
			) + off);
		}
		Type* _buffer(std::size_t off = 0) noexcept
		{
			return const_cast<Type*>(
				const_cast<const BufferBase*>(this)->_buffer(off)
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
				const_cast<const BufferBase*>(this)->_at_impl(idx)
			);
		}
	public:
		template<typename... Argv>
		static auto mstorage(const Argv&... args) noexcept
		{
			if constexpr (sizeof...(Argv))
			{
				if constexpr (sizeof...(Argv) < _sbo_max)
				{
					return sizeof(BufferBase);
				}
				else
				{
					return
						sizeof(BufferBase) +
						(Type::mstorage(args) + ...);
				}
			}
			else
			{
				return sizeof(BufferBase);
			}
		}
		static auto mstorage(const list& cpy) noexcept
		{
			if constexpr (_is_dynamic)
			{
				return mstorage() + std::accumulate(cpy.begin(), cpy.end(), std::size_t{ 0 }, [](const auto& value, std::size_t ctr) {
					return ctr + value->storage();
				});
			}
			else
			{
				return
					mstorage() +
					cpy.size() * Type::static_storage();
			}
		}
		static auto mstorage(const Reserve& res) noexcept
		{
			if (res.size)
			{
				if (res.size < _sbo_max)
				{
					return sizeof(BufferBase);
				}
				else
				{
					return
						sizeof(BufferBase) +
						(res.size * Type::mstorage());
				}
			}
			else
			{
				return sizeof(BufferBase);
			}
		}

		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Argv&&... args) noexcept
		{
			new (view.data()) BufferBase{ std::forward<Argv>(args)... };
			return mstorage(args...);
		}

		BufferBase()
		{
			_set_dims(0, 0);
		}
		template<typename Arg, typename... Argv>
		BufferBase(Arg&& arg, Argv&&... args)
			requires (!std::is_same_v<Arg, Reserve> && !std::is_same_v<Arg, list>)
		{
			_set_size(sizeof...(Argv) + 1);
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
			_set_volume(off, sizeof...(Argv) + 1);
		}
		BufferBase(list res)
		{
			_set_size(res.size());
			std::size_t off = 0;
			for (decltype(auto) it : res)
			{
				const auto beg = off;
				off += it.size();
				std::memcpy(
					_binary_buffer(beg),
					it.data().data(),
					it.size()
				);
			}
			_set_volume(off, res.size());
		}
		BufferBase(const Reserve& res)
		{
			_set_dims(res.size, res.size * Type::mstorage());
		}

		enum rOp : proc_opcode
		{
			Range = 'R',
			Read = 'r'
		};
		enum wOp : proc_opcode
		{
			Insert = 'i',
			Write = 'w'
		};
		enum fOp : proc_opcode
		{
			Smaller = proc_opcode(SortFilterOp::Smaller),
			Larger = proc_opcode(SortFilterOp::Larger),
			Equal = proc_opcode(SortFilterOp::Equal),
		};

		template<rOp Op> struct ReadPair { };
		template<> struct ReadPair<rOp::Range>
		{
			using param = Tuple<Uint64, Uint64>;
			using result = BufferBase;
		};
		template<> struct ReadPair<rOp::Read>
		{
			using param = Uint64;
			using result = Type;
		};

		template<wOp Op> struct WritePair { };
		template<> struct WritePair<wOp::Insert>
		{
			using param = Tuple<Uint64, BufferBase>;
		};
		template<> struct WritePair<wOp::Write>
		{
			using param = Tuple<Uint64, Type>;
		};

		template<fOp Op>
		struct FilterPair
		{
			using param = BufferBase;
		};

		std::size_t size() const noexcept
		{
			return _size();
		}

		const Type& at(std::size_t idx) const noexcept
		{
			return *_at_impl(idx);
		}
		Type& at(std::size_t idx) noexcept
		{
			return *_at_impl(idx);
		}

		Iterator begin() noexcept
		{
			return Iterator(
				0,
				_size(),
				_buffer()
			);
		}
		Iterator end() noexcept
		{
			return Iterator(
				_size(),
				_size(),
				_buffer()
			);
		}

		ConstIterator begin() const noexcept
		{
			return ConstIterator(
				0,
				_size(),
				_buffer()
			);
		}
		ConstIterator end() const noexcept
		{
			return ConstIterator(
				_size(),
				_size(),
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
			return _total_volume();
		}
		std::string print() const noexcept
		{
			if constexpr (
				std::is_same_v<Type, Character> ||
				std::is_same_v<Type, U8Character> ||
				std::is_same_v<Type, U16Character> ||
				std::is_same_v<Type, U32Character>
			)
			{
				using type = std::remove_cvref_t<decltype(std::declval<Type>().value())>;
				return std::format("'{}'",
					std::basic_string_view<type>(
						_buffer()->underlying(), _size() + 1
					)
				);
			}
			else
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

	template<typename Type>
	class Buffer :
		public BufferBase<Type, false>,
		public Interface<
			Buffer<Type>,
			cmp::concat_const_string<"buf<", Type::cuname, ">">(),
			true
		>
	{ };

	template<typename Type>
	class FragmentedBuffer :
		public BufferBase<Type, true>,
		public Interface<
			FragmentedBuffer<Type>,
			cmp::concat_const_string<"fbuf<", Type::cuname, ">">(),
			true
		>
	{ };

	using Binary = FragmentedBuffer<Byte>;

	using String = Buffer<Character>;
	using U8String = Buffer<U8Character>;
	using U16String = Buffer<U16Character>;
	using U32String = Buffer<U32Character>;
}

#endif // RDB_BUFFER_HPP
