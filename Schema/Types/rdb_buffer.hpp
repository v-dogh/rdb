#ifndef RDB_BUFFER_HPP
#define RDB_BUFFER_HPP

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
	template<typename Type, bool Fragmented>
	class BufferBase :
		public InterfaceMake<BufferBase<Type, Fragmented>>,
		public InterfaceHelper<BufferBase<Type, Fragmented>>
	{
	private:
		static constexpr auto _is_dynamic = Type::uproperty.is(Type::uproperty.dynamic);
		static constexpr auto _is_trivial = Type::uproperty.is(Type::uproperty.trivial);
		using trivial_interface = impl::TrivialInterface<Type>;
	public:
		using trivial_type = typename trivial_interface::value_type;
		using view_list = std::span<const TypedView<Type>>;
		using list = std::initializer_list<TypedView<Type>>;
		using trivial_list = std::span<const trivial_type>;
		using iterator = impl::ArrayIterator<Type, false, !_is_dynamic>;
		using const_iterator = impl::ArrayIterator<Type, true, !_is_dynamic>;

		struct Reserve
		{
			std::size_t size{};
		};
	private:
		static constexpr auto _is_trivial_string =
			std::is_same_v<Type, Character> ||
			std::is_same_v<Type, U8Character> ||
			std::is_same_v<Type, U16Character> ||
			std::is_same_v<Type, U32Character>;
		static constexpr auto _is_string =
			_is_trivial_string ||
			std::is_same_v<Type, Byte>;
		static constexpr auto _sortable = Type::uproperty.is(Type::uproperty.sortable);
		static constexpr auto _sbo_max =
			sizeof(std::uint64_t) * 2 - 1;
		static constexpr auto _volume_mask =
			byte::byteswap_for_storage<std::uint64_t>(
				~std::uint64_t(0) >> 1
			);
		static constexpr auto _sbo_tag =
			std::uint8_t(0b10000000);
		static constexpr auto _sbo_mask =
			std::uint8_t(0b01111111);

		union
		{
			struct
			{
				std::uint64_t length;
				std::uint64_t volume;
			} _std{ 0, 0 };
			struct
			{
				std::array<unsigned char, _sbo_max> buffer;
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
				return _sbo_max;
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
					return std::span(_sbo.buffer.data() + off, std::dynamic_extent);
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
					return reinterpret_cast<const Type*>(_sbo.buffer.data() + off);
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

		const Type* _at(std::size_t off) const noexcept
		{
			return reinterpret_cast<const Type*>(
				reinterpret_cast<const unsigned char*>(this) + off
			);
		}
		Type* _at(std::size_t off) noexcept
		{
			return reinterpret_cast<Type*>(
				reinterpret_cast<unsigned char*>(this) + off
			);
		}
	public:
		template<typename... Argv>
		static auto minline(std::span<unsigned char> view, Argv&&... args) noexcept
		{
			const auto s = mstorage(args...);
			new (view.data()) BufferBase{ std::forward<Argv>(args)... };
			return s;
		}

		static auto minline(std::span<unsigned char> view, const std::basic_string<trivial_type>& str) noexcept requires _is_string
		{
			return minline(view, std::basic_string_view<trivial_type>(str));
		}

		static auto mstorage(view_list cpy) noexcept
		{
			std::size_t size = 0;
			if constexpr (_is_dynamic)
			{
				size = std::accumulate(
					cpy.begin(),
					cpy.end(),
					std::size_t{ 0 },
					[](std::size_t ctr, const auto& value) {
						return ctr + value->storage();
					}
				);
			}
			else
			{
				size = cpy.size() * Type::static_storage();
			}
			if (size > _sbo_max)
				return sizeof(BufferBase) + size;
			return sizeof(BufferBase);
		}
		static auto mstorage(list cpy) noexcept
		{
			return mstorage(view_list(cpy.begin(), cpy.end()));
		}
		static auto mstorage(Reserve res) noexcept
		{
			if (res.size)
			{
				if (res.size * Type::mstorage() < _sbo_max)
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

		static auto mstorage(const std::basic_string<trivial_type>& value) requires _is_string
		{
			return mstorage(Reserve(value.size()));
		}
		static auto mstorage(std::basic_string_view<trivial_type> value) requires _is_string
		{
			return mstorage(Reserve(value.size()));
		}
		static auto mstorage(const trivial_type* ptr) requires _is_string
		{
			return mstorage(
				std::basic_string_view<
					trivial_type
				>(ptr)
			);
		}
		static auto mstorage(trivial_list str) requires _is_trivial
		{
			return mstorage(
				std::basic_string_view<
					trivial_type
				>(str.begin(), str.end())
			);
		}

		template<typename... Argv>
		static auto mstorage(const Argv&... args) noexcept
		{
			if constexpr (sizeof...(Argv))
			{
				if ((Type::mstorage(args) + ... + 0) < _sbo_max)
				{
					return sizeof(BufferBase);
				}
				else
				{
					if constexpr (_is_dynamic)
					{
						return
							sizeof(BufferBase) +
							(Type::mstorage(args) + ...);
					}
					else
					{
						return
							sizeof(BufferBase) +
							(Type::static_storage() * sizeof...(Argv));
					}
				}
			}
			else
			{
				return sizeof(BufferBase);
			}
		}

		template<typename... Argv>
		static auto mpinline(std::span<unsigned char> view, rdb::Order order, Argv&&... args) noexcept requires _sortable
		{
			static_assert("Not implemented");
		}

		static auto mpinline(std::span<unsigned char> view, rdb::Order order, std::basic_string_view<trivial_type> str) noexcept requires _is_string && _sortable
		{
			std::memcpy(view.data(), str.data(), str.size() * sizeof(Type));
			return str.size() * sizeof(Type);
		}
		static auto mpinline(std::span<unsigned char> view, rdb::Order order, const std::basic_string<trivial_type>& str) noexcept requires _is_string && _sortable
		{
			return mpinline(view, order, std::basic_string_view<trivial_type>(str.begin(), str.end()));
		}

		static auto mpstorage(rdb::Order order, view_list cpy) noexcept requires _sortable
		{
			return std::accumulate(
				cpy.begin(),
				cpy.end(),
				std::size_t{ 0 },
				[&](std::size_t ctr, const auto& value) {
					return ctr + value->prefix_length(order);
				}
			);
		}
		static auto mpstorage(rdb::Order order, list cpy) noexcept requires _sortable
		{
			return mpstorage(view_list(cpy.begin(), cpy.end()));
		}

		static auto mpstorage(rdb::Order order, const std::basic_string<trivial_type>& value) requires _is_string && _sortable
		{
			return value.size() * sizeof(Type);
		}
		static auto mpstorage(rdb::Order order, std::basic_string_view<trivial_type> value) requires _is_string && _sortable
		{
			return value.size() * sizeof(Type);
		}
		static auto mpstorage(rdb::Order order, const trivial_type* ptr) requires _is_string && _sortable
		{
			return std::basic_string_view<trivial_type>(ptr).size() * Type::mpstorage(order);
		}
		static auto mpstorage(rdb::Order order, trivial_list str) requires _is_trivial && _sortable
		{
			return str.size() * Type::mpstorage(order);
		}

		template<typename... Argv>
		static auto mpstorage(rdb::Order order, const Argv&... args) noexcept requires _sortable
		{
			return (Type::mpstorage(args, order) + ...);
		}

		BufferBase()
		{
			_set_dims(0, 0);
		}
		template<typename Arg, typename... Argv>
		explicit BufferBase(Arg&& arg, Argv&&... args)
			requires (
				!std::is_same_v<std::decay_t<Arg>, Reserve> &&
				!std::is_same_v<std::decay_t<Arg>, list> &&
				!std::is_same_v<std::decay_t<Arg>, view_list> &&
				!std::is_same_v<std::decay_t<Arg>, const trivial_type*> &&
				!std::is_same_v<std::decay_t<Arg>, std::basic_string_view<trivial_type>> &&
				!std::is_same_v<std::decay_t<Arg>, trivial_list>
			)
		{
			const auto size = Type::mstorage(arg) + (Type::mstorage(args) + ... + 0);
			_set_dims(size, size);
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
		explicit BufferBase(list li) :
			BufferBase(std::span(li.begin(), li.end()))
		{}
		explicit BufferBase(view_list li)
		{
			const auto size = std::accumulate(li.begin(), li.end(), std::size_t{ 0 }, [](auto ctr, auto val) {
				return ctr + val.size();
			});
			_set_dims(size, size);
			std::size_t off = 0;
			for (decltype(auto) it : li)
			{
				const auto beg = off;
				off += it.size();
				std::memcpy(
					_buffer(beg),
					it.data().data(),
					it.size()
				);
			}
		}
		explicit BufferBase(const Reserve& res)
		{
			const auto size = res.size * Type::mstorage();
			_set_dims(size, size);
		}
		explicit BufferBase(trivial_type* ptr)
			requires _is_string : BufferBase(
				std::basic_string_view<trivial_type>(ptr)
			) {}
		explicit BufferBase(std::basic_string_view<trivial_type> str)
			requires _is_string
		{
			const auto size = str.size() * Type::static_storage();
			_set_dims(size, size);
			std::memcpy(
				_buffer(),
				str.data(),
				str.size() * sizeof(trivial_type)
			);
		}
		explicit BufferBase(trivial_list str)
			requires _is_trivial
		{
			const auto size = str.size() * Type::static_storage();
			_set_dims(size, size);
			std::memcpy(
				_buffer(),
				str.data(),
				str.size() * sizeof(trivial_type)
			);
		}

		struct Op : InterfaceDeclProcPrimary<
			DeclWrite<
				Type,
				void,
				Uint64,
				Tuple<Type, Character>,
				Tuple<Uint64, BufferBase>,
				Tuple<Uint64, Type>
			>,
			DeclFilter<
				BufferBase, BufferBase, BufferBase
			>,
			DeclRead<
				ReadPair<Tuple<Uint64, Uint64>, BufferBase>,
				ReadPair<Uint64, Type>,
				ReadPair<void, Type>,
				ReadPair<void, Uint64>
			>
		>
		{
			enum w : proc_opcode
			{
				Push,
				Pop,
				Erase,
				EraseIf,
				Insert,
				Write,
			};
			enum r : proc_opcode
			{
				Range,
				Read,
				Back,
				Size
			};
			enum f : proc_opcode
			{
				Smaller = proc_opcode(SortFilterOp::Smaller),
				Larger = proc_opcode(SortFilterOp::Larger),
				Equal = proc_opcode(SortFilterOp::Equal),
			};
		};

		std::size_t size() const noexcept
		{
			if constexpr (_is_trivial)
			{
				return _size() / Type::static_storage();
			}
			else
			{
				std::size_t idx = 0;
				std::size_t off = 0;
				while (off != _size())
				{
					off += _at(off)->storage();
					idx++;
				}
				return idx;
			}
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
			return reinterpret_cast<const trivial_type*>(_buffer());
		}
		trivial_type* data() noexcept
			requires _is_trivial
		{
			return reinterpret_cast<trivial_type*>(_buffer());
		}

		iterator begin() noexcept
		{
			return iterator(
				0,
				_size(),
				_buffer()
			);
		}
		iterator end() noexcept
		{
			return iterator(
				_size(),
				_size(),
				_buffer()
			);
		}

		const_iterator begin() const noexcept
		{
			return const_iterator(
				0,
				_size(),
				_buffer()
			);
		}
		const_iterator end() const noexcept
		{
			return const_iterator(
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
					_binary_buffer().subspan(0, _size())
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

		std::size_t prefix_length(rdb::Order order) const noexcept
		{
			if constexpr (_sortable)
			{
				if constexpr (_is_string)
				{
					return _size() * Type::static_storage();
				}
				else
				{
					std::size_t off = 0;
					std::size_t len = 0;
					for (std::size_t i = 0; i < _size(); i++)
					{
						const auto* ptr = _at(off);
						len += ptr->prefix_length();
						off += ptr->storage();
					}
					return len;
				}
			}
			else
				return 0;
		}
		std::size_t prefix(View buffer, rdb::Order order) const noexcept
		{
			if constexpr (_sortable)
			{
				if constexpr (_is_string)
				{
					const auto len = std::min(prefix_length(order), buffer.size());
					std::memcpy(buffer.mutate().data(), _buffer(), len);
					return len;
				}
				else
				{
					std::size_t off = 0;
					std::size_t len = 0;
					for (std::size_t i = 0; i < _size(); i++)
					{
						const auto* ptr = _at(off);
						len += ptr->prefix(
							buffer.subview(len,
								std::min(
									ptr->prefix_length(),
									buffer.size() - len
								)
							),
							order
						);
						off += ptr->storage();
					}
					return std::min(buffer.size(), len);
				}
			}
			else
				return 0;
		}

		std::size_t storage() const noexcept
		{
			return _total_volume();
		}
		std::string print() const noexcept
		{
			if constexpr (_is_trivial_string)
			{
				return std::format("'{}'",
					std::basic_string_view<trivial_type>(
						_buffer()->underlying(), _size()
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

		wproc_query_result wproc(proc_opcode opcode, const proc_param& arguments, wproc_query query) noexcept
		{
			if constexpr (Fragmented)
			{
				return wproc_type::Delta;
			}
			else
			{
				if (query == wproc_query::Commit)
				{
					if (opcode == Op::Push)
					{
						const auto size = _size();
						const auto req = size + arguments.size();
						if (req > _volume())
						{
							_set_dims(
								req,
								req * 1.5
							);
						}
						else
							_set_size(req);

						std::memcpy(
							_binary_buffer(size).data(),
							arguments.data().data(),
							arguments.size()
						);

						return wproc_status::Ok;
					}
					else if (opcode == Op::EraseIf)
					{
						auto value = TypedView<Tuple<Type, Character>>::view(arguments.data());
						const auto fop = value->template field<1>()->value();
						std::size_t removed = 0;
						for (std::size_t off = 0; off != _size();)
						{
							auto* ptr = _at(off);
							const auto size = ptr->storage();
							if (ptr->fproc(fop, value->template field<0>()))
							{
								removed += size;
								std::memmove(
									ptr,
									_at(off + size),
									storage() - removed
								);
							}
							off += size;
						}
						const auto nsize = _size() - removed;
						_set_dims(
							nsize,
							nsize < _volume() / 2 ? nsize : _volume()
						);

						return wproc_status::Ok;
					}
				}
				else if (query == wproc_query::Storage)
				{
					if (opcode == Op::Push)
					{
						const auto req = _size() + arguments.size();
						if (req <= _volume() || req < _sbo_max)
						{
							if (_has_sbo() && req < _sbo_max)
								return _volume();
							return _total_volume();
						}
						else
						{
							return sizeof(BufferBase) + req * 1.5;
						}
					}
					else if (opcode == Op::EraseIf)
					{
						auto value = TypedView<Tuple<Type, Character>>::view(arguments.data());
						const auto fop = value->template field<1>()->value();
						std::size_t removed = 0;
						for (decltype(auto) it : *this)
							if (it.fproc(fop, value->template field<0>()))
								removed += it.storage();
						const auto nsize = _size() - removed;
						if (nsize < _volume() / 2)
							return nsize;
						return _volume();
					}
				}
				return wproc_type::Dynamic;
			}
			return wproc_status::Error;
		}
		rproc_result rproc(proc_opcode opcode, const proc_param&) const noexcept
		{
			return nullptr;
		}
		bool fproc(proc_opcode opcode, const proc_param& arguments) const noexcept
		{
			TypedView<BufferBase> other = TypedView<BufferBase>::view(
				arguments.data()
			);
			if constexpr (_is_string)
			{
				auto i1 = begin();
				auto i2 = other->begin();
				auto e1 = end();
				auto e2 = other->end();

				auto result = std::strong_ordering::less;
				for (; i1 != e1 && i2 != e2; ++i1, ++i2)
				{
					auto cmp = i1->value() <=> i2->value();
					if (cmp != 0)
					{
						result = cmp;
						break;
					}
				}
				if (i1 == e1)
					if (i2 == e2)
						result = std::strong_ordering::equal;

				if (opcode == Op::Smaller) return result == std::strong_ordering::less;
				else if (opcode == Op::Larger) return result == std::strong_ordering::greater;
				else if (opcode == Op::Equal) return result == std::strong_ordering::equal;
			}
			else
			{
				if (opcode == Op::Equal)
				{
					auto it1 = begin();
					auto end1 = end();
					auto it2 = other->begin();
					auto end2 = other->end();
					while (it1 != end1 && it2 != end2)
					{
						if (!it1->fproc(
								proc_opcode(SortFilterOp::Equal),
								it2->view()
							))
						{
							return false;
						}
						++it1;
						++it2;
					}
				}
				else
				{
					auto it1 = begin();
					auto end1 = end();
					auto it2 = other->begin();
					auto end2 = other->end();
					while (it1 != end1 && it2 != end2)
					{
						if (!it1->fproc(
								proc_opcode(SortFilterOp::Equal),
								it2->view()
							))
						{
							break;
						}
						++it1;
						++it2;
					}

					const auto is_larger =
						it1->fproc(
							proc_opcode(SortFilterOp::Larger),
							it2->view()
						);

					if (opcode == Op::Smaller) return !is_larger;
					else if (opcode == Op::Larger) return is_larger;
				}
			}
			return true;
		}
	};

	template<typename Type>
	class Buffer :
		public BufferBase<Type, false>,
		public Interface<
			Buffer<Type>,
			cmp::concat_const_string<"buf<", Type::cuname, ">">(),
			InterfaceProperty::dynamic |
			(Type::uproperty.is(InterfaceProperty::sortable) ? InterfaceProperty::sortable : 0x00)
		>
	{ };

	template<typename Type>
	class FragmentedBuffer :
		public BufferBase<Type, true>,
		public Interface<
			FragmentedBuffer<Type>,
			cmp::concat_const_string<"fbuf<", Type::cuname, ">">(),
			InterfaceProperty::dynamic | InterfaceProperty::fragmented
		>
	{ };

	using Binary = FragmentedBuffer<Byte>;

	using String = Buffer<Character>;
	using U8String = Buffer<U8Character>;
	using U16String = Buffer<U16Character>;
	using U32String = Buffer<U32Character>;
}

#endif // RDB_BUFFER_HPP
