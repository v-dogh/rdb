#ifndef RDB_BITSET_HPP
#define RDB_BITSET_HPP

#include <Types/rdb_tuple.hpp>
#include <Types/rdb_scalar.hpp>
#include <rdb_schema.hpp>
#include <sstream>

namespace rdb::type
{
	namespace impl
	{
		template<std::size_t Size>
		class BitBuffer
		{
		public:
			struct Set
			{
				std::size_t pos{ 0 };
			};
		private:
			using storage_type = unsigned int;
			static constexpr unsigned int bitlen_ = 8 * sizeof(storage_type);
			static constexpr unsigned int divcnt_ = std::countr_zero(bitlen_);
			static constexpr unsigned int remcnt_ = bitlen_ - 1;

			std::array<
				storage_type,
				(Size + remcnt_) / bitlen_
			> _data{};
		private:
			constexpr std::pair<unsigned int, unsigned int> _offs(std::size_t idx) const noexcept
			{
				return {
					static_cast<unsigned int>(idx >> divcnt_),
					static_cast<unsigned int>(idx & remcnt_)
				};
			}

			constexpr bool _get(std::size_t idx) const noexcept
			{
				const auto [quot, rem] = _offs(idx);
				const storage_type mask = 1u << rem;
				return _data[quot] & mask;
			}
			constexpr void _set(std::size_t idx, bool value) noexcept
			{
				const auto [quot, rem] = _offs(idx);
				const storage_type mask = 1u << rem;
				_data[quot] = (_data[quot] & ~mask) | (mask * value);
			}
			constexpr void _flip(std::size_t idx) noexcept
			{
				const auto [quot, rem] = _offs(idx);
				const storage_type mask = 1u << rem;
				_data[quot] ^= mask;
			}
		public:
			constexpr BitBuffer() noexcept = default;
			constexpr BitBuffer(BitBuffer&&) noexcept = default;
			constexpr BitBuffer(const BitBuffer&) noexcept = default;
			constexpr BitBuffer(std::initializer_list<bool> li) noexcept
			{
				for (std::size_t i = 0; i < li.size(); ++i)
					set(i, li.begin()[i]);
			}
			constexpr BitBuffer(std::initializer_list<Set> li) noexcept
			{
				for (std::size_t i = 0; i < li.size(); ++i)
					set(li.begin()[i].pos);
			}
			constexpr explicit BitBuffer(bool value) noexcept
			{
				fill(value);
			}

			constexpr std::size_t size() const noexcept
			{
				return Size;
			}
			constexpr std::span<const unsigned char> buffer() const noexcept
			{
				return std::span(
					reinterpret_cast<const unsigned char*>(_data.data()),
					_data.size() * sizeof(storage_type)
				);
			}
			constexpr std::span<unsigned char> buffer() noexcept
			{
				return std::span(
					reinterpret_cast<const unsigned char*>(_data.data()),
					_data.size() * sizeof(storage_type)
				);
			}

			constexpr void flip(std::size_t idx) noexcept
			{
				_flip(idx);
			}
			constexpr void set(std::size_t idx, bool value = true) noexcept
			{
				_set(idx, value);
			}
			constexpr bool test(std::size_t idx) const noexcept
			{
				return _get(idx);
			}

			constexpr void fill(bool value) noexcept
			{
				_data.fill(~0u * value);
			}
			constexpr void fill(std::size_t offset, bool value) noexcept
			{
				const std::size_t abs = offset / bitlen_;
				const std::size_t rem = offset & remcnt_;

				std::fill(_data.begin() + abs + 1, _data.end(), ~0u * value);

				if (abs < _data.size())
				{
					const storage_type mask = ~0u << rem;
					_data[abs] = (_data[abs] & ~mask) | (mask * value);
				}
			}
			constexpr void fill(std::size_t offset, std::size_t count, bool value) noexcept
			{
				const auto start = offset;
				const auto end = offset + count;

				const auto absb = start / bitlen_;
				const auto remb = start & remcnt_;
				const auto abse = end / bitlen_;
				const auto reme = end & remcnt_;

				if (absb == abse)
				{
					const storage_type mask = (~0u << remb) & (~0u >> (bitlen_ - reme - 1));
					_data[absb] = (_data[absb] & ~mask) | (mask * value);
					return;
				}

				if (remb)
				{
					const storage_type mask = ~0u << remb;
					_data[absb] = (_data[absb] & ~mask) | (mask * value);
				} else
				{
					_data[absb] = ~0u * value;
				}

				std::fill(_data.begin() + absb + 1, _data.begin() + abse, ~0u * value);

				if (reme)
				{
					const storage_type mask = ~0u >> (bitlen_ - reme);
					_data[abse] = (_data[abse] & ~mask) | (mask * value);
				}
			}

			constexpr BitBuffer& operator=(const BitBuffer&) noexcept = default;
			constexpr BitBuffer& operator=(BitBuffer&&) noexcept = default;
		};
	}

	template<std::size_t Count>
	class Bitset :
		public Interface<Bitset<Count>, cmp::concat_const_string<"bs", cmp::int_to_const_string<Count>()>()>,
		public InterfaceMake<Bitset<Count>>,
		public InterfaceHelper<Bitset<Count>>
	{
	public:
		using buffer_type = impl::BitBuffer<Count>;
		using list = std::initializer_list<typename buffer_type::Set>;
	private:
		buffer_type _bits;
	public:
		static auto mstorage(list li = {}) noexcept
		{
			return sizeof(Bitset);
		}
		static auto minline(std::span<unsigned char> view, list li = {}) noexcept
		{
			new (view.data()) Bitset(li);
			return sizeof(Bitset);
		}

		Bitset() = default;
		explicit Bitset(list li = {})
			: _bits(li)
		{ }

		struct Op : InterfaceDeclProcPrimary<
			DeclWrite<
				Tuple<Uint64, Boolean>,
				Uint64,
				Boolean,
				Tuple<Uint64, Uint64, Boolean>
			>,
			DeclFilter<Uint64>,
			DeclRead<>
		>
		{
			enum w : proc_opcode
			{
				// [ Offset, Value ]
				Set,
				// [ Value ]
				Flip,
				// [ Value ]
				Fill,
				// [ Begin, Count, Value ]
				FillRegion
			};
			enum r : proc_opcode {};
			enum f : proc_opcode
			{
				// [ Position ] -> [ Value ]
				Test
			};
		};

		key_type hash() const noexcept
		{
			return uuid::xxhash(_bits.buffer());
		}

		static constexpr std::size_t static_storage() noexcept
		{
			return sizeof(Bitset);
		}
		std::size_t storage() const noexcept
		{
			return sizeof(Bitset);
		}
		std::string print() const noexcept
		{
			std::stringstream out;
			out << "[ ";
			for (std::size_t i = 0; i < Count; i++)
				out << (_bits.test(i) ? '+' : '-');
			out << " ]";
			return out.str();
		}

		wproc_query_result wproc(proc_opcode opcode, const proc_param& arguments, wproc_query query) noexcept
		{
			if (query == wproc_query::Commit)
			{
				if (opcode == Op::Set)
				{
					const auto& idx =
						TypedView<Tuple<Uint64, Boolean>>::view(
							arguments.data()
						)->field<0>()->value();
					const auto& val =
						TypedView<Tuple<Uint64, Boolean>>::view(
							arguments.data()
						)->field<1>()->value();
					if (idx >= Count)
						return wproc_status::Error;
					_bits.set(idx, val);
				}
				else if (opcode == Op::Flip)
				{
					const auto& idx =
						TypedView<Uint64>::view(
							arguments.data()
						)->value();
					if (idx >= Count)
						return wproc_status::Error;
					_bits.flip(idx);
				}
				else if (opcode == Op::Fill)
				{
					const auto& val =
						TypedView<Boolean>::view(
							arguments.data()
						)->value();
					_bits.fill(val);
				}
				else if (opcode == Op::FillRegion)
				{
					const auto& idx =
						TypedView<Tuple<Uint64, Uint64, Boolean>>::view(
							arguments.data()
						)->field<0>()->value();
					const auto& cnt =
						TypedView<Tuple<Uint64, Uint64, Boolean>>::view(
							arguments.data()
						)->field<1>()->value();
					const auto& val =
						TypedView<Tuple<Uint64, Uint64, Boolean>>::view(
							arguments.data()
						)->field<2>()->value();
					if (idx >= Count || cnt > Count - idx)
						return wproc_status::Error;
					_bits.fill(idx, val);
				}
				return wproc_status::Ok;
			}
			return wproc_type::Static;
		}
		rproc_result rproc(proc_opcode opcode, const proc_param&) const noexcept
		{
			return nullptr;
		}
		bool fproc(proc_opcode opcode, const proc_param& arguments) const noexcept
		{
			if (opcode == Op::Test)
			{
				const auto& idx =
					TypedView<Uint64>::view(
						arguments.data()
					)->value();
				return _bits.test(idx);
			}
		}
	};
}

#endif // RDB_BITSET_HPP
