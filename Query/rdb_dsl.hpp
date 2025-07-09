#ifndef RDB_DSL_HPP
#define RDB_DSL_HPP

#include <memory_resource>
#include <functional>
#include <future>
#include <tuple>
#include <rdb_locale.hpp>
#include <rdb_reflect.hpp>
#include <rdb_utils.hpp>
#include <rdb_qop.hpp>

namespace rdb
{
	enum class Policy
	{
		Async = 1 << 0,
		Atomic = 1 << 1,
	};

	template<typename Schema>
	class TableListIterator
	{
	public:
		using value_type = const Schema::topology;
		using pointer = value_type::value;
		using reference = value_type&;
		using difference_type = std::ptrdiff_t;
		using iterator_category = std::forward_iterator_tag;
	private:
		pointer _data{ nullptr };
		std::size_t _idx{ 0 };
		std::size_t _len{ 0 };

		void _move() noexcept
		{
			_idx += _len;
			if (_idx < _data.size())
				_len = _data.subview(_idx)->storage();
			else
				_len = 0;
		}
		pointer _cur() const noexcept
		{
			return _data.subview(_idx, _len);
		}
	public:
		explicit TableListIterator(const pointer& data, bool end)
			: _data(View::view(data.data()))
		{
			if (end)
				_idx = data.size();
			else if (data.size())
				_len = _data->storage();
		}

		reference operator*() const noexcept { return *_cur(); }
		pointer operator->() const noexcept { return _cur(); }

		TableListIterator& operator++() noexcept { _move(); return *this; }
		TableListIterator operator++(int) noexcept { TableListIterator tmp = *this; ++(*this); return tmp; }

		bool operator==(const TableListIterator& other) const noexcept { return _idx == other._idx; }
		bool operator!=(const TableListIterator& other) const noexcept { return _idx != other._idx; }
		bool operator<(const TableListIterator& other) const noexcept { return _idx < other._idx; }
		bool operator<=(const TableListIterator& other) const noexcept { return _idx <= other._idx; }
		bool operator>(const TableListIterator& other) const noexcept { return _idx > other._idx; }
		bool operator>=(const TableListIterator& other) const noexcept { return _idx >= other._idx; }
	};

	template<typename Schema>
	class TableList
	{
	public:
		using iterator = TableListIterator<Schema>;
	private:
		Schema::value _data{ nullptr };
	public:
		void push(std::span<const unsigned char> data) noexcept
		{
			_data = Schema::value::copy(data);
		}

		auto begin() const noexcept
		{
			return iterator(_data, false);
		}
		auto end() const noexcept
		{
			return iterator(_data, true);
		}

		bool empty() const noexcept
		{
			return _data.empty();
		}
	};

	namespace cmd
	{
		struct EvalTrait {};
		struct PredicateTrait {};
		struct FetchTrait {};
		struct ExecuteTrait {};
		struct OperationChainTrait {};
		struct ControlFlowTrait {};
		struct FallbackTrait {};

		struct ExtractNothing
		{
			constexpr auto extract() const noexcept
			{
				return nullptr;
			}
		};

		template<qOp Op>
		struct Operand
		{
			static constexpr auto op = Op;
		};

		template<qOp Op, typename... Traits>
		struct Command : ExtractNothing, Operand<Op>, Traits...
		{
			template<typename>
			using instantiate = Command;

			constexpr auto size() const noexcept
			{
				return 0;
			}
			constexpr auto fill(std::span<unsigned char>) const noexcept
			{
				return 0;
			}
		};
	}
	namespace cmd
	{
		template<typename... Ops>
		struct OperationChain :
			OperationChainTrait,
			std::conditional_t<std::is_base_of_v<ControlFlowTrait, std::remove_cvref_t<std::tuple_element_t<0, std::tuple<Ops...>>>>, ControlFlowTrait, FallbackTrait>
		{
			template<typename... Op> using append_ops = OperationChain<Ops..., Op...>;
			template<typename... Op> using pre_append_ops = OperationChain<Op..., Ops...>;
			template<typename Chain> using combine_chains = Chain::template pre_append_ops<Ops...>;

			std::tuple<Ops...> ops;

			constexpr OperationChain(Ops... ops) noexcept : ops(std::move(ops)...) {}
			constexpr OperationChain(std::tuple<Ops...> ops) noexcept : ops(std::move(ops)) {}

			constexpr auto extract() noexcept
			{
				auto tup = [&]<std::size_t... Idx>(std::index_sequence<Idx...>) {
					return std::tuple_cat(
						[&]() {
							if constexpr (std::is_base_of_v<EvalTrait, Ops>)
								return std::tuple(std::move(std::get<Idx>(ops)));
							else
								return std::tuple<>();
						}()...
					);
				}(std::make_index_sequence<sizeof...(Ops)>());

				using sub = std::remove_cvref_t<decltype(tup)>;
				if constexpr (std::is_same_v<sub, std::tuple<>>)
				{
					return nullptr;
				}
				else
				{
					return [ops = std::move(tup)](std::size_t op, std::span<const unsigned char> buffer) {
						static std::array table = []<std::size_t... Idx>(std::index_sequence<Idx...>) {
							return std::array{
								+[](const sub& ops, std::span<const unsigned char> buffer) {
									return std::get<Idx>(ops).eval(buffer);
								}...
							};
						}(std::make_index_sequence<std::tuple_size_v<sub>>());
						return table[op](ops, buffer);
					};
				}
			}
			constexpr auto size() noexcept
			{
				return std::apply([](const auto&... ops) {
					return
						(ops.size() + ...) +
						sizeof...(Ops) +
						((sizeof(std::uint32_t) * std::is_base_of_v<ControlFlowTrait, std::remove_cvref_t<Ops>>) + ...);
				}, ops);
			}
			constexpr void fill(std::span<unsigned char> buffer) noexcept
			{
				std::apply([&](auto&... ops) {
					std::size_t idx = 0;
					([&]<typename Type>(Type& op) {
						buffer[idx++] = char(Type::op);
						if constexpr (std::is_base_of_v<ControlFlowTrait, Type>)
							idx += byte::swrite<std::uint32_t>(buffer, idx, size() - idx - sizeof(std::uint32_t));
						idx += op.fill(buffer.subspan(idx));
					}(ops), ...);
				}, ops);
			}
		};
	}
	namespace cmd
	{
		template<typename Type>
		struct is_typed_view : std::false_type {};

		template<typename Type>
		struct is_typed_view<TypedView<Type>> : std::true_type {};

		using compound_key = std::pair<StackView<64>, StackView<64>>;

		// Operands

		template<typename First, typename... Ops, typename Op> requires std::is_base_of_v<FetchTrait, Op>
		constexpr auto operator|(OperationChain<First, Ops...> chain, Op next) noexcept
		{
			using type = typename Op::template instantiate<typename First::schema>;
			return OperationChain<First, Ops..., type>(
				std::tuple_cat(
					std::move(chain.ops),
					std::forward_as_tuple(type(std::move(next)))
				)
			);
		}

		template<typename Schema>
		struct Fetch : Operand<qOp::Fetch>
		{
			using schema = Schema;

			compound_key key{};

			template<typename Op> requires std::is_base_of_v<FetchTrait, Op>
			constexpr auto operator|(Op op) const noexcept
			{
				using type = typename Op::template instantiate<Schema>;
				return cmd::OperationChain<Fetch<Schema>, type>(
					std::move(*this), type(std::move(op))
				);
			}
			constexpr auto size() const noexcept
			{
				return
					sizeof(schema_type) +
					key.first.size() +
					key.second.size() + (sizeof(std::uint32_t) * !key.second.empty());
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.first.data());
				if (!key.second.empty())
				{
					off += byte::swrite<std::uint32_t>(buffer, off, key.second.size());
					off += byte::swrite(buffer, off, key.second.data());
				}
				return off;
			}
		};

		template<typename Schema>
		struct Page : EvalTrait, Operand<qOp::Page>
		{
			Schema::partition::value key{};
			TableList<Schema>* out{ nullptr };
			std::size_t count{ 0 };

			constexpr auto extract()
			{
				return [out = out](std::size_t op, std::span<const unsigned char> buffer) {
					out->push(buffer);
				};
			}
			constexpr auto size() const noexcept
			{
				return
					sizeof(schema_type) +
					key.size() +
					sizeof(std::uint32_t);
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.data());
				off += byte::swrite(buffer, off, count);
				return off;
			}
			constexpr auto eval(std::span<const unsigned char> buffer) const noexcept
			{
				out->push(buffer);
				return 1;
			}
		};

		template<typename Schema>
		struct PageFrom : EvalTrait, Operand<qOp::PageFrom>
		{
			compound_key key{};
			TableList<Schema>* out{ nullptr };
			std::size_t count{ 0 };

			constexpr auto extract()
			{
				return [out = out](std::size_t op, std::span<const unsigned char> buffer) {
					out->push(buffer);
				};
			}
			constexpr auto size() const noexcept
			{
				return
					sizeof(schema_type) +
					key.first.size() +
					key.second.size() +
					sizeof(std::uint32_t);
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.first.data());
				if (!key.second.empty())
				{
					off += byte::swrite<std::uint32_t>(buffer, off, key.second.size());
					off += byte::swrite(buffer, off, key.second.data());
				}
				off += byte::swrite(buffer, off, count);
				return off;
			}
			constexpr auto eval(std::span<const unsigned char> buffer) const noexcept
			{
				out->push(buffer);
				return 1;
			}
		};

		template<typename Schema>
		struct Create : ExtractNothing, Operand<qOp::Create>
		{
			Schema::partition::value key{};
			Schema::data::value data{};

			constexpr auto size() const noexcept
			{
				return
					sizeof(schema_type) +
					key.size() +
					data.size();
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.data());
				off += byte::swrite(buffer, off, data.data());
				return off;
			}
		};

		template<typename Schema>
		struct Remove : ExtractNothing, Operand<qOp::Remove>
		{
			compound_key key{};

			constexpr auto size() const noexcept
			{
				return
					sizeof(schema_type) +
					key.first.size() +
					key.second.size() + (sizeof(std::uint32_t) * !key.second.empty());
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.first.data());
				if (!key.second.empty())
				{
					off += byte::swrite<std::uint32_t>(buffer, off, key.second.size());
					off += byte::swrite(buffer, off, key.second.data());
				}
				return off;
			}
		};

		template<typename Schema>
		struct Check : EvalTrait, Operand<qOp::Check>
		{
			std::function<void(bool)> callback{};
			compound_key key{};

			template<typename Op> requires std::is_base_of_v<PredicateTrait, Op>
			constexpr auto operator<(Op op) const noexcept
			{
				return cmd::OperationChain<Check<Schema>, Op>(std::move(*this), op);
			}
			constexpr auto size() const noexcept
			{
				return
					sizeof(schema_type) +
					key.first.size() +
					key.second.size() + (sizeof(std::uint32_t) * !key.second.empty());;
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.first.data());
				if (!key.second.empty())
				{
					off += byte::swrite<std::uint32_t>(buffer, off, key.second.size());
					off += byte::swrite(buffer, off, key.second.data());
				}
				return off;
			}
			constexpr auto eval(std::span<const unsigned char> buffer) const noexcept
			{
				callback(buffer[0]);
				return 1;
			}
		};

		// Conditionals

		using If = Command<qOp::If, ControlFlowTrait>;

		// Mutants

		template<typename Op>
		struct Invert : Op/*, Operand<qOp::Invert>*/
		{
			static constexpr auto op = qOp::Invert;

			constexpr auto size() const noexcept
			{
				return Op::size() + sizeof(qOp);
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				buffer[0] = char(Op::op);
				return Op::fill(buffer.subspan(1)) + 1;
			}
		};

		// Operators

		using Reset = Command<qOp::Reset, FetchTrait>;

		template<typename Schema, cmp::ConstString Field, typename... Argv>
		struct WriteImpl;

		template<cmp::ConstString Field, typename... Argv>
		struct Write : FetchTrait
		{
			template<typename Schema>
			using instantiate = WriteImpl<Schema, Field, Argv...>;

			std::tuple<Argv...> data;
		};

		template<typename Schema, cmp::ConstString Field, typename... Argv>
		struct WriteImpl : Write<Field, Argv...>, Operand<qOp::Write>
		{
			static constexpr auto field = *Field;

			constexpr auto size() const noexcept
			{
				static_assert(Schema::data::template has<Field>, "Cannot write to a key field");
				return
					sizeof(std::uint8_t) +
					sizeof(std::uint32_t) +
					std::apply([](auto&&... args) {
						return Schema::
							template interface<Field>::mstorage(args...);
					}, this->data);
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<std::uint32_t>(buffer, off,
					std::apply([](auto&&... args) {
						return Schema::
							template interface<Field>::mstorage(args...);
					}, this->data)
				);
				buffer[off++] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				off +=
					std::apply([&](auto&&... args) {
						return Schema::template interface<Field>::minline(
							buffer.subspan(off),
							std::forward<Argv>(args)...
						);
					}, this->data);
				return off;
			}
		};

		template<typename Schema, cmp::ConstString Field, typename Type>
		struct WriteViewImpl;

		template<cmp::ConstString Field, typename Type>
		struct WriteView : FetchTrait
		{
			template<typename Schema>
			using instantiate = WriteViewImpl<Schema, Field, Type>;

			Type data;
		};

		template<typename Schema, cmp::ConstString Field, typename Type>
		struct WriteViewImpl : WriteView<Field, Type>, Operand<qOp::Write>
		{
			static constexpr auto field = *Field;

			constexpr auto size() const noexcept
			{
				static_assert(Schema::data::template has<Field>, "Cannot write to a key field");
				return
					sizeof(std::uint8_t) +
					sizeof(std::uint32_t) +
					this->data.size();
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				buffer[off++] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				off += byte::swrite<std::uint32_t>(buffer, off, this->data.size());
				off += byte::swrite(buffer, off, this->data);
				return off;
			}
		};

		template<typename Schema, cmp::ConstString Field, char Op, typename... Argv>
		struct WProcImpl;

		template<cmp::ConstString Field, char Op, typename... Argv>
		struct WProc : FetchTrait
		{
			template<typename Schema>
			using instantiate = WProcImpl<Schema, Field, Op, Argv...>;

			std::tuple<Argv...> data;
		};

		template<typename Schema, cmp::ConstString Field, char Op, typename... Argv>
		struct WProcImpl : WProc<Field, Op, Argv...>, Operand<qOp::WProc>
		{
			using type = Schema::template interface<Field>;
			using wtype = type::Op::template wtype<proc_opcode(Op)>;

			static constexpr auto field = *Field;

			constexpr auto size() const noexcept
			{
				static_assert(Schema::data::template has<Field>, "Cannot write to a key field");
				return
					sizeof(std::uint8_t) +
					sizeof(proc_opcode) +
					sizeof(std::uint32_t) +
					std::apply([](const auto&... args) {
						return wtype::mstorage(args...);
					}, this->data);
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				std::size_t off = 0;
				off += byte::swrite<std::uint32_t>(buffer, off,
					std::apply([&](const auto&... args) {
						return wtype::mstorage(args...);
					}, this->data)
				);
				buffer[off++] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				buffer[off++] = static_cast<unsigned char>(Op);
				std::apply([&](auto&&... args) {
					off += wtype::minline(
						buffer.subspan(off), std::forward<Argv>(args)...
					);
				}, this->data);
				return off;
			}
		};

		template<typename Schema, cmp::ConstString Field, char Op, typename Type>
		struct WProcViewImpl;

		template<cmp::ConstString Field, char Op, typename Type>
		struct WProcView : FetchTrait
		{
			template<typename Schema>
			using instantiate = WProcViewImpl<Schema, Field, Op, Type>;

			Type data;
		};

		template<typename Schema, cmp::ConstString Field, char Op, typename Type>
		struct WProcViewImpl : WProcView<Field, Op, Type>, Operand<qOp::WProc>
		{
			static constexpr auto field = *Field;

			constexpr auto size() const noexcept
			{
				static_assert(Schema::data::template has<Field>, "Cannot write to a key field");
				return
					sizeof(qOp) +
					sizeof(std::uint8_t) +
					sizeof(char) +
					sizeof(std::uint32_t) +
					this->data.size();
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				buffer[0] = char(qOp::WProc);
				std::size_t off = sizeof(qOp);
				off += byte::swrite<std::uint32_t>(buffer, off, this->data.size());
				buffer[off++] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				buffer[off++] = static_cast<unsigned char>(Op);
				using type = Schema::template interface<Field>;
				off += byte::swrite(buffer, off, this->data);
				return off;
			}
		};

		template<typename Schema, cmp::ConstString Field>
		struct ReadImpl;

		template<cmp::ConstString Field>
		struct Read : FetchTrait, EvalTrait
		{
			template<typename Schema>
			using instantiate = ReadImpl<Schema, Field>;

			std::function<void(View)> callback;
		};

		template<typename Schema, cmp::ConstString Field>
		struct ReadImpl : Read<Field>, Operand<qOp::Read>
		{
			static constexpr auto field = *Field;

			constexpr auto size() const noexcept
			{
				return sizeof(std::uint8_t);
			}
			constexpr auto fill(std::span<unsigned char> buffer) noexcept
			{
				buffer[0] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				return 1;
			}
			constexpr auto eval(std::span<const unsigned char> buffer) const noexcept
			{
				RuntimeSchemaReflection::RTSI& info =
					RuntimeSchemaReflection::info(Schema::ucode);
				const auto off = info.reflect(
					Schema::template index_of<Field>()
				).storage(buffer.data());
				this->callback(buffer.empty() ? nullptr : View::view(buffer.subspan(0, off)));
				return off;
			}
		};

		// Filters

		using Exists = Command<qOp::FilterExists, PredicateTrait>;

		// Commands

		template<unsigned char... Flags>
		struct Execute : ExecuteTrait
		{
			static constexpr auto flags = (Flags | ... | 0x00);
			bool* status{ nullptr };

			void resolve(bool value) const noexcept
			{
				if (status)
					*status = value;
			}
		};

		struct Flush {};

		using Barrier = Command<qOp::Barrier>;
	}
	namespace cmd
	{
		template<typename Schema, typename... Argv>
		constexpr cmd::compound_key keyset(Argv&&... keys) noexcept
		{
			static_assert(
				sizeof...(Argv) ==
				(Schema::partition::fields + Schema::data::sort_count),
				"Invalid argument for partition and sorting keys"
			);
			std::tuple args = std::forward_as_tuple(std::forward<Argv>(keys)...);
			return {
				[&args]<std::size_t... Idv>(std::index_sequence<Idv...>)
				{
					// return std::tuple_element_t<1, cmd::compound_key>::combine_views(
					// 	[&]() {
					// 		if constexpr (
					// 			cmd::is_typed_view<
					// 				std::decay_t<
					// 					std::tuple_element_t<Idv, std::decay_t<decltype(args)>>
					// 				>
					// 			>::value
					// 		)
					// 		{
					// 			return std::get<Idv>(args);
					// 		}
					// 		else
					// 		{
					// 			return Schema::partition::make(
					// 				std::get<Idv>(args)
					// 			);
					// 		}
					// 	}()...
					// );
					return Schema::partition::make(
						std::get<Idv>(args)...
					);
				}(std::make_index_sequence<Schema::partition::fields>()),
				[&args]<std::size_t... Idv>(std::index_sequence<Idv...>)
				{
					if constexpr (sizeof...(Idv) == 0)
					{
						return cmd::compound_key::second_type{};
					}
					else
					{
						cmd::compound_key::second_type view =
							cmd::compound_key::second_type::copy(
								([&]() {
									if constexpr (
										cmd::is_typed_view<
											std::decay_t<
												std::tuple_element_t<Idv, std::decay_t<decltype(args)>>
											>
										>::value
									)
									{
										return std::get<Schema::partition::fields + Idv>(args).prefix_length();
									}
									else
									{
										using field = Schema::template field_type<
											Schema::data::template sort<Idv>
										>;
										using interface = field::interface;
										return interface::mpstorage(
											field::order,
											std::get<Schema::partition::fields + Idv>(args)
										);
									}
								}() + ...)
							);
						std::size_t off = 0;
						([&]() {
							using field = Schema::template field_type<
								Schema::data::template sort<Idv>
							>;
							using interface = field::interface;
							if constexpr (
								cmd::is_typed_view<
									std::decay_t<
										std::tuple_element_t<Idv, std::decay_t<decltype(args)>>
									>
								>::value
							)
							{
								off += std::get<Schema::partition::fields + Idv>(
									args
								)->prefix(view.subview(off), field::order);
							}
							else
							{
								off += interface::mpinline(
									view.mutate().subspan(off),
									field::order,
									std::get<Schema::partition::fields + Idv>(args)
								);
							}
						}(), ...);
						return view;
					}
				}(std::make_index_sequence<Schema::data::sort_count>())
			};
		}

		template<typename Operand>
		constexpr auto size(Operand&& chain) noexcept
		{
			return chain.size();
		}
		template<typename Operand>
		constexpr void fill(Operand&& chain, std::span<unsigned char> buffer) noexcept
		{
			chain.fill(buffer);
		}
	}

	//
	// Aliases
	//

	// Commands

	constexpr auto flush = cmd::Flush();

	template<auto... Opts>
	constexpr auto execute = cmd::Execute<static_cast<unsigned char>(Opts)...>();

	constexpr auto barrier = cmd::Barrier();

	template<auto... Opts>
	constexpr auto execute_checked(bool* out = nullptr) noexcept
	{
		return cmd::Execute<static_cast<unsigned char>(Opts)...>(out);
	}

	// Operands

	template<typename Schema, typename... Argv>
	constexpr auto remove(Argv&&... keys) noexcept
	{
		return cmd::Remove<Schema>{
			.key = cmd::keyset<Schema>(std::forward<Argv>(keys)...)
		};
	}

	template<typename Schema, typename... Argv>
	constexpr auto fetch(Argv&&... keys) noexcept
	{
		return cmd::Fetch<Schema>{
			.key = cmd::keyset<Schema>(std::forward<Argv>(keys)...)
		};
	}

	template<typename Schema, typename... Argv>
	constexpr auto page(TableList<Schema>* out, std::size_t count, Argv&&... args) noexcept
	{
		return cmd::Page<Schema>{
			.key = Schema::partition::make(std::forward<Argv>(args)...),
			.out = out,
			.count = count
		};
	}

	template<typename Schema, typename... Argv>
	constexpr auto page_from(TableList<Schema>* out, std::size_t count, Argv&&... keys) noexcept
	{
		return cmd::PageFrom<Schema>{
			.key = cmd::keyset<Schema>(std::forward<Argv>(keys)...),
			.out = out,
			.count = count
		};
	}

	template<typename Schema, typename... Argv>
	constexpr auto create(Argv&&... args) noexcept
	{
		std::tuple targs = std::forward_as_tuple(std::forward<Argv>(args)...);
		return cmd::Create<Schema>{
			.key = [&targs]<std::size_t... Idv>(std::index_sequence<Idv...>)
			{
				// return std::tuple_element_t<1, cmd::compound_key>::combine_views(
				// 	[&]() {
				// 		if constexpr (
				// 			cmd::is_typed_view<
				// 				std::decay_t<
				// 					std::tuple_element_t<Idv, std::decay_t<decltype(targs)>>
				// 				>
				// 			>::value
				// 		)
				// 		{
				// 			return std::get<Idv>(targs);
				// 		}
				// 		else
				// 		{
				// 			return Schema::partition::make(
				// 				std::get<Idv>(targs)...
				// 			);
				// 		}
				// 	}()...
				// );
				return Schema::partition::make(
					std::get<Idv>(targs)...
				);
			}(std::make_index_sequence<Schema::partition::fields>()),
			.data = [&targs]<std::size_t... Idv>(std::index_sequence<Idv...>)
			{
				return Schema::make(
					std::get<Schema::partition::fields + Idv>(targs)...
				);
			}(std::make_index_sequence<Schema::data::fields>())
		};
	}

	// Operators

	constexpr auto reset = cmd::Reset();

	template<cmp::ConstString Field, typename... Argv>
	constexpr auto write(Argv&&... args) noexcept
	{
		if constexpr (sizeof...(Argv) == 1 && (cmd::is_typed_view<Argv>::value && ...))
		{
			return cmd::WriteView<Field, Argv...>{
				.data = std::forward<Argv>(args)...
			};
		}
		else
		{
			return cmd::Write<Field, Argv...>{
				.data = std::forward_as_tuple(std::forward<Argv>(args)...)
			};
		}
	}

	template<cmp::ConstString Field, char Op, typename... Argv>
	constexpr auto wproc(Argv&&... args)
	{
		if constexpr (sizeof...(Argv) == 1 && (cmd::is_typed_view<Argv>::value && ...))
		{
			return cmd::WProcView<Field, Op, Argv...>{
				.data = std::forward<Argv>(args)...
			};
		}
		else
		{
			return cmd::WProc<Field, Op, Argv...>{
				.data = std::forward_as_tuple(std::forward<Argv>(args)...)
			};
		}
	}

	template<cmp::ConstString Field, typename Value>
	constexpr auto read(Value* ptr) noexcept
	{
		auto func = [ptr](View view) {
			*ptr = Value::copy(view.data());
		};
		return cmd::Read<Field>{ .callback = std::move(func) };
	}

	template<cmp::ConstString Field, typename Func>
	constexpr auto read(Func&& func) noexcept
	{
		return cmd::Read<Field>{ .callback = [func = std::forward<Func>(func)](View view) mutable {
			func(view);
		}};
	}

	// Filters

	template<typename Schema, typename... Argv>
	constexpr auto check(bool* out, Argv&&... keys) noexcept
	{
		auto func = [out](bool value) {
			*out = value;
		};
		return cmd::Check<Schema>{
			.callback = func,
			.key = cmd::keyset<Schema>(std::forward<Argv>(keys)...)
		};
	}

	template<typename Schema, typename Func, typename... Argv>
	constexpr auto check(Func&& func, Argv&&... keys) noexcept
	{
		return cmd::Check<Schema>{
			.callback = std::forward<Func>(func),
			.key = cmd::keyset<Schema>(std::forward<Argv>(keys)...)
		};
	}

	// Predicates

	constexpr auto exists = cmd::Exists();

	// Control flow

	template<typename Cond, typename Schema, typename... Expr>
	constexpr auto pred(cmd::OperationChain<cmd::Check<Schema>, Cond> condition, Expr... ops) noexcept
	{
		return cmd::OperationChain(
				std::tuple_cat(
					std::forward_as_tuple(cmd::If()),
					std::move(condition.ops),
					[&]() {
						if constexpr (std::is_base_of_v<cmd::OperationChainTrait, Expr>)
							return std::move(ops.ops);
						else
							return std::forward_as_tuple(std::move(ops));
					}()...
				)
			);
	}

	// Mutants

	template<typename Op>
	constexpr auto invert(Op op) noexcept
	{
		return cmd::Invert<Op>(std::move(op));
	}

	//
	//
	//

	template<typename Cmd>
	constexpr auto compose(Cmd cmd) noexcept
	{
		return cmd;
	}

	template<typename Base>
	class QueryEngine
	{
	public:
		enum OperandFlags : char
		{
			Reads = 1 << 0,
		};
		struct ReadChainStore
		{
			using ptr = std::unique_ptr<ReadChainStore>;
			using func_type = std::function<void(std::size_t, std::span<const unsigned char>)>;
			std::aligned_storage_t<16, alignof(func_type)> handler_pool;
			std::pmr::monotonic_buffer_resource handler_resource{ &handler_pool, sizeof(handler_pool) };
			std::pmr::vector<func_type> handlers{ &handler_resource };
		};
	private:
		static void _sync(std::future<bool> fut = {}) noexcept
		{
			thread_local std::future<bool> task{};
			if (fut.valid())
				task = std::move(fut);
			else if (task.valid())
				task.wait();
		}
		static std::span<unsigned char> _qbuffer(std::size_t require) noexcept
		{
			thread_local std::unique_ptr<unsigned char[]> dynamic_buffer{ nullptr };
			thread_local unsigned char static_buffer[1024]{};
			thread_local unsigned char* buffer = static_buffer;
			thread_local std::size_t ptr = 0;
			thread_local std::size_t capacity = 1024;
			if (require == 0)
			{
				ptr = 0;
				return std::span<unsigned char>();
			}
			else if (require != ~0ull)
			{
				const auto s = std::span(buffer + ptr, require);
				ptr += require;
				[[ unlikely ]] if (ptr > capacity)
				{
					capacity *= 2;
					dynamic_buffer.reset(new unsigned char[capacity]);
					buffer = dynamic_buffer.get();
				}
				return s;
			}
			else
			{
				return std::span(buffer, ptr);
			}
		}
		static std::unique_ptr<ReadChainStore> _build_store(ReadChainStore::func_type func) noexcept
		{
			thread_local std::unique_ptr<ReadChainStore> store{};
			if (store == nullptr)
				store = std::make_unique<ReadChainStore>();

			if (func == nullptr)
			{
				std::unique_ptr<ReadChainStore> v;
				store.swap(v);
				return v;
			}
			else
			{
				store->handlers.push_back(std::move(func));
				return nullptr;
			}
		}
	public:
		template<typename Type>
		decltype(auto) operator<<(Type&& cmd) noexcept
		{
			if constexpr (std::is_same_v<std::remove_cvref_t<Type>, cmd::Flush>)
			{
				_sync();
				_qbuffer(0);
				return *this;
			}
			else if constexpr (std::is_base_of_v<cmd::ExecuteTrait, std::remove_cvref_t<Type>>)
			{
				constexpr auto flags = std::remove_cvref_t<Type>::flags;
				if constexpr (flags & unsigned(Policy::Async))
				{
					// auto f = static_cast<Base*>(this)->query_async(
					// 	_qbuffer(~0ull), _build_store(nullptr)
					// );
					// _qbuffer(0);
					// _sync(f);
					// return f;
				}
				else
				{
					if constexpr (flags & unsigned (Policy::Atomic))
					{
						const auto qid = static_cast<Base*>(this)->_log_query(_qbuffer(~0ull));
						const auto result = static_cast<Base*>(this)->query_sync(
							_qbuffer(~0ull), _build_store(nullptr)
						);
						_qbuffer(0);
						if (result)
							static_cast<Base*>(this)->_resolve_query(qid);
						cmd.resolve(result);
					}
					else
					{
						cmd.resolve(static_cast<Base*>(this)->query_sync(
							_qbuffer(~0ull), _build_store(nullptr)
						));
						_qbuffer(0);
					}
					return *this;
				}
			}
			else
			{
				using type = std::remove_cvref_t<Type>;

				constexpr auto skip_op = std::is_base_of_v<cmd::OperationChainTrait, type>;
				constexpr auto skip_size = !std::is_base_of_v<cmd::ControlFlowTrait, type> || skip_op;

				// Writes

				_sync();
				const auto base_size = cmd::size(cmd);
				const auto size = base_size +
					sizeof(OperandFlags) +
					(sizeof(cmd::qOp) * !skip_op) +
					(sizeof(std::uint32_t) * !skip_size);
				const auto buffer = _qbuffer(size);

				// Metadata

				std::size_t off = 0;

				auto r = cmd.extract();
				if constexpr (!std::is_same_v<std::remove_cvref_t<decltype(r)>, std::nullptr_t>)
				{
					buffer[off++] = OperandFlags::Reads;
					_build_store(std::move(r));
				}
				else
				{
					buffer[off++] = 0x00;
				}
				if constexpr (!skip_op)
				{
					buffer[off++] = char(type::op);
				}
				if constexpr (!skip_size)
				{
					off += byte::swrite<std::uint32_t>(buffer, off, base_size);
				}

				// Instructions

				cmd::fill(cmd,
					std::span(
						buffer.begin() + off,
						buffer.end()
					)
				);

				return *this;
			}
		}
	};
}

#endif // RDB_DSL_HPP
