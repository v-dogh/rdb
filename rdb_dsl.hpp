#ifndef RDB_DSL_HPP
#define RDB_DSL_HPP

// RDB Streaming Language
// Operands
// -may be passed to operators with the pipe operator '|'
// -allow for batched operations
// <remove>
// <fetch>
// <remove>
// <if>
// <filter>
// Operators
// -perform the actual logic
// -implicitly asynchronous
// *Fetch
// <write>
// <read>
// <dump>
// <reset>
// <wproc>
// <rproc>
// *Filter
// <is>
// <not>
// <and>
// <or>
// Commands
// -signal the interpreter
// <atomic>
// <execute>
/*
	<rdb::compose is purely syntactic sugar since '<<' has precedence over '|'>
	auto barrier = db.query
		<< rdb::compose(rdb::fetch<Schema>("userA")
			| rdb::write<"FieldA">(0)
			| rdb::write<"FieldB">("value")
			| rdb::read<"FieldC">([](const auto& value) {
				...
			  }))
		<< rdb::compose(rdb::fetch<Schema>("userB")
			| rdb::if<
				rdb::is<"FieldA", rdb::null>,
				rdb::write<"FieldA">(666)
			>
		<< rdb::compose(rdb::fetch<Schema>("userB")
			| rdb::write<"FieldD">("meow")
			| rdb::read<"FieldQ">(&out))
		<< rdb::execute<rdb::Policy::Async>
	(...)
	barrier.sync(); <if policy is Sync the query returns itself>
*/

#include <memory_resource>
#include <functional>
#include <future>
#include <tuple>
#include "rdb_locale.hpp"
#include "rdb_reflect.hpp"
#include "rdb_utils.hpp"
#include "rdb_qop.hpp"

namespace rdb
{
	enum class Policy
	{
		Sync,
		Async
	};

	namespace cmd
	{
		struct EvalTrait {};
	}
	namespace impl
	{
		template<typename... Ops>
		struct OperationChain
		{
			std::tuple<Ops...> ops;
			constexpr OperationChain(Ops... ops) noexcept : ops(std::move(ops)...) {}
			constexpr OperationChain(std::tuple<Ops...> ops) noexcept : ops(std::move(ops)) {}
			constexpr auto extract() noexcept
			{
				auto tup = [&]<std::size_t... Idx>(std::index_sequence<Idx...>) {
					return std::tuple_cat(
						[&]<std::size_t Index, typename Op>() {
							if constexpr (std::is_base_of_v<cmd::EvalTrait, Op>)
								return std::tuple(std::move(std::get<Index>(ops)));
							else
								return std::tuple<>();
						}.template operator()<Idx, Ops>()...
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
								[](const sub& ops, std::span<const unsigned char> buffer) {
									std::get<Idx>(ops).template eval<typename std::tuple_element_t<0, std::tuple<Ops...>>::schema>(buffer);
								}...
							};
						}(std::make_index_sequence<std::tuple_size_v<sub>>());
						return table[op](ops, buffer);
					};
				}
			}
		};

		template<typename... Ops, typename Op>
		constexpr auto operator|(OperationChain<Ops...> chain, Op next) noexcept
		{
			return OperationChain<Ops..., Op>(
				std::tuple_cat(std::move(chain.ops),
				std::make_tuple(std::move(next)))
			);
		}
	}
	namespace cmd
	{
		using compound_key = std::pair<key_type, StackView<128>>;

		template<typename Schema>
		struct Fetch
		{
			using schema = Schema;
			compound_key key{};

			template<typename Op>
			constexpr auto operator|(Op op) const noexcept
			{
				return impl::OperationChain<Fetch<Schema>, Op>(std::move(*this), op);
			}
			template<typename>
			constexpr auto size() const noexcept
			{
				return
					sizeof(qOp) +
					sizeof(schema_type) +
					sizeof(key_type) +
					key.second.size();
			}
			template<typename>
			constexpr auto fill(std::span<unsigned char> buffer) const noexcept
			{
				buffer[0] = char(qOp::Fetch);
				std::size_t off = sizeof(qOp);
				off += byte::swrite<key_type>(buffer, off, key.first);
				off += byte::swrite<schema_type>(buffer, off, Schema::ucode);
				off += byte::swrite(buffer, off, key.second.data());
				return off;
			}
		};

		template<typename Schema>
		struct Remove
		{
			using schema = Schema;
			compound_key key{};
		};

		struct Reset
		{
			template<typename>
			constexpr auto size() const noexcept
			{
				return sizeof(qOp);
			}
			template<typename>
			constexpr auto fill(std::span<unsigned char> buffer) const noexcept
			{
				buffer[0] = char(qOp::Reset);
				return sizeof(qOp);
			}
		};

		template<cmp::ConstString Field, typename Type>
		struct Write
		{
			static constexpr auto field = *Field;
			Type data{};

			template<typename Schema>
			constexpr auto size() const noexcept
			{
				return
					sizeof(qOp) +
					sizeof(std::uint8_t) +
					sizeof(std::uint32_t) +
					Schema::template interface<Field>::mstorage(data);
			}
			template<typename Schema>
			constexpr auto fill(std::span<unsigned char> buffer) const noexcept
			{
				buffer[0] = char(qOp::Write);
				buffer[1] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				std::size_t off = sizeof(qOp) + sizeof(std::uint8_t);
				off += byte::swrite<std::uint32_t>(buffer, off,
					Schema::template interface<Field>::mstorage(data)
				);
				off += byte::swrite(buffer, off,
					Schema::template interface<Field>::make(
						std::move(data)
					)->view()
				);
				return off;
			}
		};

		template<cmp::ConstString Field, typename Type>
		struct Write<Field, TypedView<Type>>
		{
			static constexpr auto field = *Field;
			TypedView<Type> data{};

			template<typename Schema>
			constexpr auto size() const noexcept
			{
				return
					sizeof(qOp) +
					sizeof(std::uint8_t) +
					sizeof(std::uint32_t) +
					data.size();
			}
			template<typename Schema>
			constexpr auto fill(std::span<unsigned char> buffer) const noexcept
			{
				buffer[0] = char(qOp::Write);
				buffer[1] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				std::size_t off = sizeof(qOp) + sizeof(std::uint8_t);
				off += byte::swrite<std::uint32_t>(buffer, off, data.size());
				off += byte::swrite(buffer, off, data->view());
				return off;
			}
		};

		template<cmp::ConstString Field, char Op, typename Type>
		struct WProc
		{
			static constexpr auto field = *Field;
			Type data{};

			template<typename Schema>
			constexpr auto size() const noexcept
			{
				return
					sizeof(qOp) +
					sizeof(std::uint8_t) +
					sizeof(char) +
					sizeof(std::uint32_t) +
					Schema::template interface<Field>::mstorage(data);
			}
			template<typename Scheme>
			constexpr auto fill(std::span<unsigned char> buffer) const noexcept
			{
				buffer[0] = char(qOp::WProc);
				buffer[1] = static_cast<std::uint8_t>(Scheme::template index_of<Field>());
				buffer[2] = static_cast<unsigned char>(Op);
				std::size_t off = sizeof(qOp) + sizeof(std::uint8_t) + sizeof(proc_opcode);
				off += byte::swrite<std::uint32_t>(buffer, off,
					Scheme::template interface<Field>::mstorage(data)
				);
				using type = Scheme::template interface<Field>;
				off += byte::swrite(buffer, off, type::template WritePair<typename type::wOp(Op)>::param::make(
					std::move(data), buffer.subspan(off)
				)->view());
				return off;
			}
		};

		template<cmp::ConstString Field>
		struct Read : EvalTrait
		{
			static constexpr auto field = *Field;
			std::function<void(View)> callback;

			template<typename Schema>
			constexpr auto size() const noexcept
			{
				return
					sizeof(qOp) +
					sizeof(std::uint8_t);
			}
			template<typename Schema>
			constexpr auto fill(std::span<unsigned char> buffer) const noexcept
			{
				buffer[0] = char(qOp::Read);
				buffer[1] = static_cast<std::uint8_t>(Schema::template index_of<Field>());
				return size<Schema>();
			}
			template<typename Schema>
			constexpr auto eval(std::span<const unsigned char> buffer) const noexcept
			{
				RuntimeSchemaReflection::RTSI& info =
					RuntimeSchemaReflection::info(Schema::ucode);
				const auto off = info.reflect(
					Schema::template index_of<Field>()
				).storage(buffer.data());
				callback(buffer.empty() ? nullptr : View::view(buffer.subspan(0, off)));
				return off;
			}
		};

		template<cmp::ConstString Field, typename Type>
		struct Dump : EvalTrait
		{
			static constexpr auto field = *Field;
			Type func{};
		};

		template<Policy Ex>
		struct Execute {};

		template<bool Is>
		struct Atomic {};

		struct Flush {};
	}
	namespace impl
	{
		template<typename Schema, typename... Argv>
		constexpr cmd::compound_key keyset(Argv&&... keys) noexcept
		{
			std::tuple args = std::forward_as_tuple(std::forward<Argv>(keys)...);
			return {
				[&args]
					<std::size_t... Idv>(std::index_sequence<Idv...>)
				{
					return uuid::xxhash_combine({
						Schema::template interface<
							Schema::keyset::template partition<Idv>
						>::make(
							std::get<Idv>(args)
						)->hash()...
					});
				}(std::make_index_sequence<Schema::keyset::partition_count>()),
				[&args]
					<std::size_t... Idv>(std::index_sequence<Idv...>)
				{
					if constexpr (sizeof...(Idv) == 0)
					{
						return cmd::compound_key::second_type{};
					}
					else
					{
						cmd::compound_key::second_type view =
							cmd::compound_key::second_type::copy(
								(Schema::template interface<
									Schema::keyset::template sort<Idv>
								>::mstorage(
									std::get<Schema::keyset::partition_count + Idv>(args)
								) + ...)
							);
						std::size_t off = 0;
						([&]<std::size_t Idx>() mutable {
							auto value = (Schema::template interface<
								Schema::keyset::template sort<Idv>
							>::make(
								std::get<Schema::keyset::partition_count + Idx>(args))
							);
							off += byte::swrite(view.mutate(), off,
								value->view()
							);
						}.template operator()<Idv>(), ...);
						return view;
					}
				}(std::make_index_sequence<Schema::keyset::sort_count>()),
			};
		}
		template<typename Schema, typename... Ops>
		constexpr auto size(OperationChain<cmd::Fetch<Schema>, Ops...>& chain) noexcept
		{
			return std::apply([](const auto&... ops) {
				return (ops.template size<Schema>() + ...);
			}, chain.ops);
		}
		template<typename Schema, typename... Ops>
		constexpr void fill(const OperationChain<cmd::Fetch<Schema>, Ops...>& chain, std::span<unsigned char> buffer) noexcept
		{
			std::apply([&](auto&... ops) {
				std::size_t idx = 0;
				([&](auto& op) {
					idx += op.template fill<Schema>(buffer.subspan(idx));
				}(ops), ...);
			}, chain.ops);
		}
	}

	//
	// Aliases
	//

	constexpr auto flush = cmd::Flush();

	template<Policy Ex = Policy::Sync>
	constexpr auto execute = cmd::Execute<Ex>();

	template<bool Is>
	constexpr auto atomic = cmd::Atomic<Is>();

	template<typename Schema, typename... Argv>
	constexpr auto remove(Argv&&... keys) noexcept
	{
		return cmd::Remove<Schema>(
			impl::keyset<Schema>(std::forward<Argv>(keys)...)
		);
	}

	template<typename Schema, typename... Argv>
	constexpr auto fetch(Argv&&... keys) noexcept
	{
		return cmd::Fetch<Schema>(
			impl::keyset<Schema>(std::forward<Argv>(keys)...)
		);
	}

	constexpr auto reset = cmd::Reset();

	template<cmp::ConstString Field, typename Value>
	constexpr auto write(Value&& value) noexcept
	{
		return cmd::Write<Field, std::remove_cvref_t<Value>>(std::forward<Value>(value));
	}

	template<cmp::ConstString Field, char Op, typename Arg>
	constexpr auto wproc(Arg&& arg)
	{
		return cmd::WProc<Field, Op, std::remove_cvref_t<Arg>>(std::forward<Arg>(arg));
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

	template<cmp::ConstString Field, typename Value>
	constexpr auto dump(Value* ptr) noexcept
	{
		auto func = [ptr](const auto& value) {
			*ptr = value;
		};
		using type = std::remove_cvref_t<decltype(func)>;
		return cmd::Dump<Field, type>(std::move(func));
	}

	template<cmp::ConstString Field, typename Func>
	constexpr auto dump(Func&& func) noexcept
	{
		return cmd::Dump<Field, Func>(std::forward<Func>(func));
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
		static void _sync(std::future<void> fut = {}) noexcept
		{
			thread_local std::future<void> task{};
			if (fut.valid())
				task = std::move(fut);
			else if (task.valid())
				task.wait();
		}
		static std::pmr::vector<unsigned char>& _qbuffer(bool reset = false) noexcept
		{
			thread_local std::array<unsigned char, 1024> pool;
			thread_local std::pmr::monotonic_buffer_resource resource(pool.data(), pool.size());
			thread_local std::pmr::vector<unsigned char> out(&resource);
			if (reset)
			{
				out.clear();
				resource.release();
			}
			return out;
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
				_qbuffer(true);
				return *this;
			}
			else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, cmd::Execute<Policy::Sync>>)
			{
				static_cast<Base*>(this)->query_sync(_qbuffer(), _build_store(nullptr));
				_qbuffer(true);
				return *this;
			}
			else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, cmd::Execute<Policy::Async>>)
			{
				auto f = static_cast<Base*>(this)->query_async(_qbuffer(), _build_store(nullptr));
				_qbuffer(true);
				_sync(f);
				return f;
			}
			else
			{
				thread_local auto off = 0;

				// Writes

				_sync();
				if (_qbuffer().empty()) off = 0;
				const auto size = impl::size(cmd) + 1;
				_qbuffer().resize(_qbuffer().size() + size);

				// Reads

				auto r = cmd.extract();
				if constexpr (!std::is_same_v<std::remove_cvref_t<decltype(r)>, std::nullptr_t>)
				{
					_qbuffer()[off] = OperandFlags::Reads;
					_build_store(std::move(r));
				}
				else
				{
					_qbuffer()[off] = 0x00;
				}

				// Instructions

				impl::fill(cmd,
					std::span(
						_qbuffer().begin() + off + 1,
						_qbuffer().end()
					)
				);
				off += size;

				return *this;
			}
		}
	};
}

#endif // RDB_DSL_HPP
