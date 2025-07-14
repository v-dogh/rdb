#ifndef RDB_REFLECT_HPP
#define RDB_REFLECT_HPP

#include <rdb_containers.hpp>
#include <rdb_utils.hpp>
#include <rdb_keytype.hpp>

namespace rdb
{
	using proc_param = View;
	using proc_opcode = char;

	using rproc_result = View;
	using wproc_query_result = unsigned long long;
	enum class wproc_query
	{
		// Returns wproc characteristics (wproc_type)
		Type,
		// Returns the new storage size
		Storage,
		// Performs the actual procedure
		Commit
	};
	enum wproc_type : wproc_query_result
	{
		//
		Reserved,
		// Storage stays constant
		Static,
		// Storage changes
		Dynamic,
		// Writes only a delta
		Delta,
	};
	enum wproc_status : wproc_query_result
	{
		// There occured an unrecoverable error during writing
		Error,
		// No issues
		Ok,
	};

	enum class SortFilterOp : proc_opcode
	{
		Equal = '\xF0',
		Smaller = '\xF1',
		Larger = '\xF2'
	};
	enum class SortReadOp : proc_opcode
	{
		PrefixSize = '\xF0',
		PrefixExtract = '\xFe',
		PrefixCompare = '\xFc'
	};
	enum class Order
	{
		Ascending,
		Descending
	};

	class AccumulatorHandle
	{
	public:
		enum class Type
		{
			Delta,
			Root
		};
	private:
		void* _state{ nullptr };
		View(*_consume)(void*, View, Type){ nullptr };
		void(*_destroy_state)(void*){ nullptr };
	public:
		template<typename State>
		static AccumulatorHandle make() noexcept
		{
			AccumulatorHandle handle;
			if constexpr (!std::is_default_constructible_v<State>)
			{
				if constexpr (!std::is_void_v<State>)
				{
					handle._consume = [](void* ptr, View view, Type type) -> View {
						return State::consume(View::view(view), type);
					};
				}
			}
			else
			{
				handle._consume = [](void* ptr, View view, Type type) -> View {
					return static_cast<State*>(ptr)->consume(View::view(view), type);
				};
				handle._state = new State;
				handle._destroy_state = [](void* ptr) {
					delete static_cast<State*>(ptr);
				};
			}
			return handle;
		}

		AccumulatorHandle() = default;
		~AccumulatorHandle()
		{
			if (_state)
				_destroy_state(_state);
		}

		View consume(View data, Type type) noexcept
		{
			return _consume(_state, View::view(data), type);
		}
	};
	class CompressorHandle
	{
	private:
		void* _state{ nullptr };
		View(*_compress)(void*, View){ nullptr };
		void(*_consume_for_compression)(void*, View){ nullptr };
		void(*_destroy_state)(void*){ nullptr };
	public:
		template<typename State>
		static CompressorHandle make() noexcept
		{
			CompressorHandle handle;
			if constexpr (!std::is_default_constructible_v<State>)
			{
				if constexpr (!std::is_void_v<State>)
				{
					handle._consume_for_compression = [](void* ptr, View view) {
						State::consume(View::view(view));
					};
					handle._compress = [](void* ptr, View view) -> View {
						return State::compress(View::view(view));
					};
				}
			}
			else
			{
				handle._consume_for_compression = [](void* ptr, View view) {
					static_cast<State*>(ptr)->consume(View::view(view));
				};
				handle._compress = [](void* ptr, View view) -> View {
					return static_cast<State*>(ptr)->compress(View::view(view));
				};
				handle._state = new State;
				handle._destroy_state = [](void* ptr) {
					delete static_cast<State*>(ptr);
				};
			}
			return handle;
		}

		CompressorHandle() = default;
		~CompressorHandle()
		{
			if (_state)
				_destroy_state(_state);
		}

		void consume(View data) noexcept
		{
			_consume_for_compression(_state, View::view(data));
		}
		View compress(View data) noexcept
		{
			return _compress(_state, View::view(data));
		}
	};

	struct FieldWriteApplyState
	{
		std::size_t size{ 0 };
		std::size_t capacity{ ~0ull };

		std::size_t internal_field_offset{ ~0ull };
		std::size_t internal_field_size{ 0 };
	};
	struct WriteProcApplyState
	{
		std::size_t size{ 0 };
		std::size_t capacity{ ~0ull };

		std::size_t internal_field_offset{ ~0ull };
		std::size_t internal_field_size{ 0 };
		std::size_t internal_wproc_required_size{ 0 };
		wproc_type internal_wproc_type{ wproc_type::Reserved };
	};

	class RuntimeInterfaceReflection
	{
	public:
		struct RTII
		{
			bool(*dynamic)();
			std::size_t(*storage)(const void*);
			std::size_t(*sstorage)();
			key_type(*hash)(const void*);
			wproc_query_result(*wproc)(void*, proc_opcode, const proc_param&, wproc_query);
			rproc_result(*rproc)(const void*, proc_opcode, const proc_param&);
			bool(*fproc)(const void*, proc_opcode, const proc_param&);
			bool(*fragmented)();
			AccumulatorHandle(*accumulate)(){ nullptr };
			CompressorHandle(*compress)(){ nullptr };
		};
	private:
		static inline ct::hash_map<std::size_t, RTII> _interface_info{};
	public:
		RuntimeInterfaceReflection() = delete;

		static RTII* fetch(ucode_type ucode) noexcept;
		static RTII& info(ucode_type ucode) noexcept;
		static RTII& reg(ucode_type ucode, RTII info) noexcept;
	};
	class RuntimeSchemaReflection
	{
	public:
		struct RTSI
		{
			void(*construct)(void*, const View&);
			std::size_t(*cstorage)(const View&);
			std::size_t(*storage)(const void*);

			std::size_t(*fwapply)(void*, std::size_t, const View&, FieldWriteApplyState&);
			std::size_t(*wpapply)(void*, std::size_t, proc_opcode, const proc_param&, WriteProcApplyState&);

			View(*cfield)(const void*, std::size_t);
			View(*field)(void*, std::size_t);
			View(*skfield)(const void*, std::size_t);
			View(*transcode)(version_type, const View&);

			key_type(*hash_partition)(const void*);
			std::size_t(*partition_size)(const void*);

			version_type(*topology)(std::size_t);
			std::size_t(*fields)();
			std::size_t(*skeys)();
			Order(*skey_order)(std::size_t);
			RuntimeInterfaceReflection::RTII&(*reflect)(std::size_t);
			RuntimeInterfaceReflection::RTII&(*reflect_pkey)(std::size_t);
			RuntimeInterfaceReflection::RTII&(*reflect_skey)(std::size_t);

			std::string(*print_data)(const void*);
			std::string(*print_partition_data)(const void*);
			std::string(*show_topology)();
			std::string(*show_parition_topology)();

			bool(*static_prefix)();
			std::size_t(*sprefix_length)();
			std::size_t(*prefix_length)(const void*);
			std::size_t(*prefix)(const void*, View);
		};
	private:
		static inline std::atomic_flag _lock{};
		static inline ct::hash_map<schema_type, RTSI> _schema_info{};
		static inline std::size_t _version{ 0 };
	public:
		RuntimeSchemaReflection() = delete;

		static RTSI* fetch(schema_type version) noexcept;
		static RTSI& info(schema_type version) noexcept;
		static RTSI& reg(schema_type version, RTSI info) noexcept;

		static bool stale(std::size_t id) noexcept;
		static std::pair<std::size_t, RTSI*> version(schema_type version) noexcept;
	};
}

#endif // RDB_REFLECT_HPP
