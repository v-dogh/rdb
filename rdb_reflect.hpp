#ifndef RDB_REFLECT_HPP
#define RDB_REFLECT_HPP

#include <unordered_map>
#include "rdb_keytype.hpp"
#include "rdb_utils.hpp"

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
		// Storage stays constant
		Static = 1 << 0,
		// Storage changes
		Dynamic = 1 << 1,

		Default = Static
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
		PrefixExtract = '\xFe'
	};
	enum class Order
	{
		Ascending,
		Descending
	};

	class RuntimeInterfaceReflection
	{
	public:
		struct RTII
		{
			bool(*dynamic)();
			std::size_t(*alignment)();
			std::size_t(*storage)(const void*);
			std::size_t(*sstorage)();
			wproc_query_result(*wproc)(void*, proc_opcode, proc_param, wproc_query);
			rproc_result(*rproc)(const void*, proc_opcode, proc_param);
			bool(*fproc)(const void*, proc_opcode, proc_param);
		};
	private:
		static inline std::unordered_map<std::size_t, RTII> _interface_info{};
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
			void(*construct)(void*);
			std::size_t(*cstorage)();
			std::size_t(*alignment)();
			std::size_t(*storage)(const void*);

			std::size_t(*fwapply)(void*, std::size_t, View, std::size_t);
			std::size_t(*wpapply)(void*, std::size_t, proc_opcode, proc_param, std::size_t);

			View(*cfield)(const void*, std::size_t);
			View(*field)(void*, std::size_t);
			View(*transcode)(version_type, View);

			version_type(*topology)(std::size_t);
			std::size_t(*fields)();
			std::size_t(*skeys)();
			Order(*skey_order)(std::size_t);
			RuntimeInterfaceReflection::RTII&(*reflect)(std::size_t);
			RuntimeInterfaceReflection::RTII&(*reflect_pkey)(std::size_t);
			RuntimeInterfaceReflection::RTII&(*reflect_skey)(std::size_t);
		};
	private:
		static inline std::unordered_map<schema_type, RTSI> _schema_info{};
	public:
		RuntimeSchemaReflection() = delete;

		static RTSI* fetch(schema_type version) noexcept;
		static RTSI& info(schema_type version) noexcept;
		static RTSI& reg(schema_type version, RTSI info) noexcept;
	};

	struct SortKeyComparator
	{
		schema_type schema{};
		bool operator()(const View& lhs, const View& rhs) const noexcept;
	};
	struct ThreewaySortKeyComparator
	{
		schema_type schema{};
		int operator()(const View& lhs, const View& rhs) const noexcept;
	};
	struct EqualityKeyComparator
	{
		schema_type schema{};
		std::pair<bool, std::size_t> operator()(const View& lhs, const View& rhs) const noexcept;
	};
}

#endif // RDB_REFLECT_HPP
