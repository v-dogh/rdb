#include <rdb_reflect.hpp>
#include <rdb_dbg.hpp>

namespace rdb
{
	RuntimeInterfaceReflection::RTII* RuntimeInterfaceReflection::fetch(ucode_type ucode) noexcept
	{
		auto f = _interface_info.find(ucode);
		if (f == _interface_info.end())
			return nullptr;
		return &f->second;
	}
	RuntimeInterfaceReflection::RTII& RuntimeInterfaceReflection::info(ucode_type ucode) noexcept
	{
		return _interface_info.at(ucode);
	}
	RuntimeInterfaceReflection::RTII& RuntimeInterfaceReflection::reg(ucode_type ucode, RTII info) noexcept
	{
		return (_interface_info[ucode] = std::move(info));
	}

	RuntimeSchemaReflection::RTSI* RuntimeSchemaReflection::fetch(schema_type ucode) noexcept
	{
		auto f = _schema_info.find(ucode);
		if (f == _schema_info.end())
			return nullptr;
		return &f->second;
	}
	RuntimeSchemaReflection::RTSI& RuntimeSchemaReflection::info(schema_type ucode) noexcept
	{
		return _schema_info.at(ucode);
	}
	RuntimeSchemaReflection::RTSI& RuntimeSchemaReflection::reg(schema_type ucode, RTSI info) noexcept
	{
		if (fetch(ucode) != nullptr)
			RDB_WARN("Overriding already existing schema information (either conflicting names or duplicate require expression)")
		return (_schema_info[ucode] = std::move(info));
	}
}
