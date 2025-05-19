#include <rdb_reflect.hpp>

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
		return (_schema_info[ucode] = std::move(info));
	}

	bool SortKeyComparator::operator()(const View& lhs, const View& rhs) const noexcept
	{
		const auto* ldata = lhs.data().data();
		const auto* rdata = rhs.data().data();
		RuntimeSchemaReflection::RTSI& info
			= RuntimeSchemaReflection::info(schema);
		std::size_t off1 = 0;
		std::size_t off2 = 0;
		for (std::size_t i = 0; i < info.skeys(); i++)
		{
			RuntimeInterfaceReflection::RTII& key =
				info.reflect_skey(i);
			Order ordering = info.skey_order(i);
			if (key.fproc(
					ldata + off1, proc_opcode(SortFilterOp::Smaller),
					View::view(std::span(rdata + off2, std::dynamic_extent)))
				)
				return ordering == Order::Ascending;
			else if (key.fproc(
					ldata + off1, proc_opcode(SortFilterOp::Larger),
					View::view(std::span(rdata + off2, std::dynamic_extent)))
				)
				return ordering != Order::Ascending;
			off1 += key.storage(ldata + off1);
			off2 += key.storage(rdata + off2);
		}
		return false;
	}
	int ThreewaySortKeyComparator::operator()(const View& lhs, const View& rhs) const noexcept
	{
		const auto* ldata = lhs.data().data();
		const auto* rdata = rhs.data().data();
		RuntimeSchemaReflection::RTSI& info
			= RuntimeSchemaReflection::info(schema);
		std::size_t off1 = 0;
		std::size_t off2 = 0;
		for (std::size_t i = 0; i < info.skeys(); i++)
		{
			RuntimeInterfaceReflection::RTII& key =
				info.reflect_skey(i);
			if (key.fproc(
					ldata + off1, proc_opcode(SortFilterOp::Smaller),
					View::view(std::span(rdata + off2, std::dynamic_extent)))
				)
				return -1;
			else if (key.fproc(
					ldata + off1, proc_opcode(SortFilterOp::Larger),
					View::view(std::span(rdata + off2, std::dynamic_extent)))
				)
				return 1;
			off1 += key.storage(ldata + off1);
			off2 += key.storage(rdata + off2);
		}
		return 0;
	}
	std::pair<bool, std::size_t> EqualityKeyComparator::operator()(const View& lhs, const View& rhs) const noexcept
	{
		const auto* ldata = lhs.data().data();
		const auto* rdata = rhs.data().data();
		RuntimeSchemaReflection::RTSI& info
			= RuntimeSchemaReflection::info(schema);
		std::size_t off1 = 0;
		std::size_t off2 = 0;
		bool match = true;
		for (std::size_t i = 0; i < info.skeys(); i++)
		{
			RuntimeInterfaceReflection::RTII& key =
				info.reflect_skey(i);
			match = match && key.fproc(
				ldata + off1, proc_opcode(SortFilterOp::Equal),
				View::view(std::span(rdata + off2, std::dynamic_extent))
			);
			off1 += key.storage(ldata + off1);
			off2 += key.storage(rdata + off2);
		}
		return { match, off2 };
	}
}
