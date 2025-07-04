#ifndef RDB_QOP_HPP
#define RDB_QOP_HPP

namespace rdb::cmd
{
	enum class qOp : char
	{
		// Operands

		Fetch,
		Create,
		Reset,
		Remove,
		Write,
		Read,
		Dump,
		WProc,
		RProc,
		Page,
		PageFrom,

		// Filter operands

		Check,
		Conditional,

		// Filters

		FilterCompare,
		FilterExists
	};
}

#endif // RDB_QOP_HPP
