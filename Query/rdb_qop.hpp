#ifndef RDB_QOP_HPP
#define RDB_QOP_HPP

namespace rdb::cmd
{
	enum class qOp : char
	{
		// Operands are specifically sequential (in values)
		// So that they fit in lookup table for the query parser

		// Operands

		Fetch,
		Create,
		Remove,
		Page,
		PageFrom,

		// Filter operands

		Check,

		// Control flow

		If,
		Atomic,
		Lock,
		Barrier,

		// Fetch operators

		Reset,
		Write,
		Read,
		WProc,
		RProc,

		// Filters

		FilterCompare,
		FilterExists,

		// Mutants

		Invert,
	};
}

#endif // RDB_QOP_HPP
