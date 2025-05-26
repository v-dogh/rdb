#ifndef RDB_QOP_HPP
#define RDB_QOP_HPP

namespace rdb::cmd
{
	enum class qOp : char
	{
		Fetch = 'f',
		Reset = 'R',
		Rewrite = 'N',
		Remove = 'P',
		Write = 'w',
		Read = 'r',
		Dump = 'd',
		Compare = '=',
		Conditional = '?',
		WProc = '+',
		RProc = '-'
	};
}

#endif // RDB_QOP_HPP
