#ifndef RDB_WRITETYPE_HPP
#define RDB_WRITETYPE_HPP

namespace rdb
{
	enum class WriteType : char
	{
		Field = 'f',
		Table = 'r',
		WProc = 'w',
		RProc = 'r',
		Remov = 'x',
		Reset = 'n',
		CreatePartition = 'P',
		Reserved = '\0'
	};
	enum class ReadType : char
	{
		Field = 'f',
		Table = 'r',
	};
}

#endif // RDB_WRITETYPE_HPP
