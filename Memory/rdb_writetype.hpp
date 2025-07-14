#ifndef RDB_WRITETYPE_HPP
#define RDB_WRITETYPE_HPP

namespace rdb
{
	enum class WriteType : char
	{
		Reserved,
		Field,
		Table,
		WProc,
		Remov,
		Reset,
		CreatePartition
	};
}

#endif // RDB_WRITETYPE_HPP
