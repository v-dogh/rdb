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
    enum class ReadType : char
    {
        Field,
        Schema,
        RProc,
        Page
    };
}

#endif // RDB_WRITETYPE_HPP
