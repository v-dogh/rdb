#ifndef RDB_DISK_CACHE_HPP
#define RDB_DISK_CACHE_HPP

namespace rdb
{
    // Base class for a <literal> cache implementation
    // It will be accessed by the MemoryCache during queries
    // To speed up frequently accessed disk keys
    class DiskCache
    {
    private:
    public:
    };

    namespace dc
    {
        class LeastFrequentlyUsed : public DiskCache
        {

        };
        class LeastRecentlyUsed : public DiskCache
        {

        };
        class AdaptiveLayeredCache : public DiskCache
        {

        };
    }
}

#endif // RDB_DISK_CACHE_HPP
