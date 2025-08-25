#include <rdb_root_config.hpp>

namespace rdb
{
    EventStore::Handle::Handle(wptr store, std::size_t id, void(*rel)(ptr, std::size_t)) :
        _store(store),
        _id(id),
        _release(rel)
    {}
    EventStore::Handle::~Handle()
    {
        release();
    }

    void EventStore::Handle::release()
    {
        if (const auto ptr = _store.lock(); ptr)
        {
            _release(ptr, _id);
            _store = wptr();
        }
    }
    void EventStore::Handle::drop()
    {
        _store = wptr();
    }
}
