#ifndef RDB_TASK_RING_HPP
#define RDB_TASK_RING_HPP

#include <atomic>
#include <array>
#include <chrono>
#include <semaphore>
#include <rdb_utils.hpp>

namespace rdb::ct
{
    template<typename Type, std::size_t Size>
    class TaskRing
    {
    private:
        static_assert((Size & (Size - 1)) == 0, "TaskRing size must be power of 2");

        static constexpr auto _mask = Size - 1;

        // _tail stores the pointer to the current task that is next in line for execution
        // _head stores the pointer of the next free task slot
        // _head_check makes sure that the consumer does not grab a task that was reserved but not written to

        /* data     */                                                       std::array<Type, Size>        _ring{};
        /* consumer */ alignas(std::hardware_destructive_interference_size) std::atomic<std::size_t>      _tail{ 0 };
        /* producer */ alignas(std::hardware_destructive_interference_size) std::atomic<std::size_t>      _head{ 0 };
        /* producer */ alignas(std::hardware_destructive_interference_size) std::atomic<std::size_t>      _head_check{ 0 };
        /* notify   */ alignas(std::hardware_destructive_interference_size) std::counting_semaphore<Size> _available{ 0 };

        template<typename... Argv>
        bool _enqueue_impl(bool wait, Argv&&... args) noexcept
        {
            std::size_t head = 0;
            while (true)
            {
                head = _head.load();
                if (head - _tail >= Size)
                {
                    if (wait)
                    {
                        util::spinlock_yield();
                        continue;
                    }
                    return false;
                }
                if (_head.compare_exchange_weak(head, head + 1))
                    break;
            }
            _ring[head & _mask] = Type{ std::forward<Argv>(args)... };
            _head_check.fetch_add(1);
            _head_check.notify_one();
            _available.release();
            return true;
        }
        bool _dequeue_impl(Type& value) noexcept
        {
            std::size_t tail = _tail.load();
            if (_head_check.load() <= tail)
                return false;
            value = std::move(_ring[tail & _mask]);
            _tail.fetch_add(1);
            return true;
        }
    public:
        TaskRing() = default;
        TaskRing(const TaskRing&) = delete;
        TaskRing(TaskRing&& copy) noexcept {}

        template<typename... Argv>
        void enqueue(Argv&&... args) noexcept
        {
            _enqueue_impl(true, std::forward<Argv>(args)...);
        }
        template<typename... Argv>
        bool try_enqueue(Argv&&... args) noexcept
        {
            return _enqueue_impl(false, std::forward<Argv>(args)...);
        }

        bool try_dequeue(Type& value) noexcept
        {
            return _dequeue_impl(value);
        }
        bool dequeue(Type& value) noexcept
        {
            while (!_dequeue_impl(value))
                _head_check.wait(_head_check);
            return true;
        }
        bool dequeue(Type& value, std::chrono::microseconds max) noexcept
        {
            const auto beg = std::chrono::steady_clock::now();
            do
            {
                if (std::chrono::steady_clock::now() - beg > max ||
                        !_available.try_acquire_for(max))
                    return false;
            }
            while (!_dequeue_impl(value));
            return true;
        }

        TaskRing& operator=(const TaskRing&) = delete;
        TaskRing& operator=(TaskRing&&) noexcept { return *this; }
    };
}

#endif // RDB_TASK_RING_HPP
