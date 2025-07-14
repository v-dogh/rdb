#ifndef RDB_TASK_RING_HPP
#define RDB_TASK_RING_HPP

#include <cstddef>
#include <array>
#include <atomic>
#include <rdb_utils.hpp>

namespace rdb::ct
{
	template<typename Type, std::size_t Size>
	class TaskRing
	{
	private:
		static_assert((Size & (Size - 1)) == 0, "TaskRing size must be power of 2");

		static constexpr auto _mask = Size - 1;

		// <very nice comments>
		// _tail stores the pointer to the current task that is next in line for execution
		// _head stores the pointer of the next free task slot
		// _head_check makes sure that the consumer does not grab a task that was reserved but not written to

		std::array<Type, Size> _ring{};
		/* consumer */ alignas(std::hardware_destructive_interference_size) std::atomic<std::size_t> _tail{ 0 };
		/* producer */ alignas(std::hardware_destructive_interference_size) std::atomic<std::size_t> _head{ 0 };
		/* producer */ alignas(std::hardware_destructive_interference_size) std::atomic<std::size_t> _head_check{ 0 };
	public:
		TaskRing() = default;
		TaskRing(const TaskRing&) = delete;
		TaskRing(TaskRing&& copy) noexcept {}

		template<typename... Argv>
		void enqueue(Argv&&... args) noexcept
		{
			std::size_t head;
			while (true)
			{
				head = _head.load(std::memory_order::relaxed) & _mask;
				const auto tail = _tail.load(std::memory_order::acquire) & _mask;
				if (head - tail >= Size)
				{
					util::spinlock_yield();
					continue;
				}
				if (_head.compare_exchange_weak(
						head, (head + 1) & _mask,
						std::memory_order::acq_rel,
						std::memory_order::relaxed)
					) break;
			}
			_ring[head] = Type{ std::forward<Argv>(args)... };
			_head_check.fetch_add(1, std::memory_order::acq_rel);
			_head_check.notify_one();
		}
		bool try_dequeue(Type& value) noexcept
		{
			const auto head = _head.load(std::memory_order::acquire);
			const auto tail = _tail.load(std::memory_order::relaxed);

			if (head == tail || (_head_check.load(std::memory_order::acquire) & _mask) != head)
				return false;

			value = std::move(_ring[tail]);

			_tail.store((tail + 1) & _mask, std::memory_order::release);

			return true;
		}
		bool dequeue(Type& value) noexcept
		{
			while (!try_dequeue(value))
				_head_check.wait(
					_head_check.load(std::memory_order::relaxed),
					std::memory_order::relaxed
				);
			return true;
		}

		TaskRing& operator=(const TaskRing&) = delete;
		TaskRing& operator=(TaskRing&&) noexcept { return *this; }
	};
}

#endif // RDB_TASK_RING_HPP
