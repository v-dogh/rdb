#ifndef RDB_ARRAY_ITERATOR_HPP
#define RDB_ARRAY_ITERATOR_HPP

#include <type_traits>
#include <iterator>

namespace rdb::type::impl
{
	template<typename Type, bool Const, bool Trivial>
	class ArrayIterator;

	template<typename Type, bool Const>
	class ArrayIterator<Type, Const, true>
	{
	public:
		using value_type = std::conditional_t<Const, const Type, Type>;
		using pointer = value_type*;
		using reference = value_type&;
		using difference_type = std::ptrdiff_t;
		using iterator_category = std::random_access_iterator_tag;
	private:
		pointer _ptr{ nullptr };
		std::size_t _size{};

		static pointer _at(pointer ptr, int idx) noexcept
		{
			return reinterpret_cast<pointer>(
				reinterpret_cast<std::conditional_t<
					Const, const unsigned char*, unsigned char*
				>>(ptr) + idx
			);
		}
	public:
		explicit ArrayIterator(
			std::size_t idx = 0,
			std::size_t size = 0,
			pointer ptr = nullptr
		) : _ptr(_at(ptr, idx)), _size(size) {}

		reference operator*() const noexcept { return *_ptr; }
		pointer operator->() const noexcept { return _ptr; }

		reference operator[](difference_type n) const noexcept { return *_at(_ptr, n); }

		ArrayIterator& operator++() noexcept { _ptr = _at(_ptr,  1); return *this; }
		ArrayIterator& operator--() noexcept { _ptr = _at(_ptr, -1); return *this; }

		ArrayIterator operator++(int) noexcept { ArrayIterator tmp = *this; ++(*this); return tmp; }
		ArrayIterator operator--(int) noexcept { ArrayIterator tmp = *this; --(*this); return tmp; }

		ArrayIterator operator+(difference_type n) const noexcept { return ArrayIterator(_at(_ptr,  n)); }
		ArrayIterator operator-(difference_type n) const noexcept { return ArrayIterator(_at(_ptr, -n)); }

		ArrayIterator& operator+=(difference_type n) noexcept { _ptr = _at(_ptr,   n); return *this; }
		ArrayIterator& operator-=(difference_type n) noexcept { _ptr = _at(_ptr,  -n); return *this; }

		difference_type operator-(const ArrayIterator& other) const noexcept { return _ptr - other._ptr; }

		bool operator==(const ArrayIterator& other) const noexcept { return _ptr == other._ptr; }
		bool operator!=(const ArrayIterator& other) const noexcept { return _ptr != other._ptr; }
		bool operator<(const ArrayIterator& other) const noexcept { return _ptr < other._ptr; }
		bool operator<=(const ArrayIterator& other) const noexcept { return _ptr <= other._ptr; }
		bool operator>(const ArrayIterator& other) const noexcept { return _ptr > other._ptr; }
		bool operator>=(const ArrayIterator& other) const noexcept { return _ptr >= other._ptr; }
	};

	template<typename Type, bool Const>
	class ArrayIterator<Type, Const, false>
	{
	public:
		using value_type = std::conditional_t<Const, const Type, Type>;
		using pointer = value_type*;
		using reference = value_type&;
		using difference_type = std::ptrdiff_t;
		using iterator_category = std::random_access_iterator_tag;
	private:
		pointer _ptr{ nullptr };
		std::size_t _size{};
		std::size_t _idx{};
	public:
		explicit ArrayIterator(
			std::size_t idx = 0,
			std::size_t size = 0,
			pointer ptr = nullptr
		) : _ptr(ptr), _size(size), _idx(idx) {}

		reference operator*() const noexcept { return *_ptr; }
		pointer operator->() const noexcept { return _ptr; }

		reference operator[](difference_type n) const noexcept { return _ptr[n]; }

		ArrayIterator& operator++() noexcept { ++_ptr; return *this; }
		ArrayIterator& operator--() noexcept { --_ptr; return *this; }

		ArrayIterator operator++(int) noexcept { ArrayIterator tmp = *this; ++(*this); return tmp; }
		ArrayIterator operator--(int) noexcept { ArrayIterator tmp = *this; --(*this); return tmp; }

		ArrayIterator operator+(difference_type n) const noexcept { return ArrayIterator(_ptr + n); }
		ArrayIterator operator-(difference_type n) const noexcept { return ArrayIterator(_ptr - n); }

		ArrayIterator& operator+=(difference_type n) noexcept { _ptr += n; return *this; }
		ArrayIterator& operator-=(difference_type n) noexcept { _ptr -= n; return *this; }

		difference_type operator-(const ArrayIterator& other) const noexcept { return _ptr - other._ptr; }

		bool operator==(const ArrayIterator& other) const noexcept { return _ptr == other._ptr; }
		bool operator!=(const ArrayIterator& other) const noexcept { return _ptr != other._ptr; }
		bool operator<(const ArrayIterator& other) const noexcept { return _ptr < other._ptr; }
		bool operator<=(const ArrayIterator& other) const noexcept { return _ptr <= other._ptr; }
		bool operator>(const ArrayIterator& other) const noexcept { return _ptr > other._ptr; }
		bool operator>=(const ArrayIterator& other) const noexcept { return _ptr >= other._ptr; }
	};

}

#endif // RDB_ARRAY_ITERATOR_HPP
