#include <rdb_shared_buffer.hpp>
#include <XXHash/xxhash.hpp>

namespace rdb
{
	namespace impl
	{
		SharedBufferData* SharedBufferData::make(std::span<const unsigned char> data, std::size_t alignment, std::size_t reserve) noexcept
		{
			auto* ptr = std::malloc(
				sizeof(SharedBufferData) +
				std::max(data.size(), reserve) +
				(alignment ? alignment - 1 : 0)
			);
			std::memcpy(
				static_cast<unsigned char*>(ptr) + sizeof(SharedBufferData),
				data.data(),
				data.size()
			);
			auto* block = static_cast<SharedBufferData*>(ptr);
			block->_size = data.size();
			block->_reserved = reserve;
			block->_alignment = alignment;
			block->_refcnt = 0;
			return block;
		}
		SharedBufferData* SharedBufferData::make(std::size_t size, std::size_t alignment, std::size_t reserve) noexcept
		{
			auto* ptr = std::malloc(
				sizeof(SharedBufferData) +
				std::max(size, reserve) +
				(alignment ? alignment - 1 : 0)
			);
			auto* block = static_cast<SharedBufferData*>(ptr);
			block->_size = size;
			block->_reserved = reserve;
			block->_alignment = alignment;
			block->_refcnt = 0;
			return block;
		}

		SharedBufferData* SharedBufferData::acquire() noexcept
		{
			_refcnt.fetch_add(1, std::memory_order::acquire);
			return this;
		}
		void SharedBufferData::release() noexcept
		{
			if (_refcnt.fetch_sub(1, std::memory_order::release) == 1)
			{
				std::free(this);
			}
		}
		void* SharedBufferData::realloc(std::size_t size, std::size_t reserve) noexcept
		{
			if (auto* ptr = std::realloc(this, sizeof(SharedBufferData) + std::max(size, reserve));
				ptr != nullptr)
			{
				return ptr;
			}
			_size = size;
			_reserved = reserve;
			return nullptr;
		}
		void* SharedBufferData::resize(std::size_t size, std::size_t reserve) noexcept
		{
			if (size <= _reserved)
			{
				_size = size;
				return nullptr;
			}
			else
			{
				return realloc(size, reserve);
			}
		}

		std::span<const unsigned char> SharedBufferData::data() const noexcept
		{
			void* ptr = const_cast<void*>(static_cast<const void*>(this + 1));
			std::size_t s = _size;
			std::align(
				_alignment, 1,
				ptr, s
			);
			return std::span(
				reinterpret_cast<const unsigned char*>(ptr), s
			);
		}
		std::span<unsigned char> SharedBufferData::data() noexcept
		{
			void* ptr = this + 1;
			std::size_t s = _size;
			std::align(
				_alignment, 1,
				ptr, s
			);
			return std::span(
				reinterpret_cast<unsigned char*>(ptr), s
			);
		}

		const void* SharedBufferData::memory() const noexcept
		{
			void* ptr = const_cast<void*>(static_cast<const void*>(this + 1));
			std::size_t s = _size;
			std::align(
				_alignment, 1,
				ptr, s
			);
			return ptr;
		}
		void* SharedBufferData::memory() noexcept
		{
			void* ptr = this + 1;
			std::size_t s = _size;
			std::align(
				_alignment, 1,
				ptr, s
			);
			return ptr;
		}
	}

	std::span<const unsigned char> SharedBuffer::data() const noexcept
	{
		return _block->data();
	}
	std::span<unsigned char> SharedBuffer::data() noexcept
	{
		return _block->data();
	}

	impl::SharedBufferData* SharedBuffer::block() const noexcept
	{
		return _block;
	}

	bool SharedBuffer::empty() const noexcept
	{
		return _block == nullptr;
	}
	std::size_t SharedBuffer::size() const noexcept
	{
		if (_block == nullptr)
			return 0;
		return _block->_size;
	}

	void SharedBuffer::resize(std::size_t size, std::size_t reserve) noexcept
	{
		if (size > _block->_size)
		{
			if (auto* ptr = _block->resize(size, reserve);
				ptr != nullptr)
			{
				_block = static_cast<impl::SharedBufferData*>(ptr);
			}
		}
	}
	void SharedBuffer::reserve(std::size_t size) noexcept
	{
		if (auto* ptr = _block->resize(this->size(), size);
			ptr != nullptr)
		{
			_block = static_cast<impl::SharedBufferData*>(ptr);
		}
	}
	void SharedBuffer::clear() noexcept
	{
		_block->release();
		_block = nullptr;
	}

	std::size_t SourceMultiplexer::fragments() const noexcept
	{
		return _input.size();
	}
	std::size_t SourceMultiplexer::size() const noexcept
	{
		return _size;
	}
	std::uint64_t SourceMultiplexer::digest() const noexcept
	{
		return _digest;
	}
	bool SourceMultiplexer::empty() const noexcept
	{
		return _size == 0;
	}

	void SourceMultiplexer::push(std::span<const unsigned char> data) noexcept
	{
		_input.push_back(data);
		_size += data.size();
	}
	void SourceMultiplexer::flush() noexcept
	{
		xxh::hash_state_t<64> state;
		_block.resize(_size);
		std::size_t idx = 0;
		for (decltype(auto) it : _input)
		{
			std::memcpy(_block.data() + idx, it.data(), it.size());
			state.update(it.begin(), it.end());
			idx += it.size();
		}
		_input.clear();
		_digest = state.digest();
	}
	void SourceMultiplexer::clear() noexcept
	{
		_block.clear();
		_size = 0;
	}
	std::span<const unsigned char> SourceMultiplexer::block() noexcept
	{
		return std::span(_block).subspan(0, _size);
	}

	const char* SourceMultiplexer::Peek(std::size_t* len)
	{
		*len = _size - _pos;
		return reinterpret_cast<const char*>(_block.data() + _pos);
	}
	void SourceMultiplexer::Skip(std::size_t cnt)
	{
		_pos += cnt;
	}
	std::size_t SourceMultiplexer::Available() const
	{
		return _size - _pos;
	}

	const char* SourceView::Peek(std::size_t* len)
	{
		*len = _data.size() - _pos;
		return reinterpret_cast<const char*>(_data.data() + _pos);
	}
	void SourceView::Skip(std::size_t cnt)
	{
		_pos += cnt;
	}
	std::size_t SourceView::Available() const
	{
		return _data.size() - _pos;
	}

	StaticBufferSink::StaticBufferSink(std::size_t size)
	{
		_buffer.reserve(size);
	}
	StaticBufferSink::StaticBufferSink(std::size_t size, std::span<unsigned char> pool)
		: _buffer_pool(pool.data(), pool.size())
	{
		_buffer.reserve(size);
	}

	void StaticBufferSink::Append(const char* data, std::size_t cnt)
	{
		_buffer.resize(_buffer.size() + cnt);
		std::memcpy(&_buffer[_pos], data, cnt);
		_pos += cnt;
	}
	char* StaticBufferSink::GetAppendBuffer(std::size_t len, char* scratch)
	{
		_buffer.resize(_pos + len);
		return &_buffer[_pos];
	}

	std::size_t StaticBufferSink::size() const noexcept
	{
		return _buffer.size();
	}
	std::span<const unsigned char> StaticBufferSink::data() const noexcept
	{
		return {
			reinterpret_cast<const unsigned char*>(_buffer.data()),
			_buffer.size()
		};
	}
	void StaticBufferSink::clear() noexcept
	{
		_buffer.clear();
		_buffer.shrink_to_fit();
	}
}
