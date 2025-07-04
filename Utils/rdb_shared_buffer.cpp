#include <rdb_shared_buffer.hpp>
#include <XXHash/xxhash.hpp>

namespace rdb
{
	std::size_t BlockSourceMultiplexer::fragments() const noexcept
	{
		return _input.size();
	}
	std::size_t BlockSourceMultiplexer::size() const noexcept
	{
		return _size;
	}
	std::uint64_t BlockSourceMultiplexer::digest() const noexcept
	{
		return _digest;
	}
	bool BlockSourceMultiplexer::empty() const noexcept
	{
		return _size == 0;
	}

	void BlockSourceMultiplexer::push(Node node) noexcept
	{
		_input.push_back(node);
		_size +=
			(!node.key.empty() * sizeof(std::uint32_t)) +
			node.key.size() + node.data.size();
	}
	void BlockSourceMultiplexer::flush() noexcept
	{
		xxh::hash_state_t<64> state;
		_block.resize(_size);
		std::size_t idx = 0;
		for (decltype(auto) it : _input)
		{			
			_block[idx++] = it.data[0];
			if (!it.key.empty())
			{
				state.update(it.key.begin(), it.key.end());
				const std::uint32_t len = it.key.size();
				std::memcpy(_block.data() + idx, &len, sizeof(len));
				idx += sizeof(len);
				std::memcpy(_block.data() + idx, it.key.data(), it.key.size());
				idx += it.key.size();
			}
			state.update(it.data.begin(), it.data.end());
			std::memcpy(_block.data() + idx, it.data.data() + 1, it.data.size() - 1);
			idx += it.data.size() - 1;
		}
		_input.clear();
		_digest = state.digest();
	}
	void BlockSourceMultiplexer::clear() noexcept
	{
		_block.clear();
		_size = 0;
		_pos = 0;
	}
	std::span<const unsigned char> BlockSourceMultiplexer::block() noexcept
	{
		return std::span(_block).subspan(0, _size);
	}

	const char* BlockSourceMultiplexer::Peek(std::size_t* len)
	{
		*len = _size - _pos;
		return reinterpret_cast<const char*>(_block.data() + _pos);
	}
	void BlockSourceMultiplexer::Skip(std::size_t cnt)
	{
		_pos += cnt;
	}
	std::size_t BlockSourceMultiplexer::Available() const
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

	void StaticBufferSink::Append(const char* data, std::size_t n)
	{
		if (data == _buffer.data() + _buffer.size())
			_buffer.resize(_buffer.size() + n);
		else
			_buffer.insert(_buffer.end(), data, data + n);
	}
	char* StaticBufferSink::GetAppendBuffer(std::size_t len, char* scratch)
	{
		if (len <= _buffer.capacity() - _buffer.size())
		{
			_buffer.reserve(_buffer.size() + len);
			return _buffer.data() + _buffer.size();
		}
		return scratch;
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
