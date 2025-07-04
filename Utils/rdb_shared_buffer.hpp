#ifndef RDB_SHARED_BUFFER_HPP
#define RDB_SHARED_BUFFER_HPP

#include <memory_resource>
#include <cstring>
#include <span>
#include <rdb_memunits.hpp>
#include <rdb_containers.hpp>
#include <Snappy/snappy.h>
#include <Snappy/snappy-sinksource.h>

namespace rdb
{
	class BlockSourceMultiplexer : public snappy::Source
	{
	public:
		struct Node
		{
			std::span<const unsigned char> key;
			std::span<const unsigned char> data;
		};
	private:
		std::pmr::monotonic_buffer_resource _pool{};
		std::pmr::monotonic_buffer_resource _block_pool{};
		std::pmr::vector<Node> _input{ &_pool };
		std::pmr::vector<unsigned char> _block{ &_block_pool };
		std::size_t _size{ 0 };
		std::size_t _pos{ 0 };
		std::uint64_t _digest{};
	public:
		BlockSourceMultiplexer(std::span<unsigned char> block, std::span<Node> fragments) :
			_pool(fragments.data(), fragments.size() * sizeof(fragments[0])),
			_block_pool(block.data(), block.size())
		{
			_input.reserve(fragments.size());
		}
		BlockSourceMultiplexer(const BlockSourceMultiplexer&) = delete;
		BlockSourceMultiplexer(BlockSourceMultiplexer&&) = delete;
		virtual ~BlockSourceMultiplexer() = default;

		std::size_t fragments() const noexcept;
		std::size_t size() const noexcept;
		std::uint64_t digest() const noexcept;
		bool empty() const noexcept;

		void push(Node node) noexcept;
		void flush() noexcept;
		void clear() noexcept;
		std::span<const unsigned char> block() noexcept;

		virtual const char* Peek(std::size_t* len) override;
		virtual void Skip(std::size_t cnt) override;
		virtual std::size_t Available() const override;
	};
	class SourceView : public snappy::Source
	{
	private:
		std::size_t _pos{ 0 };
		std::span<const unsigned char> _data{};
	public:
		SourceView() = default;
		SourceView(std::span<const unsigned char> data) : _data(data) {}
		virtual ~SourceView() = default;

		virtual const char* Peek(std::size_t* len) override;
		virtual void Skip(std::size_t cnt) override;
		virtual std::size_t Available() const override;
	};
	class StaticBufferSink : public snappy::Sink
	{
	private:
		std::pmr::monotonic_buffer_resource _buffer_pool{};
		std::pmr::vector<char> _buffer{ &_buffer_pool };
		std::size_t _pos{ 0 };
	public:
		StaticBufferSink() = default;
		StaticBufferSink(std::size_t, std::span<unsigned char> pool);
		StaticBufferSink(std::size_t size);
		StaticBufferSink(const StaticBufferSink&) = delete;
		StaticBufferSink(StaticBufferSink&&) = delete;
		virtual ~StaticBufferSink() = default;

		virtual void Append(const char* data, std::size_t cnt) override;
		virtual char* GetAppendBuffer(std::size_t len, char* scratch) override;

		std::size_t size() const noexcept;
		std::span<const unsigned char> data() const noexcept;
		void clear() noexcept;

		StaticBufferSink& operator=(const StaticBufferSink&) = delete;
		StaticBufferSink& operator=(StaticBufferSink&&) = delete;
	};
}

#endif // RDB_SHARED_BUFFER_HPP
