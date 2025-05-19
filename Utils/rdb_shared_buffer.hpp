#ifndef RDB_SHARED_BUFFER_HPP
#define RDB_SHARED_BUFFER_HPP

#include <memory_resource>
#include <cstring>
#include <atomic>
#include <span>
#include <Snappy/snappy.h>
#include <Snappy/snappy-sinksource.h>
#include <rdb_memunits.hpp>

namespace rdb
{
	namespace impl
	{
		struct SharedBufferData
		{
			static SharedBufferData* make(std::span<const unsigned char> data, std::size_t alignment = 0, std::size_t reserve = 0) noexcept;
			static SharedBufferData* make(std::size_t size, std::size_t alignment = 0, std::size_t reserve = 0) noexcept;

			std::atomic<std::uint32_t> _refcnt{ 0 };
			std::uint32_t _size{ 0 };
			std::uint32_t _reserved{ 0 };
			std::uint32_t _alignment{ 0 };

			SharedBufferData* acquire() noexcept;
			void release() noexcept;
			void* realloc(std::size_t size, std::size_t reserve = 0) noexcept;
			void* resize(std::size_t size, std::size_t reserve = 0) noexcept;

			std::span<const unsigned char> data() const noexcept;
			std::span<unsigned char> data() noexcept;

			const void* memory() const noexcept;
			void* memory() noexcept;
		};
	}

	class SharedBuffer
	{
	protected:
		impl::SharedBufferData* _block{ nullptr };
	public:
		SharedBuffer() noexcept = default;
		SharedBuffer(std::nullptr_t) noexcept {}
		SharedBuffer(std::span<const unsigned char> data, std::size_t alignment, std::size_t reserve = 0) noexcept
			: _block(impl::SharedBufferData::make(data, alignment, reserve))
		{ _block->acquire(); }
		SharedBuffer(std::size_t size, std::size_t alignment = 0, std::size_t reserve = 0) noexcept
			: _block(impl::SharedBufferData::make(size, alignment, reserve))
		{ _block->acquire(); }
		SharedBuffer(const SharedBuffer& copy) noexcept
			: _block(copy._block->acquire())
		{}
		SharedBuffer(SharedBuffer&& copy) noexcept
			: _block(copy._block)
		{ copy._block = nullptr; }
		~SharedBuffer()
		{
			if (_block) _block->release();
		}

		std::span<const unsigned char> data() const noexcept;
		std::span<unsigned char> data() noexcept;

		impl::SharedBufferData* block() const noexcept;

		std::size_t size() const noexcept;

		void resize(std::size_t size, std::size_t reserve = 0) noexcept;
		void reserve(std::size_t size) noexcept;
		void clear() noexcept;

		SharedBuffer& operator=(const SharedBuffer& copy) noexcept
		{
			_block = copy._block->acquire();
			return *this;
		}
		SharedBuffer& operator=(SharedBuffer&& copy) noexcept
		{
			_block = copy._block;
			copy._block = nullptr;
			return *this;
		}

		bool operator==(std::nullptr_t) const noexcept
		{
			return _block == nullptr;
		}
		bool operator!=(std::nullptr_t) const noexcept
		{
			return _block != nullptr;
		}
	};

	class SourceMultiplexer : public snappy::Source
	{
	private:
		std::pmr::monotonic_buffer_resource _pool{};
		std::pmr::monotonic_buffer_resource _block_pool{};
		std::pmr::vector<std::span<const unsigned char>> _input{ &_pool };
		std::pmr::vector<unsigned char> _block{ &_block_pool };
		std::size_t _size{ 0 };
		std::size_t _pos{ 0 };
		std::uint64_t _digest{};
	public:
		SourceMultiplexer(std::span<unsigned char> block, std::span<std::span<const unsigned char>> fragments) :
			_pool(fragments.data(), fragments.size() * sizeof(fragments[0])),
			_block_pool(block.data(), block.size())
		{
			_input.reserve(fragments.size());
		}
		SourceMultiplexer(const SourceMultiplexer&) = delete;
		SourceMultiplexer(SourceMultiplexer&&) = delete;
		virtual ~SourceMultiplexer() = default;

		std::size_t fragments() const noexcept;
		std::size_t size() const noexcept;
		std::uint64_t digest() const noexcept;

		void push(std::span<const unsigned char> data) noexcept;
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
	class SharedBufferSink :
		public SharedBuffer,
		public snappy::Sink
	{
	private:
		std::size_t _pos{ 0 };
	public:
		using SharedBuffer::SharedBuffer;
		virtual ~SharedBufferSink() = default;

		virtual void Append(const char* data, std::size_t cnt) override;
		virtual char* GetAppendBuffer(std::size_t len, char* scratch) override;

		using SharedBuffer::operator=;
	};
}

#endif // RDB_SHARED_BUFFER_HPP
