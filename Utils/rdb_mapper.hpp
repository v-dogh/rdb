#ifndef RDB_MAPPER_HPP
#define RDB_MAPPER_HPP

#include <filesystem>
#include <span>
#ifdef __unix__
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <unistd.h>
#else
#error(platform unsupported)
#endif
#include <rdb_utils.hpp>

namespace rdb
{
	class Mapper
	{
	public:
		enum class Access
		{
			Default,
			Sequential,
			Random,
			Hot,
			Cold
		};
		enum OpenMode : unsigned char
		{
			Read = 1 << 0,
			Write = 1 << 1,
			Execute = 1 << 2,

			RW = Read | Write,
			RWE = Read | Write | Execute,
			RO = Read
		};
	private:
		void* _memory{ nullptr };
		std::size_t _length{ 0 };
		std::size_t _vmap{ false };
		std::filesystem::path _filepath{ "" };
		Access _hint{ Access::Default };
		unsigned char _open{};
		int _descriptor{ -1 };
	public:
		Mapper() noexcept = default;
		Mapper(const Mapper&) noexcept = delete;
		Mapper(Mapper&&) noexcept = default;
		~Mapper() noexcept { close(); }

		bool is_mapped() const noexcept;
		bool is_opened() const noexcept;

		std::span<const unsigned char> memory() const noexcept;
		std::span<unsigned char> memory() noexcept;

		const unsigned char* append() const noexcept;
		unsigned char* append() noexcept;

		std::size_t size() const noexcept;

		void flush(std::size_t pos, std::size_t size) noexcept;
		void flush() noexcept;

		void reserve(std::size_t size) noexcept;
		void reserve_aligned(std::size_t required) noexcept;

		void map(const std::filesystem::path& path, std::size_t length, unsigned char flags = OpenMode::RW) noexcept;
		void map(const std::filesystem::path& path, unsigned char flags = OpenMode::RW) noexcept;
		void map(std::size_t length, unsigned char flags = OpenMode::RW) noexcept;
		void map(unsigned char flags = OpenMode::RW) noexcept;
		void unmap(bool full = false) noexcept;

		void vmap(unsigned char flags = OpenMode::RW) noexcept;
		void vmap_reserve(std::size_t size) noexcept;
		void vmap_reset(std::size_t size) noexcept;
		void vmap_increment(std::size_t size) noexcept;
		void vmap_decrement(std::size_t size) noexcept;
		void vmap_flush() noexcept;

		void open(const std::filesystem::path& path, std::size_t reserve, unsigned char flags = OpenMode::RW) noexcept;
		void open(const std::filesystem::path& path, unsigned char flags = OpenMode::RW) noexcept;
		void close() noexcept;
		void remove() noexcept;

		template<std::size_t Count>
		void write(std::size_t off, const std::array<View, Count>& data) noexcept
		{
			std::array<iovec, Count> vec;
			for (std::size_t i = 0; i < Count; i++)
			{
				vec[i].iov_base = const_cast<unsigned char*>(data[i].data().data());
				vec[i].iov_len = data[i].data().size();
			}
			pwritev(_descriptor, vec.data(), vec.size(), off);
		}
		void write(std::size_t offset, std::initializer_list<View> li) noexcept;
		void write(std::size_t offset, View data) noexcept;
		void write(std::size_t offset, char ch) noexcept;
		void write(View data) noexcept;
		View read(std::size_t offset, std::size_t count) noexcept;
		unsigned char read(std::size_t off) noexcept;

		void hint(Access acc) noexcept;

		Mapper& operator=(const Mapper&) = delete;
		Mapper& operator=(Mapper&&) = default;
	};
}

#endif // RDB_MAPPER_HPP
