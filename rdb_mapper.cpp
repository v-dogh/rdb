#include "rdb_mapper.hpp"
#include "rdb_memunits.hpp"
#include <numeric>

namespace rdb
{
	bool Mapper::is_mapped() const noexcept
	{
		return _descriptor != -1;
	}
	bool Mapper::is_opened() const noexcept
	{
		return _descriptor != -1;
	}

	std::size_t Mapper::size() const noexcept
	{
		return _length;
	}

	std::span<const unsigned char> Mapper::memory() const noexcept
	{
		return std::span(
			static_cast<const unsigned char*>(_memory),
			_length
		);
	}
	std::span<unsigned char> Mapper::memory() noexcept
	{
		return std::span(
			static_cast<unsigned char*>(_memory),
			_length
		);
	}

	const unsigned char* Mapper::append() const noexcept
	{
		return static_cast<const unsigned char*>(_memory) + _length;
	}
	unsigned char* Mapper::append() noexcept
	{
		return static_cast<unsigned char*>(_memory) + _length;
	}

	void Mapper::flush(std::size_t pos, std::size_t size) noexcept
	{
#		ifdef __unix__
			if (_memory != nullptr && !_vmap)
			{
				msync(
					static_cast<char*>(_memory) + pos,
					size,
					MS_SYNC
				);
			}
			else
			{
				fsync(_descriptor);
			}
#		endif
	}
	void Mapper::flush() noexcept
	{
		flush(0, _length);
	}

	void Mapper::reserve(std::size_t size) noexcept
	{
		bool remap = false;
		if (_memory != nullptr && !_vmap)
		{
			unmap();
			remap = true;
		}
#		ifdef __unix__
			const auto fd = _descriptor == -1 ? ::open(_filepath.c_str(), O_RDWR | O_CREAT, 0666) : _descriptor;
			if (fd == -1)
				return;
			ftruncate(fd, size);
			fsync(fd);
			_descriptor = fd;
			_length = size;
			if (remap)
			{
				_memory = mmap(nullptr, size,
					(_open & OpenMode::Write ? PROT_WRITE : 0x00) |
					(_open & OpenMode::Read ? PROT_READ : 0x00) |
					(_open & OpenMode::Execute ? PROT_EXEC : 0x00),
					MAP_SHARED,
					_descriptor, 0
				);
				hint(_hint);
			}
#		endif
	}
	void Mapper::reserve_aligned(std::size_t required) noexcept
	{
#		ifdef __unix__
			struct statvfs vfs;
			if (statvfs(_filepath.c_str(), &vfs) != 0)
				return reserve(required);
			const auto page = sysconf(_SC_PAGESIZE);
			const auto block = vfs.f_frsize;

			const auto base = std::lcm(page, block);
			if (base == 0)
				return reserve(required);
			reserve(((required + base - 1) / base) * base);
#		endif
	}

	void Mapper::map(const std::filesystem::path& path, std::size_t length, unsigned char flags) noexcept
	{
		if (std::filesystem::exists(path))
			open(path, flags);
		else
			open(path, length, flags);
		map(length, flags);
	}
	void Mapper::map(const std::filesystem::path& path, unsigned char flags) noexcept
	{
		if (is_mapped())
			unmap(true);
		open(path, flags);
		map();
	}
	void Mapper::map(std::size_t length, unsigned char flags) noexcept
	{
		if (is_mapped())
			unmap(false);
#		ifdef __unix__
			_memory = mmap(nullptr, length,
				(_open & OpenMode::Write ? PROT_WRITE : 0x00) |
				(_open & OpenMode::Read ? PROT_READ : 0x00) |
				(_open & OpenMode::Execute ? PROT_EXEC : 0x00),
				MAP_SHARED,
				_descriptor, 0
			);
			_length = length;
#		endif
	}
	void Mapper::map(unsigned char flags) noexcept
	{
		map(std::filesystem::file_size(_filepath));
	}
	void Mapper::unmap(bool full) noexcept
	{
#		ifdef __unix__
			if (_vmap)
			{
				munmap(_memory, _vmap);
			}
			else
			{
				flush();
				munmap(_memory, _length);
			}
			if (full)
			{
				::close(_descriptor);
				_descriptor = -1;
			}
			_memory = nullptr;
#		endif
	}

	void Mapper::vmap(unsigned char flags) noexcept
	{
#		ifdef __unix__
			std::size_t size = mem::GiB(5012);
			while (
				(_memory = mmap(nullptr, size,
					(_open & OpenMode::Write ? PROT_WRITE : 0x00) |
					(_open & OpenMode::Read ? PROT_READ : 0x00) |
					(_open & OpenMode::Execute ? PROT_EXEC : 0x00),
					MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE,
					-1, 0
				)) == MAP_FAILED
			) size /= 2;
			_vmap = size;
#		endif
	}
	void Mapper::vmap_reserve(std::size_t size) noexcept
	{
		_length = size;
	}
	void Mapper::vmap_reset(std::size_t size) noexcept
	{
		_length = size;
	}
	void Mapper::vmap_increment(std::size_t size) noexcept
	{
		_length += size;
	}
	void Mapper::vmap_decrement(std::size_t size) noexcept
	{
		_length -= size;
	}
	void Mapper::vmap_flush() noexcept
	{
		write(View::view(memory()));
	}

	void Mapper::open(const std::filesystem::path& path, std::size_t res, unsigned char flags) noexcept
	{
		if (is_opened())
			close();

		_filepath = path;
		_open = flags;
#		ifdef __unix__
			_descriptor = ::open(
				_filepath.c_str(),
				(flags & OpenMode::Write ? O_RDWR | O_CREAT : O_RDONLY),
				0666
			);
#		endif
		if (res)
			reserve(res);
		else
			_length = std::filesystem::file_size(path);
	}
	void Mapper::open(const std::filesystem::path& path, unsigned char flags) noexcept
	{
		open(path, 0, flags);
	}
	void Mapper::close() noexcept
	{
#		ifdef __unix__
			if (_memory != nullptr)
			{
				unmap();
			}
			else
			{
				::close(_descriptor);
				_descriptor = -1;
			}
#		endif
	}
	void Mapper::remove() noexcept
	{
		close();
		std::filesystem::remove(_filepath);
	}

	void Mapper::write(View data) noexcept
	{
#		ifdef __unix__
			::write(_descriptor, data.data().data(), data.size());
#		endif
	}
	void Mapper::write(std::size_t off, std::initializer_list<View> data) noexcept
	{
#		ifdef __unix__
			std::vector<iovec> vec;
			vec.resize(data.size());
			for (std::size_t i = 0; i < data.size(); i++)
			{
				vec[i].iov_base = const_cast<unsigned char*>(data.begin()[i].data().data());
				vec[i].iov_len = data.begin()[i].size();
			}
			pwritev(_descriptor, vec.data(), vec.size(), off);
#		endif
	}
	void Mapper::write(std::size_t off, View data) noexcept
	{
#		ifdef __unix__
			pwrite(_descriptor, data.data().data(), data.size(), off);
#		endif
	}
	void Mapper::write(std::size_t off, char ch) noexcept
	{
#		ifdef __unix__
			pwrite(_descriptor, &ch, sizeof(ch), off);
#		endif
	}
	View Mapper::read(std::size_t off, std::size_t count) noexcept
	{
#		ifdef __unix__
			View result = View::copy(count);
			pread(_descriptor, result.mutate().data(), result.size(), off);
			return result;
#		endif
	}
	unsigned char Mapper::read(std::size_t off) noexcept
	{
#		ifdef __unix__
			unsigned char ch;
			pread(_descriptor, &ch, 1, off);
			return ch;
#		endif
	}

	void Mapper::hint(Access acc) noexcept
	{
#		ifdef __unix__
			int flag;
			switch (acc)
			{
				case Access::Default: flag = MADV_NORMAL; break;
				case Access::Sequential: flag = MADV_SEQUENTIAL; break;
				case Access::Random: flag = MADV_RANDOM; break;
				case Access::Hot: flag = MADV_WILLNEED; break;
				case Access::Cold: flag = MADV_DONTNEED; break;
			}
			madvise(_memory, _length, flag);
			_hint = acc;
#		endif
	}
}
