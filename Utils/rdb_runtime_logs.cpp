#include <signal.h>
#include <cmath>
#include <chrono>
#include <shared_mutex>
#include <absl/debugging/stacktrace.h>
#include <rdb_runtime_logs.hpp>
#include <rdb_locale.hpp>
#include <iostream>

namespace rdb::rs
{
	namespace impl
	{
		LogStream::LogStream(std::span<unsigned char> buffer) : _buffer(buffer)
		{
			setp(
				reinterpret_cast<char*>(_buffer.data()),
				reinterpret_cast<char*>(_buffer.data()) + _buffer.size()
			);
		}

		int LogStream::sync()
		{
			return 0;
		}
		int LogStream::overflow(int ch)
		{
			if (ch == EOF || pptr() == epptr())
				return EOF;
			*pptr() = static_cast<char>(ch);
			pbump(1);
			return ch;
		}

		std::size_t LogStream::size() const noexcept
		{
			return pptr() - pbase();
		}
		std::span<unsigned char> LogStream::buffer() const noexcept
		{
			return std::span(
				_buffer.data(),
				size()
			);
		}
		void LogStream::reset() noexcept
		{
			setp(pbase(), epptr());
		}
	}

	std::string_view signal_to_str(int sig) noexcept
	{
		switch (sig)
		{
			case SIGSEGV: return "SIGSEGV (Segmentation Fault)";
			case SIGABRT: return "SIGABRT (Abort)";
			case SIGFPE: return "SIGFPE (Floating Point Exception)";
			case SIGILL: return "SIGILL (Illegal Instruction)";
			case SIGBUS: return "SIGBUS (Bus Error)";
			case SIGTERM: return "SIGTERM (Termination Request)";
			case SIGINT: return "SIGINT (Exit)";
			case SIGQUIT: return "SIGQUIT (Quit)";
			default: return "Unknown signal";
		}
	}
	std::string_view severity_to_str(Severity severity) noexcept
	{
		switch (severity)
		{
		case Severity::Info: return "Info";
		case Severity::Verbose: return "Verbose";
		case Severity::Warning: return "Warning";
		case Severity::Error: return "Error";
		case Severity::Critical: return "Critical";
		case Severity::Module: return "Module";
		case Severity::Reserved: return "Reserved";
		case Severity::Debug: return "Debug";
		default: return "Unknown";
		};
	}

	void ConsoleSink::accept(std::span<const unsigned char> buffer) noexcept
	{
		std::clog << RuntimeLogs::print(buffer) << '\n';
	}
	void ColoredConsoleSink::accept(std::span<const unsigned char> buffer) noexcept
	{
		constexpr std::string_view reset = "\033[0m";
		constexpr std::string_view slate_blue = "\033[38;2;75;0;130m";
		constexpr std::string_view silver = "\033[38;2;192;192;192m";
		constexpr std::string_view orange = "\033[38;2;255;165;0m";
		constexpr std::string_view yellow = "\033[93m";
		constexpr std::string_view olive = "\033[38;2;128;128;0m";
		constexpr std::string_view red = "\033[91m";
		constexpr std::string_view cyan = "\033[38;2;0;159;159m";
		constexpr std::array<std::string_view, std::size_t(Severity::Critical)> table{
			silver,
			silver,
			orange,
			cyan,
			yellow,
			olive,
			red
		};

		const auto entry = RuntimeLogs::decode(buffer);
		const auto color = table[std::size_t(entry.severity) - 1];
		std::clog << std::format(
			"{1:}[{3:%D} - {3:%H}:{3:%M}:{3:%S}]{0:}({2:}{4:}{0:})<{2:}{5:}{0:}> : {2:}{6:}{0:}\n",
			reset,
			slate_blue,
			color,
			std::chrono::time_point_cast<std::chrono::seconds>(entry.timestamp),
			severity_to_str(entry.severity),
			entry.module,
			entry.msg
		);
	}

	std::shared_mutex global_logs_mtx{};
	std::vector<std::shared_ptr<RuntimeLogs>> global_logs{};
	std::array<void(*)(int), NSIG> global_logs_prev_handlers{};
	std::atomic<bool> hooked{ false };

	RuntimeLogs::ptr RuntimeLogs::make(Config cfg)
	{
		auto ptr = std::shared_ptr<RuntimeLogs>(new RuntimeLogs{ std::move(cfg) });
		if (ptr->_cfg.handle_signals)
		{
			std::lock_guard lock(global_logs_mtx);
			global_logs.push_back(ptr);
			_hook_signal();
		}
		return ptr;
	}
	RuntimeLogs::ptr RuntimeLogs::make()
	{
		return make(Config());
	}
	RuntimeLogs::LogEntry RuntimeLogs::decode(std::span<const unsigned char> log) noexcept
	{
		LogEntry entry;

		std::size_t off = 0;
		{
			entry.severity = static_cast<Severity>(log[off++]);
			entry.id = byte::sread<std::uint64_t>(log, off);
			entry.timestamp = std::chrono::system_clock::time_point(std::chrono::system_clock::duration(
				byte::sread<std::uint64_t>(log, off)
			));
		}
		{
			const auto len = log[off++];
			entry.module = std::string_view(
				reinterpret_cast<const char*>(log.data()) + off,
				len
			);
			off += len;
		}
		{
			const auto len = byte::sread<std::uint16_t>(log, off);
			entry.msg = std::string_view(
				reinterpret_cast<const char*>(log.data()) + off,
				len
			);
			off += len;
		}
		entry.length = off;

		return entry;
	}
	std::string RuntimeLogs::print(std::span<const unsigned char> log) noexcept
	{
		const auto entry = decode(log);
		return std::format(
			"[{0:%D} - {0:%H}:{0:%M}:{0:%S}]({1:})<{2:}> : {3:}",
			std::chrono::time_point_cast<std::chrono::seconds>(entry.timestamp),
			severity_to_str(entry.severity),
			entry.module,
			entry.msg
		);
	}

	RuntimeLogs::RuntimeLogs(Config cfg)
		: _cfg(std::move(cfg))
	{
		std::filesystem::create_directories(_cfg.root);

		std::filesystem::remove(_cfg.root/"logs.dat");
		std::filesystem::remove(_cfg.root/"data.dat");
		std::filesystem::remove(_cfg.root/"dump.log");

		_logs.map(_cfg.root/"logs.dat", _cfg.max_log_data);
		_data.map(_cfg.root/"data.dat", _cfg.max_static_data);
		_fallback.open(_cfg.root/"dump.log");
	}

	inline std::size_t RuntimeLogs::_stacktrace(stacktrace_print& out) noexcept
	{
		stacktrace_data data{};
		absl::GetStackTrace(data.data(), max_strace_depth, 0);

		std::size_t off = 0;
		for (std::size_t i = 0; i < data.size(); i++)
		{
			out[off++] = '#';

			{
				const auto beg = out.begin() + off;
				const auto end = beg + 3;
				const auto [ ptr, _ ] = std::to_chars(beg, end, i);
				const auto diff = end - ptr;
				std::memset(ptr, '0', diff);
			}
			off += 3;

			out[off++] = ':';
			out[off++] = ' ';
			out[off++] = '0';
			out[off++] = 'x';

			{
				const auto max_len = 16;
				const auto val = std::uintptr_t(data[i]);
				const auto len = static_cast<std::size_t>(std::log(val) / std::log(16));
				const auto set = max_len - len;

				const auto beg = out.begin() + off + set;
				const auto end = beg + len;
				std::to_chars(beg, end, val);
				std::memset(out.data() + off, '0', set);
			}
			off += 16;
		}

		return off;
	}
	void RuntimeLogs::_signal_handler(int sig) noexcept
	{
		std::lock_guard lock(global_logs_mtx);

		stacktrace_print stacktrace;
		const auto strace = std::string_view(
			stacktrace.begin(),
			stacktrace.begin() + _stacktrace(stacktrace)
		);
		const auto signal = signal_to_str(sig);

		const auto strace_view = View::view(byte::sspan(strace));
		const auto edl_view = View::view(byte::sspan(std::string_view("\n")));
		const auto signal_view = View::view(byte::sspan(signal));

		for (decltype(auto) it : global_logs)
		{
			it->_fallback.write(0, {
				strace_view,
				edl_view,
				signal_view
			});
		}

		for (decltype(auto) it : global_logs)
			it->sync();

		global_logs_prev_handlers[sig](sig);
	}
	void RuntimeLogs::_hook_signal() noexcept
	{
		if (!hooked.load(std::memory_order::acquire))
		{
			global_logs_prev_handlers[SIGSEGV] = signal(SIGSEGV, _signal_handler);
			global_logs_prev_handlers[SIGFPE] = signal(SIGFPE, _signal_handler);
			global_logs_prev_handlers[SIGILL] = signal(SIGILL, _signal_handler);
			global_logs_prev_handlers[SIGBUS] = signal(SIGBUS, _signal_handler);
			global_logs_prev_handlers[SIGTERM] = signal(SIGTERM, _signal_handler);
			global_logs_prev_handlers[SIGABRT] = signal(SIGABRT, _signal_handler);
			global_logs_prev_handlers[SIGQUIT] = signal(SIGQUIT, _signal_handler);
			hooked.store(true, std::memory_order::release);
		}
	}

	std::size_t RuntimeLogs::_log_size() const noexcept
	{
		return
			// Severity
			sizeof(Severity) +
			// Msg ID
			sizeof(std::uint64_t) +
			// Msg timestamp
			sizeof(std::uint64_t) +
			// Module size
			sizeof(std::uint8_t) +
			// module.size() +
			// Msg size
			sizeof(std::uint16_t) +
			_cfg.max_log_msg_len;
	}
	std::size_t RuntimeLogs::_log_capacity() const noexcept
	{
		return _cfg.max_log_data / _log_size();
	}
	void RuntimeLogs::_log(Severity severity, std::string_view module, std::span<const unsigned char> data, std::span<const unsigned char> msg) noexcept
	{
		// [[ unlikely ]] if (!data.empty())
		// {
		// 	const auto size =
		// 		// Data ID
		// 		sizeof(std::uint64_t) +
		// 		// Data size
		// 		sizeof(std::uint64_t) +
		// 		data.size();

		// 	const auto id = _data_ctr++;
		// 	auto off = _data_ptr.load();
		// 	if (off + size > _cfg.max_static_data)
		// 	{
		// 		_data_ptr.store(0);
		// 		off = 0;
		// 	}
		// 	else
		// 		off = _data_ptr.fetch_add(size);

		// 	off += byte::swrite<std::uint64_t>(_data.memory(), off, id);
		// 	off += byte::swrite<std::uint64_t>(_data.memory(), off, data.size());
		// 		   byte::swrite(_data.memory(), off, byte::sspan(data));
		// }

		const auto size = _log_size();
		const auto capacity = _log_capacity();
		const auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
		const auto id = _logs_ctr++;
		const auto len = std::min(_cfg.max_log_msg_len, msg.size());
		const auto beg = size * (id % capacity);
		auto off = beg;

		_logs.memory()[off++] = static_cast<unsigned char>(severity);
		off += byte::swrite<std::uint64_t>(_logs.memory(), off, id);
		off += byte::swrite<std::uint64_t>(_logs.memory(), off, timestamp);
		_logs.memory()[off++] = static_cast<std::uint8_t>(module.size());
		off += byte::swrite(_logs.memory(), off, byte::sspan(module));
		off += byte::swrite<std::uint16_t>(_logs.memory(), off, len);
			   byte::swrite(_logs.memory(), off, byte::sspan(msg).subspan(
					0, len
				));

		for (decltype(auto) it : _sinks)
			it->accept(_logs.memory().subspan(beg, off - beg));
	}

	void RuntimeLogs::sync() noexcept
	{

	}
}
