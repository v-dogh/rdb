#ifndef RDB_RUNTIME_LOGS_H
#define RDB_RUNTIME_LOGS_H

#include <array>
#include <atomic>
#include <memory>
#include <filesystem>
#include <rdb_memunits.hpp>
#include <rdb_mapper.hpp>

namespace rdb::rs
{
	namespace impl
	{
		class LogStream : public std::streambuf
		{
		private:
			std::span<unsigned char> _buffer{};
		public:
			LogStream(std::span<unsigned char> buffer);

			int sync() override;
			int overflow(int ch) override;

			std::size_t size() const noexcept;
			std::span<unsigned char> buffer() const noexcept;
			void reset() noexcept;
		};
	}

	enum class Severity : unsigned char
	{
		Reserved,
		// Fine grained information regarding more complex operations
		Verbose,
		// Regular information regarding the function of operations
		Info,
		// Used for debug messages
		Debug,
		// Module events (e.g. during loading or shutdown)
		Module,
		// Information that signals not an error but a possible misconfiguration or unintended effect
		Warning,
		// A controlled error during the lifetime of an operation
		Error,
		// An uncontrolled error during the lifetime of an operation (e.g. an out of memory exception)
		Critical,
	};

	std::string_view signal_to_str(int sig) noexcept;
	std::string_view severity_to_str(Severity severity) noexcept;

	class RuntimeLogSink
	{
	public:
		virtual ~RuntimeLogSink() = default;
		virtual void accept(std::span<const unsigned char> buffer) noexcept {}
	};
	class ConsoleSink : public RuntimeLogSink
	{
	public:
		virtual void accept(std::span<const unsigned char> buffer) noexcept override;
	};
	class ColoredConsoleSink : public RuntimeLogSink
	{
	public:
		virtual void accept(std::span<const unsigned char> buffer) noexcept override;
	};

	class RuntimeLogs : std::enable_shared_from_this<RuntimeLogs>
	{
	public:
		struct Config
		{
			std::filesystem::path root{ "/" };
			std::size_t max_log_data{ rdb::mem::MiB(8) };
			std::size_t max_static_data{ rdb::mem::MiB(2) };
			std::size_t min_sync_count{ 32 };
			std::size_t max_log_msg_len{ 86 };
			bool handle_signals{ true };
		};
		struct LogEntry
		{
			Severity severity{};
			std::uint64_t id{};
			std::chrono::system_clock::time_point timestamp{};
			std::string_view module{};
			std::string_view msg{};
			std::size_t length{};
		};
		using ptr = std::shared_ptr<RuntimeLogs>;
	private:
		// stacktrace_type length
		// '#XXX: '  - 6 bytes (max stacktrace depth)
		// '0x'      - 2 bytes
		// 'FF...FF' - 16 bytes (for max uint64)
		// * max stacktrace
		// total 3072 bytes

		static constexpr auto max_strace_depth = 128;
		static constexpr auto max_strace_line_width = 6 + 2 + 16;

		using stacktrace_data = std::array<void*, max_strace_depth>;
		using stacktrace_print = std::array<char, max_strace_depth * max_strace_line_width>;

		static inline std::size_t _stacktrace(stacktrace_print& out) noexcept;
		static void _signal_handler(int sig) noexcept;
		static void _hook_signal() noexcept;
	private:
		Config _cfg{};
		Mapper _logs{};
		Mapper _data{};
		Mapper _fallback{};

		std::atomic<std::size_t> _last_clog_ctr{ 0 };
		std::atomic<std::size_t> _logs_ctr{ 0 };
		std::atomic<std::size_t> _data_ptr{ 0 };
		std::atomic<std::size_t> _data_ctr{ 0 };

		std::vector<std::unique_ptr<RuntimeLogSink>> _sinks{};

		std::size_t _log_size() const noexcept;
		std::size_t _log_capacity() const noexcept;
		void _log(Severity severity, std::string_view module, std::span<const unsigned char> data, std::span<const unsigned char> msg) noexcept;

		explicit RuntimeLogs(Config cfg);
		RuntimeLogs() : RuntimeLogs(Config{}) {}
	public:
		static ptr make(Config cfg);
		static ptr make();
		static LogEntry decode(std::span<const unsigned char> log) noexcept;
		static std::string print(std::span<const unsigned char> log) noexcept;

		~RuntimeLogs()
		{
			sync();
		}

		void sync() noexcept;

		template<typename Type, typename... Argv>
		Type& sink(Argv&&... args)
		{
			_sinks.push_back(std::make_unique<Type>(
				std::forward<Argv>(args)...
			));
			return static_cast<Type&>(*_sinks.back());
		}

		template<typename... Argv>
		RuntimeLogs& logd(Severity severity, std::string_view module, std::string_view data, Argv&&... args) noexcept
		{
			thread_local std::array<unsigned char, 256> buffer;
			thread_local impl::LogStream stream(buffer);
			thread_local std::ostream out(&stream);

			(out <<  ... << std::forward<Argv>(args));

			_log(severity, module, std::span<const unsigned char>(
				reinterpret_cast<const unsigned char*>(data.data()),
				data.size()
			), stream.buffer());

			stream.reset();

			return *this;
		}
		template<typename... Argv>
		RuntimeLogs& log(Severity severity, std::string_view module, Argv&&... args) noexcept
		{
			return logd(severity, module, "", std::forward<Argv>(args)...);
		}
	};
}

#endif // RDB_RUNTIME_LOGS_H
