#ifndef RDB_DBG_HPP
#define RDB_DBG_HPP

#if !defined(NDEBUG) || defined(RDB_RUNTIME_LOGS)
#include <iostream>
#include <format>
#include <source_location>
#define RDB_LOG(_logs_) (std::cout << std::format("[RDB] {}", _logs_) << std::endl);
#define RDB_FMT(_fmt_, ...) RDB_LOG(std::format(_fmt_ __VA_OPT__(,) __VA_ARGS__))
#define RDB_WARN(_fmt_, ...) RDB_FMT(_fmt_, __VA_ARGS__)
#define RDB_TRACE(_fmt_, ...) RDB_FMT(_fmt_, __VA_ARGS__)
#define RDB_WARN_IF(_cond_, _fmt_, ...) ((_cond_) ? (std::cout << std::format("[RDB] {}", std::format(_fmt_ __VA_OPT__(,) __VA_ARGS__))) : (std::cout));
#define RDB_ASSERT(_cond_) ((_cond_) ? void() : \
	(RDB_FMT("Assertion failed: {}:{}:{}", \
	std::source_location::current().file_name(), \
	std::source_location::current().line(), \
	std::source_location::current().function_name()), \
	std::terminate()))
#else
#define RDB_LOG(_) (void());
#define RDB_FMT(...) RDB_LOG(_)
#define RDB_WARN(...) RDB_LOG(_)
#define RDB_TRACE(...) RDB_LOG(_)
#define RDB_WARN_IF( ...) RDB_LOG(_)
#define RDB_ASSERT(_) RDB_LOG(_)
#endif

#endif // RDB_DBG_HPP
