#ifndef RDB_DBG_HPP
#define RDB_DBG_HPP

#include <rdb_runtime_logs.hpp>

#if defined(RDB_RUNTIME_LOGS)
#include <iostream>
#include <format>
#include <source_location>
#define RDB_MSG(_sev_, _mod_, ...) (this->_shared.logs->log(::rdb::rs::Severity::_sev_, #_mod_, __VA_ARGS__))
#define RDB_WARN_IF(_cond_, _mod_, ...) ((_cond_) ? RDB_MSG(Warning, _mod_, __VA_ARGS__) : (*this->_shared.logs));
#define RDB_ASSERT(_cond_, _mod_) ((_cond_) ? void() : \
	(RDB_CRITICAL(_mod_, "Assertion failed:", \
	std::source_location::current().file_name(), ":", \
	std::source_location::current().line(), ":", \
	std::source_location::current().function_name()), \
	std::terminate()))
#else
#define RDB_MSG(...) (void())
#define RDB_WARN_IF(...) (void());
#define RDB_ASSERT(...) (void());
#endif
#define RDB_LOG(_mod_, ...) RDB_MSG(Info, _mod_, __VA_ARGS__);
#define RDB_TRACE(_mod_, ...) RDB_MSG(Verbose, _mod_, __VA_ARGS__);
#define RDB_WARN(_mod_, ...) RDB_MSG(Warning, _mod_, __VA_ARGS__);
#define RDB_MODULE(_mod_, ...) RDB_MSG(Module, _mod_, __VA_ARGS__);
#define RDB_DBG(_mod_, ...) RDB_MSG(Debug, _mod_, __VA_ARGS__);
#define RDB_ERROR(_mod_, ...) RDB_MSG(Error, _mod_, __VA_ARGS__);
#define RDB_CRITICAL(_mod_, ...) RDB_MSG(Critical, _mod_, __VA_ARGS__);

#endif // RDB_DBG_HPP
