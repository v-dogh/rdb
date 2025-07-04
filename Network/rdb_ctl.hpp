#ifndef RDB_CTL_HPP
#define RDB_CTL_HPP

#include <rdb_mount.hpp>
#include <rdb_containers.hpp>

namespace rdb
{
	namespace impl
	{
		template<typename Type>
		struct is_list_kind : std::false_type {};
		template<typename Type>
		struct is_list_kind<std::initializer_list<Type>> : std::true_type {};
	}

	class CTL
	{
	public:
		template<typename Type>
		using list = std::vector<Type>;
		struct string : std::string_view { using std::string_view::operator=; };
		struct word : std::string_view { using std::string_view::operator=; };
		struct variadic { std::string_view args; };
		using integer = int;
		using decimal = float;
		using ptr = std::shared_ptr<CTL>;
	private:
		Mount::ptr _mnt{ nullptr };
		ct::hash_map<std::string, std::pair<std::function<void(std::string_view)>, std::function<std::string()>>> _variables{};
		ct::hash_map<std::string, std::function<std::string(std::string_view)>> _procedures{};
	public:
		void hook_mount() noexcept;
		void hook_memory_cache() noexcept;

		static auto make(Mount::ptr mnt)
		{
			return std::make_shared<CTL>(mnt);
		}

		CTL() = default;
		CTL(Mount::ptr mnt);
		CTL(const CTL&) = delete;
		CTL(CTL&&) = default;

		Mount::ptr mnt() const noexcept
		{
			return _mnt;
		}

		std::string eval(std::string_view str) noexcept;

		template<typename Type>
		void expose_variable(const std::string& name, std::function<void(const Type&)> fset, std::function<const Type&()> fget) noexcept
		{
			_variables.emplace(
				name,
				std::make_pair(
					[fset = std::move(fset)](std::string_view str) {
						std::istringstream sstream((std::string(str)));
						if (Type value;
							sstream >> value)
						{
							fset(value);
						}
					},
					[fget = std::move(fget)]() -> std::string {
						return std::to_string(fget());
					}
				)
			);
		}
		template<typename Type>
		void expose_variable(const std::string& name, Type* ptr) noexcept
		{
			return expose_variable<Type>(
				name,
				[ptr](const Type& value) {
					*ptr = value;
				},
				[ptr]() -> const Type& {
					return *ptr;
				}
			);
		}

		template<typename... Argv>
		void expose_procedure(const std::string& name, std::function<std::string(const Argv&...)> func) noexcept
		{
			_procedures.emplace(name, [func = std::move(func)](std::string_view str) -> std::string {
				try
				{
					std::size_t off = 0;
					std::tuple<Argv...> args;
					[&]<std::size_t... Idv>(std::index_sequence<Idv...>) {
						([&] {
							using type = std::tuple_element_t<Idv, std::tuple<Argv...>>;
							if (off >= str.size() || str[off] != ' ')
								throw std::runtime_error{ std::format("Expected argument after argument {}", Idv + 1) };
							off++;

							if constexpr (std::is_same_v<type, variadic>)
							{
								std::get<Idv>(args) = variadic(
									str.substr(off)
								);
							}
							else if constexpr (std::is_same_v<type, string>)
							{
								const auto err = std::format("Expected quotes (') at {} in argument {}", off, Idv + 1);
								if (off + 1 >= str.size() || str[off++] != '\'')
									throw std::runtime_error{ err };
								const auto f = str.find('\'', off);
								if (f == std::string::npos)
									throw std::runtime_error{ err };
								std::get<Idv>(args)
									= str.substr(off, f - off);
								off = f + 1;
							}
							else if constexpr (std::is_same_v<type, word>)
							{
								const auto f = str.find(' ', off);
								if (Idv != sizeof...(Argv) - 1 && f == std::string::npos)
									throw std::runtime_error{ std::format("Expected argument after argument {}", Idv + 1) };
								std::get<Idv>(args)
									= str.substr(off, f - off);
								off = f;
							}
							else if constexpr (impl::is_list_kind<type>::value)
							{

							}
							else
							{
								std::istringstream sstream(std::string(
									str.begin() + off,
									str.end()
								));
								const auto beg = sstream.tellg();
								if (!(sstream >> std::get<Idv>(args)))
									throw std::runtime_error{ std::format("Error while parsing argument {}", Idv + 1) };
								off += sstream.tellg() - beg;
							}
						}(), ...);
					}.template operator()(std::make_index_sequence<sizeof...(Argv)>());
					return std::apply([&](const Argv&... args) {
						return func(args...);
					}, args);
				}
				catch (const std::runtime_error& idx)
				{
					return idx.what();
				}
			});
		}
		template<typename... Argv>
		void expose_procedure(const std::string& name, std::function<void(const Argv&...)> func) noexcept
		{
			return expose_procedure<Argv...>(name, std::function([func = std::move(func)](const Argv&... args) -> std::string {
				func(args...);
				return "";
			}));
		}

		CTL& operator=(const CTL&) = delete;
		CTL& operator=(CTL&&) = default;
	};
}

#endif // RDB_CTL_HPP
