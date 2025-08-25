#ifndef RDB_META_HPP
#define RDB_META_HPP

#include <array>

namespace rdb::meta
{
    template<typename... Argv>
    class PackInfo
    {
    public:
        template<typename Type>
        using map_t = std::array<Type, sizeof...(Argv)>;

        template<typename Type>
        struct type_t { using result = Type; };
    private:
        template<typename Arg, typename... Rest>
        static consteval std::size_t _largest_impl()
        {
            if constexpr(!sizeof...(Rest))
                return sizeof(Arg);
            else
                return std::max(sizeof(Arg), _largest_impl<Rest...>());
        }

        template<std::size_t index, typename Arg, typename... Rest>
        static consteval auto _at_impl()
        {
            if constexpr(index == 0)
                return type_t<Arg>();
            else
                return _at_impl<index - 1, Rest...>();
        }

        template<typename Search>
        static consteval std::size_t _map_impl() noexcept
        {
            std::size_t index{ 0 };

            auto search = [&index]<typename Arg>() consteval -> bool
            {
                if (std::is_same_v<Arg, Search>)
                    return true;
                index++;
                return false;
            };
            ((search.template operator()<Argv>()) || ...);

            return index;
        }
    public:
        PackInfo() = delete;

        static consteval std::size_t largest()
        {
            return _largest_impl<Argv...>();
        }
        template<std::size_t index>
        static consteval auto at()
        {
            return _at_impl<index, Argv...>();
        }
        static consteval auto size()
        {
            return sizeof...(Argv);
        }
        static consteval auto filter(auto f)
        {
            return (f.template operator()<Argv>() || ...);
        }
        static auto rfilter(auto f)
        {
            return (f.template operator()<Argv>() || ...);
        }

        template<typename Search, typename Type>
        static consteval const auto& map(const map_t<Type>& values)
        {
            return values[_map_impl<Search>()];
        }

        template<typename Search, typename Type>
        static consteval auto map(map_t<Type>&& values)
        {
            return values[_map_impl<Search>()];
        }
    };

    template<typename EnType, typename Pack, std::size_t Offset = 0>
    class EnumStateMap
    {
    private:
        template<std::size_t Idx>
        struct State
        {
            typename decltype(Pack::template at<Idx>())::result data{};
        };

        template<typename>
        struct Storage;

        template<std::size_t... Idx>
        struct Storage<std::index_sequence<Idx...>> :
            State<Offset + Idx>...
        {
            template<EnType Ev>
            auto& at()
            {
                return State<static_cast<std::size_t>(Ev)>::data;
            }
        };
        Storage<std::make_index_sequence<Pack::size()>> storage{};
    public:
        EnumStateMap() = default;

        template<EnType Ev>
        auto& state()
        {
            return storage.template at<Ev>();
        }
    };
}

#endif // RDB_META_HPP
