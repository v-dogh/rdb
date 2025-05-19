#ifndef RDB_UTILS_HPP
#define RDB_UTILS_HPP

#include <chrono>
#include <cstring>
#include <string_view>
#include <algorithm>
#include <optional>
#include <variant>
#include <vector>
#include <array>
#include <span>
#include <string>
#include <iomanip>
#include <sstream>
#include <memory>
#include "rdb_keytype.hpp"

namespace rdb
{
	namespace cmp
	{
		template<std::size_t Size>
		struct ConstString
		{
			std::array<char, Size> data{};

			constexpr ConstString() = default;
			constexpr ConstString(const char (&str)[Size])
			{
				std::copy(str, str + Size, data.begin());
			}

			constexpr auto view() const
			{
				return std::string_view(data.data());
			}
			constexpr auto operator*() const noexcept
			{
				return view();
			}
		};

		template<ConstString... Str>
		constexpr auto concat_const_string() noexcept
		{
			ConstString<(Str.data.size() + ...)> result;

			std::size_t off = 0;
			([&]<ConstString Cur>()
			{
				std::copy(
					Cur.data.begin(),
					Cur.data.end(),
					result.data.begin() + off
				);
				off += Cur.data.size();
			}.template operator()<Str>(), ...);

			return result;
		}
	}
	namespace uuid
	{
		constexpr auto table_compact = std::string_view(
			"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz~`!@#$%^&*()_+-={}[]';?,"
		);
		constexpr auto table_alnum = table_compact.substr(0, 63);

		template<typename Type>
		constexpr auto hash(std::string_view str) noexcept
		{
			auto hash = Type(14695981039346656037ull);
			for (decltype(auto) it : str)
			{
				hash ^= static_cast<Type>(it);
				hash *= Type(1099511628211ull);
			}
			return hash;
		}

		std::uint64_t random_machine() noexcept;
		std::uint64_t stable_machine() noexcept;
		std::array<unsigned char, 16> ugen(std::uint64_t machine) noexcept;

		std::size_t decode(const std::string& uuid, std::string_view table) noexcept;
		std::string encode(std::size_t id, std::string_view table) noexcept;

		key_type xxhash(std::span<const unsigned char> data, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash(std::initializer_list<std::span<const unsigned char>> data, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash_combine(key_type a, key_type b, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash_combine(std::initializer_list<key_type> li, key_type seed = 0xaf02cb96) noexcept;
	}

	template<std::size_t Inline = 32, std::size_t Align = 0>
	class StackView
	{
	private:
		using sbo = std::pair<std::aligned_storage_t<Inline, Align>, std::size_t>;
		std::variant<
			sbo,
			std::span<const unsigned char>,
			std::span<unsigned char>,
			std::vector<unsigned char>,
			std::nullopt_t
		> _data{ std::nullopt };
	private:
		std::span<const unsigned char> _data_impl() const noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data))
			{
				return std::get<std::span<const unsigned char>>(_data);
			}
			else if (std::holds_alternative<std::span<unsigned char>>(_data))
			{
				return std::get<std::span<unsigned char>>(_data);
			}
			else if (std::holds_alternative<std::vector<unsigned char>>(_data))
			{
				return _align(std::get<std::vector<unsigned char>>(_data));
			}
			else if (std::holds_alternative<sbo>(_data))
			{
				auto& [ v, s ] = std::get<sbo>(_data);
				return std::span(
					reinterpret_cast<const unsigned char*>(&v),
					s
				);
			}
			else
				return std::span<const unsigned char>();
		}
		std::span<unsigned char> _mutate_impl() const noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data) ||
				std::holds_alternative<std::vector<unsigned char>>(_data) ||
				std::holds_alternative<sbo>(_data))
			{
				std::terminate();
			}
			else if (std::holds_alternative<std::span<unsigned char>>(_data))
			{
				return std::get<std::span<unsigned char>>(_data);
			}
			else
				return std::span<unsigned char>();
		}
		std::span<unsigned char> _mutate_impl() noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data))
			{
				std::terminate();
			}
			else if (std::holds_alternative<std::vector<unsigned char>>(_data))
			{
				return _align(std::span(std::get<std::vector<unsigned char>>(_data)));
			}
			else if (std::holds_alternative<sbo>(_data))
			{
				auto& [ v, s ] = std::get<sbo>(_data);
				return std::span(
					reinterpret_cast<unsigned char*>(&v),
					s
				);
			}
			else if (std::holds_alternative<std::span<unsigned char>>(_data))
			{
				return std::get<std::span<unsigned char>>(_data);
			}
			else
				return std::span<unsigned char>();
		}
	protected:
		template<typename Type>
		static auto _val(const Type& value) noexcept
		{
			static_assert(sizeof(Type) <= Inline, "Invalid inline size");
			StackView view;
			new (&view._data.template emplace<sbo>().first) Type{ value };
			return view;
		}
		static auto _vec(std::vector<unsigned char> v) noexcept
		{
			StackView view;
			view._data = std::move(v);
			return view;
		}
		static auto _res(std::size_t size) noexcept
		{
			StackView view;
			if (size <= Inline)
			{
				view._data.template emplace<sbo>().second = size;
			}
			else
			{
				view._data = std::vector<unsigned char>(
					Align ? size + Align - 1 : size
				);
			}
			return view;
		}

		static auto _align(std::span<const unsigned char> data) noexcept
		{
			if (Align)
			{
				void* ptr = const_cast<void*>(static_cast<const void*>(data.data()));
				std::size_t s = data.size();
				std::align(
					Align, data.size() - (Align ? Align - 1 : 0),
					ptr, s
				);
				return std::span(
					reinterpret_cast<const unsigned char*>(ptr), s
				).subspan(0, s);
			}
			else
			{
				return data;
			}
		}
		static auto _align(std::span<unsigned char> data) noexcept
		{
			if (Align)
			{
				void* ptr = static_cast<void*>(data.data());
				std::size_t s = data.size();
				std::align(
					Align, data.size() - (Align ? Align - 1 : 0),
					ptr, s
				);
				return std::span(
					reinterpret_cast<unsigned char*>(ptr), s
				).subspan(0, s);
			}
			else
			{
				return data;
			}
		}
		static bool _is_aligned(std::span<const unsigned char> data) noexcept
		{
			if constexpr (Align)
				return reinterpret_cast<std::uintptr_t>(data.data()) % Align == 0;
			else
				return true;
		}

		void _copy(const StackView& view) noexcept
		{
			if (std::holds_alternative<sbo>(view._data))
			{
				auto& [ to, st ] = _data.template emplace<sbo>();
				auto& [ from, sf ] = std::get<sbo>(view._data);
				std::memcpy(&to, &from, sf);
				st = sf;
			}
			else
			{
				_data = view._data;
			}
		}
		void _move(StackView&& view) noexcept
		{
			if (std::holds_alternative<sbo>(view._data))
			{
				auto& [ to, st ] = _data.template emplace<sbo>();
				auto& [ from, sf ] = std::get<sbo>(view._data);
				std::memcpy(&to, &from, sf);
				st = sf;
			}
			else
			{
				_data = std::move(view._data);
			}
		}
	public:
		template<std::size_t, std::size_t>
		friend class StackView;

		static auto aligned_view(std::span<const unsigned char> data) noexcept
		{
			StackView view;
			if (_is_aligned(data)) view._data = data;
			else copy(std::span(data));
			return view;
		}
		static auto view(const StackView& data) noexcept
		{
			StackView view;
			if (std::holds_alternative<std::span<const unsigned char>>(data._data))
			{
				view._data = data.data();
			}
			else
			{
				view._data = data.data();
			}
			return view;
		}
		static auto view(std::span<unsigned char> data) noexcept
		{
			StackView view;
			view._data = data;
			return view;
		}
		static auto view(std::span<const unsigned char> data) noexcept
		{
			StackView view;
			view._data = data;
			return view;
		}
		static auto copy(std::span<const unsigned char> data) noexcept
		{
			StackView view = _res(data.size());
			std::memcpy(view.mutate().data(), data.data(), data.size());
			return view;
		}
		static auto copy(StackView&& data) noexcept
		{
			StackView view;
			if (std::holds_alternative<std::vector<unsigned char>>(view._data))
			{
				view._data = std::move(data._data);
			}
			else
			{
				return copy(data.data());
			}
			return view;
		}
		static auto copy(const StackView& data) noexcept
		{
			return copy(data.data());
		}
		static auto copy(std::vector<unsigned char> data) noexcept
		{
			StackView view;
			if (_is_aligned(data)) view._data = std::move(data);
			else copy(std::span(data));
			return view;
		}
		static auto copy(std::size_t size) noexcept
		{
			return _res(size);
		}

		StackView() noexcept = default;
		StackView(std::nullptr_t) noexcept {}
		StackView(const StackView& copy) noexcept
		{
			_copy(copy);
		}
		StackView(StackView&& copy) noexcept
		{
			_move(std::move(copy));
		}

		std::span<const unsigned char> data() const noexcept
		{
			return _data_impl();
		}
		std::span<unsigned char> mutate() const noexcept
		{
			return _mutate_impl();
		}
		std::span<unsigned char> mutate() noexcept
		{
			return _mutate_impl();
		}

		auto begin() const noexcept
		{
			return data().begin();
		}
		auto end() const noexcept
		{
			return data().end();
		}

		template<std::size_t In, std::size_t Al>
		operator StackView<In, Al>() const noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data))
			{
				return StackView<In, Al>::view(data());
			}
			else if (std::holds_alternative<std::span<unsigned char>>(_data))
			{
				return StackView<In, Al>::view(mutate());
			}
			else if (std::holds_alternative<std::vector<unsigned char>>(_data))
			{
				return StackView<In, Al>::copy(data());
			}
			else if (std::holds_alternative<std::aligned_storage_t<Inline, Align>>(_data))
			{
				StackView<In, Al> view;
				auto& from = std::get<std::aligned_storage_t<Inline, Align>>(_data);
				auto& to = (view._data = std::aligned_storage_t<In, Al>());
				std::copy(
					reinterpret_cast<const unsigned char*>(&from),
					reinterpret_cast<const unsigned char*>(&from) + sizeof(from),
					reinterpret_cast<unsigned char*>(&to)
				);
				return view;
			}
			else
				return StackView<In, Al>();
		}
		operator std::span<const unsigned char>() const noexcept
		{
			return data();
		}
		operator std::span<unsigned char>() const noexcept
		{
			return mutate();
		}

		std::size_t size() const noexcept
		{
			return data().size();
		}

		StackView& operator=(const StackView& copy) noexcept
		{
			_copy(copy);
			return *this;
		}
		StackView& operator=(StackView&& copy) noexcept
		{
			_move(std::move(copy));
			return *this;
		}

		bool operator==(std::nullptr_t) const noexcept
		{
			return std::holds_alternative<std::nullopt_t>(_data);
		}
		bool operator!=(std::nullptr_t) const noexcept
		{
			return !std::holds_alternative<std::nullopt_t>(_data);
		}
	};
	using View = StackView<>;

	template<typename Type, std::size_t Align>
	class AlignedTypedView : public StackView<sizeof(Type), Align>
	{
	private:
		using base = StackView<sizeof(Type), Align>;
	public:
		// From StackView

		static auto aligned_view(std::span<const unsigned char> data) noexcept
		{
			return AlignedTypedView(base::aligned_view(data));
		}
		static auto view(std::span<unsigned char> data) noexcept
		{
			return AlignedTypedView(base::view(data));
		}
		static auto view(std::span<const unsigned char> data) noexcept
		{
			return AlignedTypedView(base::view(data));
		}
		static auto copy(AlignedTypedView&& data) noexcept
		{
			return AlignedTypedView(base::copy(data));
		}
		static auto copy(const AlignedTypedView& data) noexcept
		{
			return AlignedTypedView(base::copy(data));
		}
		static auto copy(std::span<const unsigned char> data) noexcept
		{
			return AlignedTypedView(base::copy(data));
		}
		static auto copy(std::vector<unsigned char> data) noexcept
		{
			return AlignedTypedView(base::copy(data));
		}
		static auto copy(std::size_t size) noexcept
		{
			return AlignedTypedView(base::copy(size));
		}

		//

		static auto view(const Type& data, std::size_t dynamic = 0) noexcept
		{
			return base::view(std::span(
				reinterpret_cast<const unsigned char*>(&data),
				reinterpret_cast<const unsigned char*>(&data) + sizeof(data) + dynamic
			));
		}
		static auto copy(const Type& data, std::size_t dynamic = 0) noexcept
		{
			if (dynamic == 0)
			{
				return base::_val(data);
			}
			else
			{
				return base::copy(std::span(
					reinterpret_cast<const unsigned char*>(&data),
					reinterpret_cast<const unsigned char*>(&data) + sizeof(data) + dynamic
				));
			}
		}
		static auto copy(const Type& data, std::span<const unsigned char> dynamic) noexcept
		{
			if (dynamic.empty())
			{
				return base::_val(data);
			}
			else
			{
				std::vector<unsigned char> v;
				v.reserve(sizeof(Type) + dynamic.size());
				v.insert(v.end(),
					reinterpret_cast<const unsigned char*>(&data),
					reinterpret_cast<const unsigned char*>(&data) + sizeof(data)
				);
				v.insert(v.end(),
					dynamic.begin(),
					dynamic.end()
				);
				return base::_vec(std::move(v));
			}
		}

		AlignedTypedView() noexcept = default;
		AlignedTypedView(std::nullptr_t) noexcept {}
		AlignedTypedView(const AlignedTypedView&) noexcept = default;
		AlignedTypedView(AlignedTypedView&&) noexcept = default;
		AlignedTypedView(const base& view) noexcept : base(view) {}
		AlignedTypedView(base&& view) noexcept : base(std::move(view)) {}

		AlignedTypedView& operator=(const AlignedTypedView&) noexcept = default;
		AlignedTypedView& operator=(AlignedTypedView&&) noexcept = default;

		const auto* operator->() const noexcept
		{
			return reinterpret_cast<const Type*>(this->data().data());
		}
		auto* operator->() noexcept
		{
			return reinterpret_cast<Type*>(
				const_cast<unsigned char*>(this->data().data())
			);
		}
		const auto& operator*() const noexcept
		{
			return *reinterpret_cast<const Type*>(this->data().data());
		}
		auto& operator*() noexcept
		{
			return *reinterpret_cast<Type*>(
				const_cast<unsigned char*>(this->data().data())
			);
		}
	};

	template<typename Type>
	using TypedView = AlignedTypedView<Type, alignof(Type)>;

	namespace util
	{
		template<typename Data>
		std::string hexdump(const Data& data) noexcept
		{
			std::ostringstream oss;
			for (decltype(auto) it : data)
			{
				oss
					<< std::uppercase
					<< std::hex
					<< std::setw(2)
					<< std::setfill('0')
					<< static_cast<int>(it)
					<< ' ';
			}

			std::string result = oss.str();
			if (!result.empty())
			{
				result.pop_back();
			}
			return result;
		}

		std::chrono::nanoseconds measure(auto&& func, std::size_t iterations = 1)
		{
			const auto its = iterations;
			const auto beg = std::chrono::steady_clock::now();
			while (iterations--) func();
			const auto end = std::chrono::steady_clock::now();
			return (std::chrono::duration_cast<
				std::chrono::nanoseconds
			>(end - beg)) / its;
		}
	}
}

#endif // RDB_UTILS_HPP
