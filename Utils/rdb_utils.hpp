#ifndef RDB_UTILS_HPP
#define RDB_UTILS_HPP

#include <atomic>
#include <chrono>
#include <cstring>
#include <numeric>
#include <string_view>
#include <algorithm>
#include <optional>
#include <variant>
#include <vector>
#include <array>
#include <span>
#include <string>
#include <rdb_keytype.hpp>

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
				std::copy(str, str + Size - 1, data.begin());
			}

			constexpr auto view() const
			{
				return std::string_view(data.begin(), data.end() - 1);
			}
			constexpr auto operator*() const noexcept
			{
				return view();
			}
		};

		consteval int int_abs(int value) noexcept
		{
			return value < 0 ? -value : value;
		}
		consteval int int_count_digits(int value) noexcept
		{
			return (int_abs(value) < 10 ?
				1 : 1 + int_count_digits(int_abs(value) / 10)) + (value < 0);
		}
		template<int Value>
		consteval auto int_to_const_string() noexcept
		{
			ConstString<int_count_digits(Value) + 1> str{};
			std::size_t idx = str.data.size() - 1;

			auto val = int_abs(Value);
			do
			{
				str.data[--idx] = '0' + (val % 10);
				val /= 10;
			} while (val);

			if (Value < 0)
			{
				str.data[--idx] = '-';
			}

			return str;
		}

		template<ConstString... Str>
		constexpr auto concat_const_string() noexcept
		{
			ConstString<(Str.data.size() + ...)> result;

			std::size_t off = 0;
			([&]()
			{
				std::copy(
					Str.data.begin(),
					Str.data.end(),
					result.data.begin() + off
				);
				off += Str.data.size();
			}(), ...);

			return result;
		}

		template<typename Type>
		concept stringifiable_member = requires (const Type type)
		{
			{ type.to_string() } -> std::same_as<std::string>;
		};
		template<typename Type>
		concept stringifiable_std = requires (const Type type)
		{
			{ std::to_string(type) } -> std::same_as<std::string>;
		};

		template<typename Type>
		concept stringifiable =
			(stringifiable_member<Type> || stringifiable_std<Type>);
	}
	namespace uuid
	{
		struct uint128_t
		{
			std::uint64_t low;
			std::uint64_t high;

			std::string to_string() const noexcept;

			std::span<const unsigned char> view() const noexcept;
			std::span<unsigned char> view() noexcept;

			constexpr auto operator<=>(const uint128_t& copy) const noexcept = default;
			constexpr uint128_t operator~() const noexcept
			{
				return {
					.low = ~low,
					.high = ~high
				};
			}
		};

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

		uint128_t ugen_order_invert(uint128_t id) noexcept;
		uint128_t ugen_time(std::uint64_t machine, bool ascending = true) noexcept;
		uint128_t ugen_random() noexcept;

		std::size_t decode(const std::string& uuid, std::string_view table) noexcept;
		std::string encode(std::size_t id, std::string_view table) noexcept;

		key_type xxhash(std::span<const unsigned char> data, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash(std::initializer_list<std::span<const unsigned char>> data, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash_combine(key_type a, key_type b, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash_combine(std::initializer_list<key_type> li, key_type seed = 0xaf02cb96) noexcept;
		key_type xxhash_combine(std::span<const key_type> li, key_type seed = 0xaf02cb96) noexcept;
	}

	template<std::size_t Inline = 32>
	class StackView
	{
	private:
		using sbo = std::pair<std::array<unsigned char, Inline>, std::size_t>;
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
				return std::get<std::vector<unsigned char>>(_data);
			}
			else if (std::holds_alternative<sbo>(_data))
			{
				auto& [ v, s ] = std::get<sbo>(_data);
				return std::span(v.data(), s);
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
				return std::span(std::get<std::vector<unsigned char>>(_data));
			}
			else if (std::holds_alternative<sbo>(_data))
			{
				auto& [ v, s ] = std::get<sbo>(_data);
				return std::span(v.data(), s);
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
				view._data = std::vector<unsigned char>(size);
			}
			return view;
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
		template<std::size_t>
		friend class StackView;

		template<typename... Argv>
		static auto combine_views(Argv&&... args) noexcept
		{
			const auto size = (args.size() + ...);
			if (size == 0)
				return StackView(nullptr);
			StackView result = copy(size);

			std::size_t off = 0;
			([&]() {
				if (!args.empty())
				{
					std::memcpy(
						result.mutate().data() + off,
						args.data().data(),
						args.size()
					);
					off += args.size();
				}
			}(), ...);

			return result;
		}
		static auto view(const StackView& data) noexcept
		{
			StackView view;
			view._data = data.data();
			return view;
		}
		static auto view(StackView& data) noexcept
		{
			StackView view;
			if (data.is_view())
				view._data = data.data();
			else
				view._data = data.mutate();
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
		static auto copy() noexcept
		{
			StackView view;
			view._data = std::vector<unsigned char>();
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
		static auto copy(std::span<std::span<const unsigned char>> data) noexcept
		{
			const auto size = std::accumulate(data.begin(), data.end(), std::size_t(0),
				[](std::size_t ctr, const auto& v) { return ctr + v.size(); }
			);
			if (size == 0)
				return StackView(nullptr);
			StackView result = copy(
				size
			);

			std::size_t off = 0;
			for (decltype(auto) it : data)
			{
				if (!it.empty())
				{
					std::memcpy(
						result.mutate() + off,
						it.data(),
						it.size()
					);
					off += it.size();
				}
			}

			return result;
		}
		static auto copy(std::span<const StackView> data) noexcept
		{
			const auto size = std::accumulate(data.begin(), data.end(), std::size_t(0),
				[](std::size_t ctr, const auto& v) { return ctr + v.size(); }
			);
			if (size == 0)
				return StackView(nullptr);
			StackView result = copy(
				size
			);

			std::size_t off = 0;
			for (decltype(auto) it : data)
			{
				if (!it.empty())
				{
					std::memcpy(
						result.mutate().data() + off,
						it.data().data(),
						it.size()
					);
					off += it.size();
				}
			}

			return result;
		}
		static auto copy(const StackView& data) noexcept
		{
			return copy(data.data());
		}
		static auto copy(std::vector<unsigned char> data) noexcept
		{
			StackView view;
			view._data = std::move(data);
			return view;
		}
		static auto copy(std::size_t size) noexcept
		{
			return _res(size);
		}

		static constexpr auto inline_size() noexcept
		{
			return Inline;
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

		bool is_view() const noexcept
		{
			return std::holds_alternative<std::span<const unsigned char>>(_data);
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

		StackView subview(std::size_t off, std::size_t length = ~0ull) const noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data))
			{
				const auto s = data();
				return view(s.subspan(off, std::min(length, s.size() - off)));
			}
			else
			{
				const auto s = mutate();
				return view(s.subspan(off, std::min(length, s.size() - off)));
			}
		}
		StackView subview(std::size_t off, std::size_t length = ~0ull) noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data))
			{
				const auto s = data();
				return view(s.subspan(off, std::min(length, s.size() - off)));
			}
			else
			{
				const auto s = mutate();
				return view(s.subspan(off, std::min(length, s.size() - off)));
			}
		}

		auto begin() const noexcept
		{
			return data().begin();
		}
		auto end() const noexcept
		{
			return data().end();
		}

		const std::vector<unsigned char>* vec() const noexcept
		{
			if (std::holds_alternative<std::vector<unsigned char>>(_data))
				return &std::get<std::vector<unsigned char>>(_data);
			return nullptr;
		}
		std::vector<unsigned char>* vec() noexcept
		{
			if (std::holds_alternative<std::vector<unsigned char>>(_data))
				return &std::get<std::vector<unsigned char>>(_data);
			return nullptr;
		}

		template<std::size_t In>
		operator StackView<In>() const noexcept
		{
			if (std::holds_alternative<std::span<const unsigned char>>(_data))
			{
				return StackView<In>::view(data());
			}
			else if (std::holds_alternative<std::span<unsigned char>>(_data))
			{
				return StackView<In>::view(mutate());
			}
			else if (std::holds_alternative<std::vector<unsigned char>>(_data))
			{
				return StackView<In>::copy(data());
			}
			else if (std::holds_alternative<sbo>(_data))
			{
				StackView<In> view;
				auto& from = std::get<sbo>(_data);
				auto& to = view._data.template emplace<typename StackView<In>::sbo>();
				to.second = from.second;
				std::memcpy(
					&to.first,
					&from.first,
					sizeof(from.first)
				);
				return view;
			}
			else
				return StackView<In>();
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
		bool empty() const noexcept
		{
			return
				std::holds_alternative<std::nullopt_t>(_data) ||
				size() == 0;
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

	template<typename Type>
	class TypedView : public StackView<32>
	{
	private:
		using base = StackView<32>;
	public:
		// From StackView

		static auto view(std::span<unsigned char> data) noexcept
		{
			return TypedView(base::view(data));
		}
		static auto view(std::span<const unsigned char> data) noexcept
		{
			return TypedView(base::view(data));
		}
		static auto copy(TypedView&& data) noexcept
		{
			return TypedView(base::copy(data));
		}
		static auto copy(const TypedView& data) noexcept
		{
			return TypedView(base::copy(data));
		}
		static auto copy(std::span<const unsigned char> data) noexcept
		{
			return TypedView(base::copy(data));
		}
		static auto copy(std::vector<unsigned char> data) noexcept
		{
			return TypedView(base::copy(data));
		}
		static auto copy(std::size_t size) noexcept
		{
			return TypedView(base::copy(size));
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

		TypedView() noexcept = default;
		TypedView(std::nullptr_t) noexcept {}
		TypedView(const TypedView&) noexcept = default;
		TypedView(TypedView&&) noexcept = default;
		TypedView(const base& view) noexcept : base(view) {}
		TypedView(base&& view) noexcept : base(std::move(view)) {}

		TypedView subview(std::size_t off, std::size_t length = ~0ull) const noexcept
		{
			return view(this->data().subspan(off, length));
		}
		TypedView subview(std::size_t off, std::size_t length = ~0ull) noexcept
		{
			if (this->is_view())
			{
				return view(this->data().subspan(off, length));
			}
			else
			{
				return view(this->mutate().subspan(off, length));
			}
		}

		TypedView& operator=(const TypedView&) noexcept = default;
		TypedView& operator=(TypedView&&) noexcept = default;

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

	namespace cmp
	{
		template<typename>
		struct is_typed_view : std::false_type {};
		template<typename Type>
		struct is_typed_view<TypedView<Type>> : std::true_type {};
	}
	namespace util
	{
		void spinlock_yield() noexcept;
		void bind_thread(std::size_t core) noexcept;

		template<typename Type>
        void nano_wait_for(const std::atomic<Type>& var, const Type& value, std::memory_order order = std::memory_order::seq_cst) noexcept
		{
			for (int i = 0; i < 1000; ++i)
			{
				if (var.load(order) == value)
					return;
				util::spinlock_yield();
			}

            auto expected = var.load(order);
			while (expected != value)
			{
                var.wait(expected, order);
                expected = var.load(order);
			}
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
		std::chrono::nanoseconds measure_reset(auto&& func, auto&& reset, std::size_t iterations = 1)
		{
			const auto its = iterations;
			std::chrono::nanoseconds delta{ 0 };
			const auto beg = std::chrono::steady_clock::now();
			while (iterations--)
			{
				func();
				const auto dbeg = std::chrono::steady_clock::now();
				reset();
				const auto dend = std::chrono::steady_clock::now();
				delta += dend - dbeg;
			}
			const auto end = std::chrono::steady_clock::now();
			return ((std::chrono::duration_cast<
				std::chrono::nanoseconds
			>(end - beg)) - delta) / its;
		}

		template<cmp::stringifiable_member Type>
		std::string to_string(const Type& value)
		{
			return value.to_string();
		}
		template<cmp::stringifiable_std Type>
		std::string to_string(const Type& value)
		{
			return std::to_string(value);
		}

		template<cmp::stringifiable_member Type>
		std::ostream& operator<<(std::ostream& os, const Type& value)
		{
			return os << value.to_string();
		}
	}
}

#endif // RDB_UTILS_HPP
