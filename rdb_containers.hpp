#ifndef RDB_CONTAINERS_HPP
#define RDB_CONTAINERS_HPP

#include <absl/container/flat_hash_map.h>
#include <LibART/src/art.h>
#include <jemalloc/jemalloc.h>
#include <span>
#include <vector>
#include <string>

namespace rdb::ct
{
	namespace impl
	{
		template<typename Type>
		class JeAllocator
		{
		public:
			using value_type = Type;
			using size_type = std::size_t;
			using difference_type = std::ptrdiff_t;
			using pointer = Type*;
			using const_pointer = const Type*;

			constexpr JeAllocator() noexcept = default;
			template<typename Other> constexpr JeAllocator(const JeAllocator<Other>&) noexcept {}

			pointer allocate(size_type n)
			{
				if constexpr (alignof(Type) == 1)
				{
					return std::malloc(n * sizeof(Type));
				}
				else
				{
					return new Type[n];
				}
			}
			pointer reallocate(pointer p, size_type psize, size_type nsize) noexcept requires (alignof(Type) == 1)
			{
				return std::realloc(p, nsize * sizeof(Type));
			}
			void deallocate(pointer p, size_type) noexcept
			{
				if constexpr (alignof(Type) == 1)
				{
					std::free(p);
				}
				else
				{
					delete[] p;
				}
			}

			constexpr size_type max_size() const noexcept
			{
				return static_cast<size_type>(-1) / sizeof(Type);
			}

			template<typename Other> friend constexpr bool operator==(const JeAllocator&, const JeAllocator<Other>&) noexcept { return true; }
			template<typename Other> friend constexpr bool operator!=(const JeAllocator&, const JeAllocator<Other>&) noexcept { return false; }
		};

		template<typename Node>
		class ArtWrapper
		{
		public:
			using key = std::span<unsigned char>;
			using const_key = std::span<const unsigned char>;
			using pointer = Node*;
			using const_pointer = const Node*;
		private:
			mutable art_tree _tree{};
		public:
			template<typename... Argv>
			static pointer allocate_node(Argv&&... args) noexcept
			{
				auto* buffer = static_cast<pointer>(operator new(
					Node::allocation_size(args...),
					std::align_val_t(alignof(Node))
				));
				new (buffer) Node{ std::forward<Argv>(args)... };
				return buffer;
			}
			static void delete_node(pointer ptr) noexcept
			{
				ptr->~Node();
				operator delete(ptr, std::align_val_t(alignof(Node)));
			}
		public:
			ArtWrapper()
			{
				art_tree_init(&_tree);
			}
			ArtWrapper(ArtWrapper&& copy)
			{
				_tree = copy._tree;
				copy._tree = art_tree();
			}
			~ArtWrapper()
			{
				clear();
			}

			template<typename... Argv>
			pointer insert(const_key key, Argv&&... args) noexcept
			{
				auto* buffer = allocate_node(std::forward<Argv>(args)...);
				if (auto* ptr = art_insert(
						&_tree,
						key.data(),
						key.size(),
						buffer
					); ptr != nullptr)
				{
					delete_node(static_cast<pointer>(ptr));
				}
				return buffer;
			}
			template<typename... Argv>
			pointer insert(const_key key, Node* node) noexcept
			{
				if (auto* ptr = art_insert(
						&_tree,
						key.data(),
						key.size(),
						node
					); ptr != nullptr)
				{
					delete_node(static_cast<pointer>(ptr));
				}
				return node;
			}
			void remove(const_key key) noexcept
			{
				delete_node(
					art_delete(&_tree, key.data(), key.size())
				);
			}

			const_pointer find(const_key key) const noexcept
			{
				if (auto* ptr = art_search(&_tree, key.data(), key.size());
					ptr != nullptr)
				{
					return static_cast<const_pointer>(ptr);
				}
				return nullptr;
			}
			pointer find(const_key key) noexcept
			{
				return const_cast<pointer>(
					const_cast<const ArtWrapper*>(this)
						->find(key)
				);
			}

			void clear() noexcept
			{
				if (_tree.root != nullptr)
				{
					art_iter(&_tree, +[](void*, const unsigned char* key, unsigned int, void* value) -> int {
						delete_node(static_cast<pointer>(value));
						return 0;
					}, nullptr);
					art_tree_destroy(&_tree);
					art_tree_init(&_tree);
				}
			}

			// Terminates if callback returns false
			void foreach(std::function<bool(const_key, const_pointer)> callback) const noexcept
			{
				if (_tree.root != nullptr)
				{
					art_iter(&_tree, +[](void* dat, const unsigned char* key, unsigned int len, void* value) -> int {
						return !(*static_cast<std::function<bool(const_key, const_pointer)>*>(dat))
							(const_key(key, len),
							 static_cast<const_pointer>(value));
					}, &callback);
				}
			}
			void foreach(std::function<bool(const_key, pointer)> callback) noexcept
			{
				if (_tree.root != nullptr)
				{
					art_iter(&_tree, +[](void* dat, const unsigned char* key, unsigned int len, void* value) -> int {
						return !(*static_cast<std::function<bool(const_key, pointer)>*>(dat))
							(const_key(key, len),
							 static_cast<pointer>(value));
					}, &callback);
				}
			}

			void foreach(const_key start, std::function<bool(const_key, const_pointer)> callback) const noexcept
			{
				if (_tree.root != nullptr)
				{
					art_iter_prefix(&_tree, start.data(), +[](void* dat, const unsigned char* key, unsigned int len, void* value) -> int {
						return !(*static_cast<std::function<bool(const_key, const_pointer)>*>(dat))
							(const_key(key, len),
							 static_cast<const_pointer>(value));
					}, &callback);
				}
			}
			void foreach(const_key start, std::function<bool(const_key, pointer)> callback) noexcept
			{
				if (_tree.root != nullptr)
				{
					art_iter_prefix(&_tree, start.data(), +[](void* dat, const unsigned char* key, unsigned int len, void* value) -> int {
						return !(*static_cast<std::function<bool(const_key, pointer)>*>(dat))
							(const_key(key, len),
							 static_cast<pointer>(value));
					}, &callback);
				}
			}

			ArtWrapper& operator=(const ArtWrapper&) = delete;
			ArtWrapper& operator=(ArtWrapper&& copy) noexcept
			{
				_tree = copy._tree;
				copy._tree = art_tree();
				return *this;
			}
		};
	}

	template<typename Type> using allocator = impl::JeAllocator<Type>;
	template<typename Key, typename Value> using hash_map = absl::flat_hash_map<Key, Value>;
	template<typename Value> using vector = std::vector<Value>;
	template<typename Node> using ordered_byte_map = impl::ArtWrapper<Node>;
	using string = std::string;
}

#endif // RDB_CONTAINERS_HPP
