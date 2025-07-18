#ifndef JEMALLOC_INTERNAL_CACHE_BIN_H
#define JEMALLOC_INTERNAL_CACHE_BIN_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_externs.h"
#include "jemalloc/internal/ql.h"
#include "jemalloc/internal/safety_check.h"
#include "jemalloc/internal/sz.h"

/*
 * The cache_bins are the mechanism that the tcache and the arena use to
 * communicate.  The tcache fills from and flushes to the arena by passing a
 * cache_bin_t to fill/flush.  When the arena needs to pull stats from the
 * tcaches associated with it, it does so by iterating over its
 * cache_bin_array_descriptor_t objects and reading out per-bin stats it
 * contains.  This makes it so that the arena need not know about the existence
 * of the tcache at all.
 */

/*
 * The size in bytes of each cache bin stack.  We also use this to indicate
 * *counts* of individual objects.
 */
typedef uint16_t cache_bin_sz_t;

#define JUNK_ADDR ((uintptr_t)0x7a7a7a7a7a7a7a7aULL)
/*
 * Leave a noticeable mark pattern on the cache bin stack boundaries, in case a
 * bug starts leaking those.  Make it look like the junk pattern but be distinct
 * from it.
 */
static const uintptr_t cache_bin_preceding_junk = JUNK_ADDR;
/* Note: JUNK_ADDR vs. JUNK_ADDR + 1 -- this tells you which pointer leaked. */
static const uintptr_t cache_bin_trailing_junk = JUNK_ADDR + 1;
/*
 * A pointer used to initialize a fake stack_head for disabled small bins
 * so that the enabled/disabled assessment does not rely on ncached_max.
 */
extern const uintptr_t disabled_bin;

/*
 * That implies the following value, for the maximum number of items in any
 * individual bin.  The cache bins track their bounds looking just at the low
 * bits of a pointer, compared against a cache_bin_sz_t.  So that's
 *   1 << (sizeof(cache_bin_sz_t) * 8)
 * bytes spread across pointer sized objects to get the maximum.
 */
#define CACHE_BIN_NCACHED_MAX (((size_t)1 << sizeof(cache_bin_sz_t) * 8) \
    / sizeof(void *) - 1)

/*
 * This lives inside the cache_bin (for locality reasons), and is initialized
 * alongside it, but is otherwise not modified by any cache bin operations.
 * It's logically public and maintained by its callers.
 */
typedef struct cache_bin_stats_s cache_bin_stats_t;
struct cache_bin_stats_s {
	/*
	 * Number of allocation requests that corresponded to the size of this
	 * bin.
	 */
	uint64_t nrequests;
};

/*
 * Read-only information associated with each element of tcache_t's tbins array
 * is stored separately, mainly to reduce memory usage.
 */
typedef struct cache_bin_info_s cache_bin_info_t;
struct cache_bin_info_s {
	cache_bin_sz_t ncached_max;
};

/*
 * Responsible for caching allocations associated with a single size.
 *
 * Several pointers are used to track the stack.  To save on metadata bytes,
 * only the stack_head is a full sized pointer (which is dereferenced on the
 * fastpath), while the others store only the low 16 bits -- this is correct
 * because a single stack never takes more space than 2^16 bytes, and at the
 * same time only equality checks are performed on the low bits.
 *
 * (low addr)                                                  (high addr)
 * |------stashed------|------available------|------cached-----|
 * ^                   ^                     ^                 ^
 * low_bound(derived)  low_bits_full         stack_head        low_bits_empty
 */
typedef struct cache_bin_s cache_bin_t;
struct cache_bin_s {
	/*
	 * The stack grows down.  Whenever the bin is nonempty, the head points
	 * to an array entry containing a valid allocation.  When it is empty,
	 * the head points to one element past the owned array.
	 */
	void **stack_head;
	/*
	 * cur_ptr and stats are both modified frequently.  Let's keep them
	 * close so that they have a higher chance of being on the same
	 * cacheline, thus less write-backs.
	 */
	cache_bin_stats_t tstats;

	/*
	 * The low bits of the address of the first item in the stack that
	 * hasn't been used since the last GC, to track the low water mark (min
	 * # of cached items).
	 *
	 * Since the stack grows down, this is a higher address than
	 * low_bits_full.
	 */
	cache_bin_sz_t low_bits_low_water;

	/*
	 * The low bits of the value that stack_head will take on when the array
	 * is full (of cached & stashed items).  But remember that stack_head
	 * always points to a valid item when the array is nonempty -- this is
	 * in the array.
	 *
	 * Recall that since the stack grows down, this is the lowest available
	 * address in the array for caching.  Only adjusted when stashing items.
	 */
	cache_bin_sz_t low_bits_full;

	/*
	 * The low bits of the value that stack_head will take on when the array
	 * is empty.
	 *
	 * The stack grows down -- this is one past the highest address in the
	 * array.  Immutable after initialization.
	 */
	cache_bin_sz_t low_bits_empty;

	/* The maximum number of cached items in the bin. */
	cache_bin_info_t bin_info;
};

/*
 * The cache_bins live inside the tcache, but the arena (by design) isn't
 * supposed to know much about tcache internals.  To let the arena iterate over
 * associated bins, we keep (with the tcache) a linked list of
 * cache_bin_array_descriptor_ts that tell the arena how to find the bins.
 */
typedef struct cache_bin_array_descriptor_s cache_bin_array_descriptor_t;
struct cache_bin_array_descriptor_s {
	/*
	 * The arena keeps a list of the cache bins associated with it, for
	 * stats collection.
	 */
	ql_elm(cache_bin_array_descriptor_t) link;
	/* Pointers to the tcache bins. */
	cache_bin_t *bins;
};

static inline void
cache_bin_array_descriptor_init(cache_bin_array_descriptor_t *descriptor,
    cache_bin_t *bins) {
	ql_elm_new(descriptor, link);
	descriptor->bins = bins;
}

JEMALLOC_ALWAYS_INLINE bool
cache_bin_nonfast_aligned(const void *ptr) {
	if (!config_uaf_detection) {
		return false;
	}
	/*
	 * Currently we use alignment to decide which pointer to junk & stash on
	 * dealloc (for catching use-after-free).  In some common cases a
	 * page-aligned check is needed already (sdalloc w/ config_prof), so we
	 * are getting it more or less for free -- no added instructions on
	 * free_fastpath.
	 *
	 * Another way of deciding which pointer to sample, is adding another
	 * thread_event to pick one every N bytes.  That also adds no cost on
	 * the fastpath, however it will tend to pick large allocations which is
	 * not the desired behavior.
	 */
	return ((uintptr_t)ptr & san_cache_bin_nonfast_mask) == 0;
}

static inline const void *
cache_bin_disabled_bin_stack(void) {
	return &disabled_bin;
}

/*
 * If a cache bin was zero initialized (either because it lives in static or
 * thread-local storage, or was memset to 0), this function indicates whether or
 * not cache_bin_init was called on it.
 */
static inline bool
cache_bin_still_zero_initialized(cache_bin_t *bin) {
	return bin->stack_head == NULL;
}

static inline bool
cache_bin_disabled(cache_bin_t *bin) {
	bool disabled = (bin->stack_head == cache_bin_disabled_bin_stack());
	if (disabled) {
		assert((uintptr_t)(*bin->stack_head) == JUNK_ADDR);
	}
	return disabled;
}

/* Gets ncached_max without asserting that the bin is enabled. */
static inline cache_bin_sz_t
cache_bin_ncached_max_get_unsafe(cache_bin_t *bin) {
	return bin->bin_info.ncached_max;
}

/* Returns ncached_max: Upper limit on ncached. */
static inline cache_bin_sz_t
cache_bin_ncached_max_get(cache_bin_t *bin) {
	assert(!cache_bin_disabled(bin));
	return cache_bin_ncached_max_get_unsafe(bin);
}

/*
 * Internal.
 *
 * Asserts that the pointer associated with earlier is <= the one associated
 * with later.
 */
static inline void
cache_bin_assert_earlier(cache_bin_t *bin, cache_bin_sz_t earlier, cache_bin_sz_t later) {
	if (earlier > later) {
		assert(bin->low_bits_full > bin->low_bits_empty);
	}
}

/*
 * Internal.
 *
 * Does difference calculations that handle wraparound correctly.  Earlier must
 * be associated with the position earlier in memory.
 */
static inline cache_bin_sz_t
cache_bin_diff(cache_bin_t *bin, cache_bin_sz_t earlier, cache_bin_sz_t later) {
	cache_bin_assert_earlier(bin, earlier, later);
	return later - earlier;
}

/*
 * Number of items currently cached in the bin, without checking ncached_max.
 */
static inline cache_bin_sz_t
cache_bin_ncached_get_internal(cache_bin_t *bin) {
	cache_bin_sz_t diff = cache_bin_diff(bin,
	    (cache_bin_sz_t)(uintptr_t)bin->stack_head, bin->low_bits_empty);
	cache_bin_sz_t n = diff / sizeof(void *);
	/*
	 * We have undefined behavior here; if this function is called from the
	 * arena stats updating code, then stack_head could change from the
	 * first line to the next one.  Morally, these loads should be atomic,
	 * but compilers won't currently generate comparisons with in-memory
	 * operands against atomics, and these variables get accessed on the
	 * fast paths.  This should still be "safe" in the sense of generating
	 * the correct assembly for the foreseeable future, though.
	 */
	assert(n == 0 || *(bin->stack_head) != NULL);
	return n;
}

/*
 * Number of items currently cached in the bin, with checking ncached_max.  The
 * caller must know that no concurrent modification of the cache_bin is
 * possible.
 */
static inline cache_bin_sz_t
cache_bin_ncached_get_local(cache_bin_t *bin) {
	cache_bin_sz_t n = cache_bin_ncached_get_internal(bin);
	assert(n <= cache_bin_ncached_max_get(bin));
	return n;
}

/*
 * Internal.
 *
 * A pointer to the position one past the end of the backing array.
 *
 * Do not call if racy, because both 'bin->stack_head' and 'bin->low_bits_full'
 * are subject to concurrent modifications.
 */
static inline void **
cache_bin_empty_position_get(cache_bin_t *bin) {
	cache_bin_sz_t diff = cache_bin_diff(bin,
	    (cache_bin_sz_t)(uintptr_t)bin->stack_head, bin->low_bits_empty);
	byte_t *empty_bits = (byte_t *)bin->stack_head + diff;
	void **ret = (void **)empty_bits;

	assert(ret >= bin->stack_head);

	return ret;
}

/*
 * Internal.
 *
 * Calculates low bits of the lower bound of the usable cache bin's range (see
 * cache_bin_t visual representation above).
 *
 * No values are concurrently modified, so should be safe to read in a
 * multithreaded environment. Currently concurrent access happens only during
 * arena statistics collection.
 */
static inline cache_bin_sz_t
cache_bin_low_bits_low_bound_get(cache_bin_t *bin) {
	return (cache_bin_sz_t)bin->low_bits_empty -
	    cache_bin_ncached_max_get(bin) * sizeof(void *);
}

/*
 * Internal.
 *
 * A pointer to the position with the lowest address of the backing array.
 */
static inline void **
cache_bin_low_bound_get(cache_bin_t *bin) {
	cache_bin_sz_t ncached_max = cache_bin_ncached_max_get(bin);
	void **ret = cache_bin_empty_position_get(bin) - ncached_max;
	assert(ret <= bin->stack_head);

	return ret;
}

/*
 * As the name implies.  This is important since it's not correct to try to
 * batch fill a nonempty cache bin.
 */
static inline void
cache_bin_assert_empty(cache_bin_t *bin) {
	assert(cache_bin_ncached_get_local(bin) == 0);
	assert(cache_bin_empty_position_get(bin) == bin->stack_head);
}

/*
 * Get low water, but without any of the correctness checking we do for the
 * caller-usable version, if we are temporarily breaking invariants (like
 * ncached >= low_water during flush).
 */
static inline cache_bin_sz_t
cache_bin_low_water_get_internal(cache_bin_t *bin) {
	return cache_bin_diff(bin, bin->low_bits_low_water,
	    bin->low_bits_empty) / sizeof(void *);
}

/* Returns the numeric value of low water in [0, ncached]. */
static inline cache_bin_sz_t
cache_bin_low_water_get(cache_bin_t *bin) {
	cache_bin_sz_t low_water = cache_bin_low_water_get_internal(bin);
	assert(low_water <= cache_bin_ncached_max_get(bin));
	assert(low_water <= cache_bin_ncached_get_local(bin));

	cache_bin_assert_earlier(bin, (cache_bin_sz_t)(uintptr_t)bin->stack_head,
	    bin->low_bits_low_water);

	return low_water;
}

/*
 * Indicates that the current cache bin position should be the low water mark
 * going forward.
 */
static inline void
cache_bin_low_water_set(cache_bin_t *bin) {
	assert(!cache_bin_disabled(bin));
	bin->low_bits_low_water = (cache_bin_sz_t)(uintptr_t)bin->stack_head;
}

static inline void
cache_bin_low_water_adjust(cache_bin_t *bin) {
	assert(!cache_bin_disabled(bin));
	if (cache_bin_ncached_get_internal(bin)
	    < cache_bin_low_water_get_internal(bin)) {
		cache_bin_low_water_set(bin);
	}
}

JEMALLOC_ALWAYS_INLINE void *
cache_bin_alloc_impl(cache_bin_t *bin, bool *success, bool adjust_low_water) {
	/*
	 * success (instead of ret) should be checked upon the return of this
	 * function.  We avoid checking (ret == NULL) because there is never a
	 * null stored on the avail stack (which is unknown to the compiler),
	 * and eagerly checking ret would cause pipeline stall (waiting for the
	 * cacheline).
	 */

	/*
	 * This may read from the empty position; however the loaded value won't
	 * be used.  It's safe because the stack has one more slot reserved.
	 */
	void *ret = *bin->stack_head;
	cache_bin_sz_t low_bits = (cache_bin_sz_t)(uintptr_t)bin->stack_head;
	void **new_head = bin->stack_head + 1;

	/*
	 * Note that the low water mark is at most empty; if we pass this check,
	 * we know we're non-empty.
	 */
	if (likely(low_bits != bin->low_bits_low_water)) {
		bin->stack_head = new_head;
		*success = true;
		return ret;
	}
	if (!adjust_low_water) {
		*success = false;
		return NULL;
	}
	/*
	 * In the fast-path case where we call alloc_easy and then alloc, the
	 * previous checking and computation is optimized away -- we didn't
	 * actually commit any of our operations.
	 */
	if (likely(low_bits != bin->low_bits_empty)) {
		bin->stack_head = new_head;
		bin->low_bits_low_water = (cache_bin_sz_t)(uintptr_t)new_head;
		*success = true;
		return ret;
	}
	*success = false;
	return NULL;
}

/*
 * Allocate an item out of the bin, failing if we're at the low-water mark.
 */
JEMALLOC_ALWAYS_INLINE void *
cache_bin_alloc_easy(cache_bin_t *bin, bool *success) {
	/* We don't look at info if we're not adjusting low-water. */
	return cache_bin_alloc_impl(bin, success, false);
}

/*
 * Allocate an item out of the bin, even if we're currently at the low-water
 * mark (and failing only if the bin is empty).
 */
JEMALLOC_ALWAYS_INLINE void *
cache_bin_alloc(cache_bin_t *bin, bool *success) {
	return cache_bin_alloc_impl(bin, success, true);
}

JEMALLOC_ALWAYS_INLINE cache_bin_sz_t
cache_bin_alloc_batch(cache_bin_t *bin, size_t num, void **out) {
	cache_bin_sz_t n = cache_bin_ncached_get_internal(bin);
	if (n > num) {
		n = (cache_bin_sz_t)num;
	}
	memcpy(out, bin->stack_head, n * sizeof(void *));
	bin->stack_head += n;
	cache_bin_low_water_adjust(bin);

	return n;
}

JEMALLOC_ALWAYS_INLINE bool
cache_bin_full(cache_bin_t *bin) {
	return ((cache_bin_sz_t)(uintptr_t)bin->stack_head == bin->low_bits_full);
}

/*
 * Scans the allocated area of the cache_bin for the given pointer up to limit.
 * Fires safety_check_fail if the ptr is found and returns true.
 */
JEMALLOC_ALWAYS_INLINE bool
cache_bin_dalloc_safety_checks(cache_bin_t *bin, void *ptr) {
	if (!config_debug || opt_debug_double_free_max_scan == 0) {
		return false;
	}

	cache_bin_sz_t ncached = cache_bin_ncached_get_internal(bin);
	unsigned max_scan = opt_debug_double_free_max_scan < ncached
	    ? opt_debug_double_free_max_scan
	    : ncached;

	void **cur = bin->stack_head;
	void **limit = cur + max_scan;
	for (; cur < limit; cur++) {
		if (*cur == ptr) {
			safety_check_fail(
			    "Invalid deallocation detected: double free of "
			    "pointer %p\n",
			    ptr);
			return true;
		}
	}
	return false;
}

/*
 * Free an object into the given bin.  Fails only if the bin is full.
 */
JEMALLOC_ALWAYS_INLINE bool
cache_bin_dalloc_easy(cache_bin_t *bin, void *ptr) {
	if (unlikely(cache_bin_full(bin))) {
		return false;
	}

	if (unlikely(cache_bin_dalloc_safety_checks(bin, ptr))) {
		return true;
	}

	bin->stack_head--;
	*bin->stack_head = ptr;
	cache_bin_assert_earlier(bin, bin->low_bits_full,
	    (cache_bin_sz_t)(uintptr_t)bin->stack_head);

	return true;
}

/* Returns false if failed to stash (i.e. bin is full). */
JEMALLOC_ALWAYS_INLINE bool
cache_bin_stash(cache_bin_t *bin, void *ptr) {
	if (cache_bin_full(bin)) {
		return false;
	}

	/* Stash at the full position, in the [full, head) range. */
	cache_bin_sz_t low_bits_head = (cache_bin_sz_t)(uintptr_t)bin->stack_head;
	/* Wraparound handled as well. */
	cache_bin_sz_t diff = cache_bin_diff(bin, bin->low_bits_full, low_bits_head);
	*(void **)((byte_t *)bin->stack_head - diff) = ptr;

	assert(!cache_bin_full(bin));
	bin->low_bits_full += sizeof(void *);
	cache_bin_assert_earlier(bin, bin->low_bits_full, low_bits_head);

	return true;
}

/* Get the number of stashed pointers. */
JEMALLOC_ALWAYS_INLINE cache_bin_sz_t
cache_bin_nstashed_get_internal(cache_bin_t *bin) {
	cache_bin_sz_t ncached_max = cache_bin_ncached_max_get(bin);
	cache_bin_sz_t low_bits_low_bound = cache_bin_low_bits_low_bound_get(bin);

	cache_bin_sz_t n = cache_bin_diff(bin, low_bits_low_bound,
	    bin->low_bits_full) / sizeof(void *);
	assert(n <= ncached_max);
	if (config_debug && n != 0) {
		/* Below are for assertions only. */
		void **low_bound = cache_bin_low_bound_get(bin);

		assert((cache_bin_sz_t)(uintptr_t)low_bound == low_bits_low_bound);
		void *stashed = *(low_bound + n - 1);
		bool aligned = cache_bin_nonfast_aligned(stashed);
#ifdef JEMALLOC_JET
		/* Allow arbitrary pointers to be stashed in tests. */
		aligned = true;
#endif
		assert(stashed != NULL && aligned);
	}

	return n;
}

JEMALLOC_ALWAYS_INLINE cache_bin_sz_t
cache_bin_nstashed_get_local(cache_bin_t *bin) {
	cache_bin_sz_t n = cache_bin_nstashed_get_internal(bin);
	assert(n <= cache_bin_ncached_max_get(bin));
	return n;
}

/*
 * Obtain a racy view of the number of items currently in the cache bin, in the
 * presence of possible concurrent modifications.
 *
 * Note that this is the only racy function in this header.  Any other functions
 * are assumed to be non-racy.  The "racy" term here means accessed from another
 * thread (that is not the owner of the specific cache bin).  This only happens
 * when gathering stats (read-only).  The only change because of the racy
 * condition is that assertions based on mutable fields are omitted.
 *
 * It's important to keep in mind that 'bin->stack_head' and
 * 'bin->low_bits_full' can be modified concurrently and almost no assertions
 * about their values can be made.
 *
 * This function should not call other utility functions because the racy
 * condition may cause unexpected / undefined behaviors in unverified utility
 * functions.  Currently, this function calls two utility functions
 * cache_bin_ncached_max_get and cache_bin_low_bits_low_bound_get because
 * they help access values that will not be concurrently modified.
 */
static inline void
cache_bin_nitems_get_remote(cache_bin_t *bin, cache_bin_sz_t *ncached,
    cache_bin_sz_t *nstashed) {
	/* Racy version of cache_bin_ncached_get_internal. */
	cache_bin_sz_t diff = bin->low_bits_empty -
	    (cache_bin_sz_t)(uintptr_t)bin->stack_head;
	cache_bin_sz_t n = diff / sizeof(void *);
	*ncached = n;

	/* Racy version of cache_bin_nstashed_get_internal. */
	cache_bin_sz_t low_bits_low_bound = cache_bin_low_bits_low_bound_get(bin);
	n = (bin->low_bits_full - low_bits_low_bound) / sizeof(void *);
	*nstashed = n;
	/*
	 * Note that cannot assert anything regarding ncached_max because
	 * it can be configured on the fly and is thus racy.
	 */
}

/*
 * For small bins, used to calculate how many items to fill at a time.
 * The final nfill is calculated by (ncached_max >> (base - offset)).
 */
typedef struct cache_bin_fill_ctl_s cache_bin_fill_ctl_t;
struct cache_bin_fill_ctl_s {
	uint8_t base;
	uint8_t offset;
};

/*
 * Limit how many items can be flushed in a batch (Which is the upper bound
 * for the nflush parameter in tcache_bin_flush_impl()).
 * This is to avoid stack overflow when we do batch edata look up, which
 * reserves a nflush * sizeof(emap_batch_lookup_result_t) stack variable.
 */
#define CACHE_BIN_NFLUSH_BATCH_MAX ((VARIABLE_ARRAY_SIZE_MAX >> LG_SIZEOF_PTR) - 1)

/*
 * Filling and flushing are done in batch, on arrays of void *s.  For filling,
 * the arrays go forward, and can be accessed with ordinary array arithmetic.
 * For flushing, we work from the end backwards, and so need to use special
 * accessors that invert the usual ordering.
 *
 * This is important for maintaining first-fit; the arena code fills with
 * earliest objects first, and so those are the ones we should return first for
 * cache_bin_alloc calls.  When flushing, we should flush the objects that we
 * wish to return later; those at the end of the array.  This is better for the
 * first-fit heuristic as well as for cache locality; the most recently freed
 * objects are the ones most likely to still be in cache.
 *
 * This all sounds very hand-wavey and theoretical, but reverting the ordering
 * on one or the other pathway leads to measurable slowdowns.
 */

typedef struct cache_bin_ptr_array_s cache_bin_ptr_array_t;
struct cache_bin_ptr_array_s {
	cache_bin_sz_t n;
	void **ptr;
};

/*
 * Declare a cache_bin_ptr_array_t sufficient for nval items.
 *
 * In the current implementation, this could be just part of a
 * cache_bin_ptr_array_init_... call, since we reuse the cache bin stack memory.
 * Indirecting behind a macro, though, means experimenting with linked-list
 * representations is easy (since they'll require an alloca in the calling
 * frame).
 */
#define CACHE_BIN_PTR_ARRAY_DECLARE(name, nval)				\
    cache_bin_ptr_array_t name;						\
    name.n = (nval)

/*
 * Start a fill.  The bin must be empty, and This must be followed by a
 * finish_fill call before doing any alloc/dalloc operations on the bin.
 */
static inline void
cache_bin_init_ptr_array_for_fill(cache_bin_t *bin, cache_bin_ptr_array_t *arr,
    cache_bin_sz_t nfill) {
	cache_bin_assert_empty(bin);
	arr->ptr = cache_bin_empty_position_get(bin) - nfill;
}

/*
 * While nfill in cache_bin_init_ptr_array_for_fill is the number we *intend* to
 * fill, nfilled here is the number we actually filled (which may be less, in
 * case of OOM.
 */
static inline void
cache_bin_finish_fill(cache_bin_t *bin, cache_bin_ptr_array_t *arr,
    cache_bin_sz_t nfilled) {
	cache_bin_assert_empty(bin);
	void **empty_position = cache_bin_empty_position_get(bin);
	if (nfilled < arr->n) {
		memmove(empty_position - nfilled, empty_position - arr->n,
		    nfilled * sizeof(void *));
	}
	bin->stack_head = empty_position - nfilled;
}

/*
 * Same deal, but with flush.  Unlike fill (which can fail), the user must flush
 * everything we give them.
 */
static inline void
cache_bin_init_ptr_array_for_flush(cache_bin_t *bin,
    cache_bin_ptr_array_t *arr, cache_bin_sz_t nflush) {
	arr->ptr = cache_bin_empty_position_get(bin) - nflush;
	assert(cache_bin_ncached_get_local(bin) == 0
	    || *arr->ptr != NULL);
}

static inline void
cache_bin_finish_flush(cache_bin_t *bin, cache_bin_ptr_array_t *arr,
    cache_bin_sz_t nflushed) {
	unsigned rem = cache_bin_ncached_get_local(bin) - nflushed;
	memmove(bin->stack_head + nflushed, bin->stack_head,
	    rem * sizeof(void *));
	bin->stack_head += nflushed;
	cache_bin_low_water_adjust(bin);
}

static inline void
cache_bin_init_ptr_array_for_stashed(cache_bin_t *bin, szind_t binind,
    cache_bin_ptr_array_t *arr, cache_bin_sz_t nstashed) {
	assert(nstashed > 0);
	assert(cache_bin_nstashed_get_local(bin) == nstashed);

	void **low_bound = cache_bin_low_bound_get(bin);
	arr->ptr = low_bound;
	assert(*arr->ptr != NULL);
}

static inline void
cache_bin_finish_flush_stashed(cache_bin_t *bin) {
	void **low_bound = cache_bin_low_bound_get(bin);

	/* Reset the bin local full position. */
	bin->low_bits_full = (uint16_t)(uintptr_t)low_bound;
	assert(cache_bin_nstashed_get_local(bin) == 0);
}

/*
 * Initialize a cache_bin_info to represent up to the given number of items in
 * the cache_bins it is associated with.
 */
void cache_bin_info_init(cache_bin_info_t *bin_info,
    cache_bin_sz_t ncached_max);
/*
 * Given an array of initialized cache_bin_info_ts, determine how big an
 * allocation is required to initialize a full set of cache_bin_ts.
 */
void cache_bin_info_compute_alloc(const cache_bin_info_t *infos,
    szind_t ninfos, size_t *size, size_t *alignment);

/*
 * Actually initialize some cache bins.  Callers should allocate the backing
 * memory indicated by a call to cache_bin_compute_alloc.  They should then
 * preincrement, call init once for each bin and info, and then call
 * cache_bin_postincrement.  *alloc_cur will then point immediately past the end
 * of the allocation.
 */
void cache_bin_preincrement(const cache_bin_info_t *infos, szind_t ninfos,
    void *alloc, size_t *cur_offset);
void cache_bin_postincrement(void *alloc, size_t *cur_offset);
void cache_bin_init(cache_bin_t *bin, const cache_bin_info_t *info,
    void *alloc, size_t *cur_offset);
void cache_bin_init_disabled(cache_bin_t *bin, cache_bin_sz_t ncached_max);

bool cache_bin_stack_use_thp(void);

#endif /* JEMALLOC_INTERNAL_CACHE_BIN_H */
