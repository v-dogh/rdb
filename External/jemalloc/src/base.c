#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/assert.h"
#include "jemalloc/internal/extent_mmap.h"
#include "jemalloc/internal/mutex.h"
#include "jemalloc/internal/sz.h"

/*
 * In auto mode, arenas switch to huge pages for the base allocator on the
 * second base block.  a0 switches to thp on the 5th block (after 20 megabytes
 * of metadata), since more metadata (e.g. rtree nodes) come from a0's base.
 */

#define BASE_AUTO_THP_THRESHOLD    2
#define BASE_AUTO_THP_THRESHOLD_A0 5

/******************************************************************************/
/* Data. */

static base_t *b0;

metadata_thp_mode_t opt_metadata_thp = METADATA_THP_DEFAULT;

const char *const metadata_thp_mode_names[] = {
	"disabled",
	"auto",
	"always"
};

/******************************************************************************/

static inline bool
metadata_thp_madvise(void) {
	return (metadata_thp_enabled() &&
	    (init_system_thp_mode == thp_mode_default));
}

static void *
base_map(tsdn_t *tsdn, ehooks_t *ehooks, unsigned ind, size_t size) {
	void *addr;
	bool zero = true;
	bool commit = true;

	/*
	 * Use huge page sizes and alignment when opt_metadata_thp is enabled
	 * or auto.
	 */
	size_t alignment;
	if (opt_metadata_thp == metadata_thp_disabled) {
		alignment = BASE_BLOCK_MIN_ALIGN;
	} else {
		assert(size == HUGEPAGE_CEILING(size));
		alignment = HUGEPAGE;
	}
	if (ehooks_are_default(ehooks)) {
		addr = extent_alloc_mmap(NULL, size, alignment, &zero, &commit);
	} else {
		addr = ehooks_alloc(tsdn, ehooks, NULL, size, alignment, &zero,
		    &commit);
	}

	return addr;
}

static void
base_unmap(tsdn_t *tsdn, ehooks_t *ehooks, unsigned ind, void *addr,
    size_t size) {
	/*
	 * Cascade through dalloc, decommit, purge_forced, and purge_lazy,
	 * stopping at first success.  This cascade is performed for consistency
	 * with the cascade in extent_dalloc_wrapper() because an application's
	 * custom hooks may not support e.g. dalloc.  This function is only ever
	 * called as a side effect of arena destruction, so although it might
	 * seem pointless to do anything besides dalloc here, the application
	 * may in fact want the end state of all associated virtual memory to be
	 * in some consistent-but-allocated state.
	 */
	if (ehooks_are_default(ehooks)) {
		if (!extent_dalloc_mmap(addr, size)) {
			goto label_done;
		}
		if (!pages_decommit(addr, size)) {
			goto label_done;
		}
		if (!pages_purge_forced(addr, size)) {
			goto label_done;
		}
		if (!pages_purge_lazy(addr, size)) {
			goto label_done;
		}
		/* Nothing worked.  This should never happen. */
		not_reached();
	} else {
		if (!ehooks_dalloc(tsdn, ehooks, addr, size, true)) {
			goto label_done;
		}
		if (!ehooks_decommit(tsdn, ehooks, addr, size, 0, size)) {
			goto label_done;
		}
		if (!ehooks_purge_forced(tsdn, ehooks, addr, size, 0, size)) {
			goto label_done;
		}
		if (!ehooks_purge_lazy(tsdn, ehooks, addr, size, 0, size)) {
			goto label_done;
		}
		/* Nothing worked.  That's the application's problem. */
	}
label_done:
	if (metadata_thp_madvise()) {
		/* Set NOHUGEPAGE after unmap to avoid kernel defrag. */
		assert(((uintptr_t)addr & HUGEPAGE_MASK) == 0 &&
		    (size & HUGEPAGE_MASK) == 0);
		pages_nohuge(addr, size);
	}
}

static inline bool
base_edata_is_reused(edata_t *edata) {
	/*
	 * Borrow the guarded bit to indicate if the extent is a recycled one,
	 * i.e. the ones returned to base for reuse; currently only tcache bin
	 * stacks.  Skips stats updating if so (needed for this purpose only).
	 */
	return edata_guarded_get(edata);
}

static void
base_edata_init(size_t *extent_sn_next, edata_t *edata, void *addr,
    size_t size) {
	size_t sn;

	sn = *extent_sn_next;
	(*extent_sn_next)++;

	edata_binit(edata, addr, size, sn, false /* is_reused */);
}

static size_t
base_get_num_blocks(base_t *base, bool with_new_block) {
	base_block_t *b = base->blocks;
	assert(b != NULL);

	size_t n_blocks = with_new_block ? 2 : 1;
	while (b->next != NULL) {
		n_blocks++;
		b = b->next;
	}

	return n_blocks;
}

static void
huge_arena_auto_thp_switch(tsdn_t *tsdn, pac_thp_t *pac_thp) {
	assert(opt_huge_arena_pac_thp);
#ifdef JEMALLOC_JET
	if (pac_thp->auto_thp_switched) {
		return;
	}
#else
	/*
	 * The switch should be turned on only once when the b0 auto thp switch is
	 * turned on, unless it's a unit test where b0 gets deleted and then
	 * recreated.
	 */
	assert(!pac_thp->auto_thp_switched);
#endif

	edata_list_active_t *pending_list;
	malloc_mutex_lock(tsdn, &pac_thp->lock);
	pending_list = &pac_thp->thp_lazy_list;
	pac_thp->auto_thp_switched = true;
	malloc_mutex_unlock(tsdn, &pac_thp->lock);

	unsigned cnt = 0;
	edata_t *edata;
	ql_foreach(edata, &pending_list->head, ql_link_active) {
		assert(edata != NULL);
		void *addr = edata_addr_get(edata);
		size_t size = edata_size_get(edata);
		assert(HUGEPAGE_ADDR2BASE(addr) == addr);
		assert(HUGEPAGE_CEILING(size) == size && size != 0);
		pages_huge(addr, size);
		cnt++;
	}
	assert(cnt == atomic_load_u(&pac_thp->n_thp_lazy, ATOMIC_RELAXED));
}

static void
base_auto_thp_switch(tsdn_t *tsdn, base_t *base) {
	assert(opt_metadata_thp == metadata_thp_auto);
	malloc_mutex_assert_owner(tsdn, &base->mtx);
	if (base->auto_thp_switched) {
		return;
	}
	/* Called when adding a new block. */
	bool should_switch;
	if (base_ind_get(base) != 0) {
		should_switch = (base_get_num_blocks(base, true) ==
		    BASE_AUTO_THP_THRESHOLD);
	} else {
		should_switch = (base_get_num_blocks(base, true) ==
		    BASE_AUTO_THP_THRESHOLD_A0);
	}
	if (!should_switch) {
		return;
	}

	base->auto_thp_switched = true;
	assert(!config_stats || base->n_thp == 0);
	/* Make the initial blocks THP lazily. */
	base_block_t *block = base->blocks;
	while (block != NULL) {
		assert((block->size & HUGEPAGE_MASK) == 0);
		pages_huge(block, block->size);
		if (config_stats) {
			base->n_thp += HUGEPAGE_CEILING(block->size -
			    edata_bsize_get(&block->edata)) >> LG_HUGEPAGE;
		}
		block = block->next;
		assert(block == NULL || (base_ind_get(base) == 0));
	}

	/* Handle the THP auto switch for the huge arena. */
	if (!huge_arena_pac_thp.thp_madvise || base_ind_get(base) != 0) {
		/*
		 * The huge arena THP auto-switch is triggered only by b0 switch,
		 * provided that the huge arena is initialized. If b0 switch is enabled
		 * before huge arena is ready, the huge arena switch will be enabled
		 * during huge_arena_pac_thp initialization.
		 */
		return;
	}
	/*
	 * thp_madvise above is by default false and set in arena_init_huge() with
	 * b0 mtx held. So if we reach here, it means the entire huge_arena_pac_thp
	 * is initialized and we can safely switch the THP.
	 */
	malloc_mutex_unlock(tsdn, &base->mtx);
	huge_arena_auto_thp_switch(tsdn, &huge_arena_pac_thp);
	malloc_mutex_lock(tsdn, &base->mtx);
}

static void *
base_extent_bump_alloc_helper(edata_t *edata, size_t *gap_size, size_t size,
    size_t alignment) {
	void *ret;

	assert(alignment == ALIGNMENT_CEILING(alignment, QUANTUM));
	assert(size == ALIGNMENT_CEILING(size, alignment));

	*gap_size = ALIGNMENT_CEILING((uintptr_t)edata_addr_get(edata),
	    alignment) - (uintptr_t)edata_addr_get(edata);
	ret = (void *)((byte_t *)edata_addr_get(edata) + *gap_size);
	assert(edata_bsize_get(edata) >= *gap_size + size);
	edata_binit(edata, (void *)((byte_t *)edata_addr_get(edata) +
	    *gap_size + size), edata_bsize_get(edata) - *gap_size - size,
	    edata_sn_get(edata), base_edata_is_reused(edata));
	return ret;
}

static void
base_edata_heap_insert(tsdn_t *tsdn, base_t *base, edata_t *edata) {
	malloc_mutex_assert_owner(tsdn, &base->mtx);

	size_t bsize = edata_bsize_get(edata);
	assert(bsize > 0);
	/*
	 * Compute the index for the largest size class that does not exceed
	 * extent's size.
	 */
	szind_t index_floor = sz_size2index(bsize + 1) - 1;
	edata_heap_insert(&base->avail[index_floor], edata);
}

/*
 * Only can be called by top-level functions, since it may call base_alloc
 * internally when cache is empty.
 */
static edata_t *
base_alloc_base_edata(tsdn_t *tsdn, base_t *base) {
	edata_t *edata;

	malloc_mutex_lock(tsdn, &base->mtx);
	edata = edata_avail_first(&base->edata_avail);
	if (edata != NULL) {
		edata_avail_remove(&base->edata_avail, edata);
	}
	malloc_mutex_unlock(tsdn, &base->mtx);

	if (edata == NULL) {
		edata = base_alloc_edata(tsdn, base);
	}

	return edata;
}

static void
base_extent_bump_alloc_post(tsdn_t *tsdn, base_t *base, edata_t *edata,
    size_t gap_size, void *addr, size_t size) {
	if (edata_bsize_get(edata) > 0) {
		base_edata_heap_insert(tsdn, base, edata);
	} else {
		/* Freed base edata_t stored in edata_avail. */
		edata_avail_insert(&base->edata_avail, edata);
	}

	if (config_stats && !base_edata_is_reused(edata)) {
		base->allocated += size;
		/*
		 * Add one PAGE to base_resident for every page boundary that is
		 * crossed by the new allocation. Adjust n_thp similarly when
		 * metadata_thp is enabled.
		 */
		base->resident += PAGE_CEILING((uintptr_t)addr + size) -
		    PAGE_CEILING((uintptr_t)addr - gap_size);
		assert(base->allocated <= base->resident);
		assert(base->resident <= base->mapped);
		if (metadata_thp_madvise() && (opt_metadata_thp ==
		    metadata_thp_always || base->auto_thp_switched)) {
			base->n_thp += (HUGEPAGE_CEILING((uintptr_t)addr + size)
			    - HUGEPAGE_CEILING((uintptr_t)addr - gap_size)) >>
			    LG_HUGEPAGE;
			assert(base->mapped >= base->n_thp << LG_HUGEPAGE);
		}
	}
}

static void *
base_extent_bump_alloc(tsdn_t *tsdn, base_t *base, edata_t *edata, size_t size,
    size_t alignment) {
	void *ret;
	size_t gap_size;

	ret = base_extent_bump_alloc_helper(edata, &gap_size, size, alignment);
	base_extent_bump_alloc_post(tsdn, base, edata, gap_size, ret, size);
	return ret;
}

static size_t
base_block_size_ceil(size_t block_size) {
	return opt_metadata_thp == metadata_thp_disabled ?
	    ALIGNMENT_CEILING(block_size, BASE_BLOCK_MIN_ALIGN) :
	    HUGEPAGE_CEILING(block_size);
}

/*
 * Allocate a block of virtual memory that is large enough to start with a
 * base_block_t header, followed by an object of specified size and alignment.
 * On success a pointer to the initialized base_block_t header is returned.
 */
static base_block_t *
base_block_alloc(tsdn_t *tsdn, base_t *base, ehooks_t *ehooks, unsigned ind,
    pszind_t *pind_last, size_t *extent_sn_next, size_t size,
    size_t alignment) {
	alignment = ALIGNMENT_CEILING(alignment, QUANTUM);
	size_t usize = ALIGNMENT_CEILING(size, alignment);
	size_t header_size = sizeof(base_block_t);
	size_t gap_size = ALIGNMENT_CEILING(header_size, alignment) -
	    header_size;
	/*
	 * Create increasingly larger blocks in order to limit the total number
	 * of disjoint virtual memory ranges.  Choose the next size in the page
	 * size class series (skipping size classes that are not a multiple of
	 * HUGEPAGE when using metadata_thp), or a size large enough to satisfy
	 * the requested size and alignment, whichever is larger.
	 */
	size_t min_block_size = base_block_size_ceil(sz_psz2u(header_size +
	    gap_size + usize));
	pszind_t pind_next = (*pind_last + 1 < sz_psz2ind(SC_LARGE_MAXCLASS)) ?
	    *pind_last + 1 : *pind_last;
	size_t next_block_size = base_block_size_ceil(sz_pind2sz(pind_next));
	size_t block_size = (min_block_size > next_block_size) ? min_block_size
	    : next_block_size;
	base_block_t *block = (base_block_t *)base_map(tsdn, ehooks, ind,
	    block_size);
	if (block == NULL) {
		return NULL;
	}

	if (metadata_thp_madvise()) {
		void *addr = (void *)block;
		assert(((uintptr_t)addr & HUGEPAGE_MASK) == 0 &&
		    (block_size & HUGEPAGE_MASK) == 0);
		if (opt_metadata_thp == metadata_thp_always) {
			pages_huge(addr, block_size);
		} else if (opt_metadata_thp == metadata_thp_auto &&
		    base != NULL) {
			/* base != NULL indicates this is not a new base. */
			malloc_mutex_lock(tsdn, &base->mtx);
			base_auto_thp_switch(tsdn, base);
			if (base->auto_thp_switched) {
				pages_huge(addr, block_size);
			}
			malloc_mutex_unlock(tsdn, &base->mtx);
		}
	}

	*pind_last = sz_psz2ind(block_size);
	block->size = block_size;
	block->next = NULL;
	assert(block_size >= header_size);
	base_edata_init(extent_sn_next, &block->edata,
	    (void *)((byte_t *)block + header_size), block_size - header_size);
	return block;
}

/*
 * Allocate an extent that is at least as large as specified size, with
 * specified alignment.
 */
static edata_t *
base_extent_alloc(tsdn_t *tsdn, base_t *base, size_t size, size_t alignment) {
	malloc_mutex_assert_owner(tsdn, &base->mtx);

	ehooks_t *ehooks = base_ehooks_get_for_metadata(base);
	/*
	 * Drop mutex during base_block_alloc(), because an extent hook will be
	 * called.
	 */
	malloc_mutex_unlock(tsdn, &base->mtx);
	base_block_t *block = base_block_alloc(tsdn, base, ehooks,
	    base_ind_get(base), &base->pind_last, &base->extent_sn_next, size,
	    alignment);
	malloc_mutex_lock(tsdn, &base->mtx);
	if (block == NULL) {
		return NULL;
	}
	block->next = base->blocks;
	base->blocks = block;
	if (config_stats) {
		base->allocated += sizeof(base_block_t);
		base->resident += PAGE_CEILING(sizeof(base_block_t));
		base->mapped += block->size;
		if (metadata_thp_madvise() &&
		    !(opt_metadata_thp == metadata_thp_auto
		      && !base->auto_thp_switched)) {
			assert(base->n_thp > 0);
			base->n_thp += HUGEPAGE_CEILING(sizeof(base_block_t)) >>
			    LG_HUGEPAGE;
		}
		assert(base->allocated <= base->resident);
		assert(base->resident <= base->mapped);
		assert(base->n_thp << LG_HUGEPAGE <= base->mapped);
	}
	return &block->edata;
}

base_t *
b0get(void) {
	return b0;
}

base_t *
base_new(tsdn_t *tsdn, unsigned ind, const extent_hooks_t *extent_hooks,
    bool metadata_use_hooks) {
	pszind_t pind_last = 0;
	size_t extent_sn_next = 0;

	/*
	 * The base will contain the ehooks eventually, but it itself is
	 * allocated using them.  So we use some stack ehooks to bootstrap its
	 * memory, and then initialize the ehooks within the base_t.
	 */
	ehooks_t fake_ehooks;
	ehooks_init(&fake_ehooks, metadata_use_hooks ?
	    (extent_hooks_t *)extent_hooks :
	    (extent_hooks_t *)&ehooks_default_extent_hooks, ind);

	base_block_t *block = base_block_alloc(tsdn, NULL, &fake_ehooks, ind,
	    &pind_last, &extent_sn_next, sizeof(base_t), QUANTUM);
	if (block == NULL) {
		return NULL;
	}

	size_t gap_size;
	size_t base_alignment = CACHELINE;
	size_t base_size = ALIGNMENT_CEILING(sizeof(base_t), base_alignment);
	base_t *base = (base_t *)base_extent_bump_alloc_helper(&block->edata,
	    &gap_size, base_size, base_alignment);
	ehooks_init(&base->ehooks, (extent_hooks_t *)extent_hooks, ind);
	ehooks_init(&base->ehooks_base, metadata_use_hooks ?
	    (extent_hooks_t *)extent_hooks :
	    (extent_hooks_t *)&ehooks_default_extent_hooks, ind);
	if (malloc_mutex_init(&base->mtx, "base", WITNESS_RANK_BASE,
	    malloc_mutex_rank_exclusive)) {
		base_unmap(tsdn, &fake_ehooks, ind, block, block->size);
		return NULL;
	}
	base->pind_last = pind_last;
	base->extent_sn_next = extent_sn_next;
	base->blocks = block;
	base->auto_thp_switched = false;
	for (szind_t i = 0; i < SC_NSIZES; i++) {
		edata_heap_new(&base->avail[i]);
	}
	edata_avail_new(&base->edata_avail);

	if (config_stats) {
		base->edata_allocated = 0;
		base->rtree_allocated = 0;
		base->allocated = sizeof(base_block_t);
		base->resident = PAGE_CEILING(sizeof(base_block_t));
		base->mapped = block->size;
		base->n_thp = (opt_metadata_thp == metadata_thp_always) &&
		    metadata_thp_madvise() ? HUGEPAGE_CEILING(sizeof(base_block_t))
		    >> LG_HUGEPAGE : 0;
		assert(base->allocated <= base->resident);
		assert(base->resident <= base->mapped);
		assert(base->n_thp << LG_HUGEPAGE <= base->mapped);
	}

	/* Locking here is only necessary because of assertions. */
	malloc_mutex_lock(tsdn, &base->mtx);
	base_extent_bump_alloc_post(tsdn, base, &block->edata, gap_size, base,
	    base_size);
	malloc_mutex_unlock(tsdn, &base->mtx);

	return base;
}

void
base_delete(tsdn_t *tsdn, base_t *base) {
	ehooks_t *ehooks = base_ehooks_get_for_metadata(base);
	base_block_t *next = base->blocks;
	do {
		base_block_t *block = next;
		next = block->next;
		base_unmap(tsdn, ehooks, base_ind_get(base), block,
		    block->size);
	} while (next != NULL);
}

ehooks_t *
base_ehooks_get(base_t *base) {
	return &base->ehooks;
}

ehooks_t *
base_ehooks_get_for_metadata(base_t *base) {
	return &base->ehooks_base;
}

extent_hooks_t *
base_extent_hooks_set(base_t *base, extent_hooks_t *extent_hooks) {
	extent_hooks_t *old_extent_hooks =
	    ehooks_get_extent_hooks_ptr(&base->ehooks);
	ehooks_init(&base->ehooks, extent_hooks, ehooks_ind_get(&base->ehooks));
	return old_extent_hooks;
}

static void *
base_alloc_impl(tsdn_t *tsdn, base_t *base, size_t size, size_t alignment,
    size_t *esn, size_t *ret_usize) {
	alignment = QUANTUM_CEILING(alignment);
	size_t usize = ALIGNMENT_CEILING(size, alignment);
	size_t asize = usize + alignment - QUANTUM;

	edata_t *edata = NULL;
	malloc_mutex_lock(tsdn, &base->mtx);
	for (szind_t i = sz_size2index(asize); i < SC_NSIZES; i++) {
		edata = edata_heap_remove_first(&base->avail[i]);
		if (edata != NULL) {
			/* Use existing space. */
			break;
		}
	}
	if (edata == NULL) {
		/* Try to allocate more space. */
		edata = base_extent_alloc(tsdn, base, usize, alignment);
	}
	void *ret;
	if (edata == NULL) {
		ret = NULL;
		goto label_return;
	}

	ret = base_extent_bump_alloc(tsdn, base, edata, usize, alignment);
	if (esn != NULL) {
		*esn = (size_t)edata_sn_get(edata);
	}
	if (ret_usize != NULL) {
		*ret_usize = usize;
	}
label_return:
	malloc_mutex_unlock(tsdn, &base->mtx);
	return ret;
}

/*
 * base_alloc() returns zeroed memory, which is always demand-zeroed for the
 * auto arenas, in order to make multi-page sparse data structures such as radix
 * tree nodes efficient with respect to physical memory usage.  Upon success a
 * pointer to at least size bytes with specified alignment is returned.  Note
 * that size is rounded up to the nearest multiple of alignment to avoid false
 * sharing.
 */
void *
base_alloc(tsdn_t *tsdn, base_t *base, size_t size, size_t alignment) {
	return base_alloc_impl(tsdn, base, size, alignment, NULL, NULL);
}

edata_t *
base_alloc_edata(tsdn_t *tsdn, base_t *base) {
	size_t esn, usize;
	edata_t *edata = base_alloc_impl(tsdn, base, sizeof(edata_t),
	    EDATA_ALIGNMENT, &esn, &usize);
	if (edata == NULL) {
		return NULL;
	}
	if (config_stats) {
		base->edata_allocated += usize;
	}
	edata_esn_set(edata, esn);
	return edata;
}

void *
base_alloc_rtree(tsdn_t *tsdn, base_t *base, size_t size) {
	size_t usize;
	void *rtree = base_alloc_impl(tsdn, base, size, CACHELINE, NULL,
	    &usize);
	if (rtree == NULL) {
		return NULL;
	}
	if (config_stats) {
		base->rtree_allocated += usize;
	}
	return rtree;
}

static inline void
b0_alloc_header_size(size_t *header_size, size_t *alignment) {
	*alignment = QUANTUM;
	*header_size = QUANTUM > sizeof(edata_t *) ? QUANTUM :
	    sizeof(edata_t *);
}

/*
 * Each piece allocated here is managed by a separate edata, because it was bump
 * allocated and cannot be merged back into the original base_block.  This means
 * it's not for general purpose: 1) they are not page aligned, nor page sized,
 * and 2) the requested size should not be too small (as each piece comes with
 * an edata_t).  Only used for tcache bin stack allocation now.
 */
void *
b0_alloc_tcache_stack(tsdn_t *tsdn, size_t stack_size) {
	base_t *base = b0get();
	edata_t *edata = base_alloc_base_edata(tsdn, base);
	if (edata == NULL) {
		return NULL;
	}

	/*
	 * Reserve room for the header, which stores a pointer to the managing
	 * edata_t.  The header itself is located right before the return
	 * address, so that edata can be retrieved on dalloc.  Bump up to usize
	 * to improve reusability -- otherwise the freed stacks will be put back
	 * into the previous size class.
	 */
	size_t esn, alignment, header_size;
	b0_alloc_header_size(&header_size, &alignment);

	size_t alloc_size = sz_s2u(stack_size + header_size);
	void *addr = base_alloc_impl(tsdn, base, alloc_size, alignment, &esn,
	    NULL);
	if (addr == NULL) {
		edata_avail_insert(&base->edata_avail, edata);
		return NULL;
	}

	/* Set is_reused: see comments in base_edata_is_reused. */
	edata_binit(edata, addr, alloc_size, esn, true /* is_reused */);
	*(edata_t **)addr = edata;

	return (byte_t *)addr + header_size;
}

void
b0_dalloc_tcache_stack(tsdn_t *tsdn, void *tcache_stack) {
	/* edata_t pointer stored in header. */
	size_t alignment, header_size;
	b0_alloc_header_size(&header_size, &alignment);

	edata_t *edata = *(edata_t **)((byte_t *)tcache_stack - header_size);
	void *addr = edata_addr_get(edata);
	size_t bsize = edata_bsize_get(edata);
	/* Marked as "reused" to avoid double counting stats. */
	assert(base_edata_is_reused(edata));
	assert(addr != NULL && bsize > 0);

	/* Zero out since base_alloc returns zeroed memory. */
	memset(addr, 0, bsize);

	base_t *base = b0get();
	malloc_mutex_lock(tsdn, &base->mtx);
	base_edata_heap_insert(tsdn, base, edata);
	malloc_mutex_unlock(tsdn, &base->mtx);
}

void
base_stats_get(tsdn_t *tsdn, base_t *base, size_t *allocated,
    size_t *edata_allocated, size_t *rtree_allocated, size_t *resident,
    size_t *mapped, size_t *n_thp) {
	cassert(config_stats);

	malloc_mutex_lock(tsdn, &base->mtx);
	assert(base->allocated <= base->resident);
	assert(base->resident <= base->mapped);
	assert(base->edata_allocated + base->rtree_allocated <= base->allocated);
	*allocated = base->allocated;
	*edata_allocated = base->edata_allocated;
	*rtree_allocated = base->rtree_allocated;
	*resident = base->resident;
	*mapped = base->mapped;
	*n_thp = base->n_thp;
	malloc_mutex_unlock(tsdn, &base->mtx);
}

void
base_prefork(tsdn_t *tsdn, base_t *base) {
	malloc_mutex_prefork(tsdn, &base->mtx);
}

void
base_postfork_parent(tsdn_t *tsdn, base_t *base) {
	malloc_mutex_postfork_parent(tsdn, &base->mtx);
}

void
base_postfork_child(tsdn_t *tsdn, base_t *base) {
	malloc_mutex_postfork_child(tsdn, &base->mtx);
}

bool
base_boot(tsdn_t *tsdn) {
	b0 = base_new(tsdn, 0, (extent_hooks_t *)&ehooks_default_extent_hooks,
	    /* metadata_use_hooks */ true);
	return (b0 == NULL);
}
