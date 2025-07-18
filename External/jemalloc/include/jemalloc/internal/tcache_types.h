#ifndef JEMALLOC_INTERNAL_TCACHE_TYPES_H
#define JEMALLOC_INTERNAL_TCACHE_TYPES_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/sc.h"

typedef struct tcache_slow_s tcache_slow_t;
typedef struct tcache_s tcache_t;
typedef struct tcaches_s tcaches_t;

/* Used in TSD static initializer only. Real init in tsd_tcache_data_init(). */
#define TCACHE_ZERO_INITIALIZER {0}
#define TCACHE_SLOW_ZERO_INITIALIZER {{0}}

/* Used in TSD static initializer only. Will be initialized to opt_tcache. */
#define TCACHE_ENABLED_ZERO_INITIALIZER false

/* Used for explicit tcache only. Means flushed but not destroyed. */
/* NOLINTNEXTLINE(performance-no-int-to-ptr) */
#define TCACHES_ELM_NEED_REINIT ((tcache_t *)(uintptr_t)1)

#define TCACHE_LG_MAXCLASS_LIMIT LG_USIZE_GROW_SLOW_THRESHOLD
#define TCACHE_MAXCLASS_LIMIT ((size_t)1 << TCACHE_LG_MAXCLASS_LIMIT)
#define TCACHE_NBINS_MAX (SC_NBINS + SC_NGROUP *			\
    (TCACHE_LG_MAXCLASS_LIMIT - SC_LG_LARGE_MINCLASS) + 1)
#define TCACHE_GC_NEIGHBOR_LIMIT ((uintptr_t)1 << 21) /* 2M */
#define TCACHE_GC_INTERVAL_NS ((uint64_t)10 * KQU(1000000)) /* 10ms */
#define TCACHE_GC_SMALL_NBINS_MAX ((SC_NBINS > 8) ? (SC_NBINS >> 3) : 1)
#define TCACHE_GC_LARGE_NBINS_MAX 1

#endif /* JEMALLOC_INTERNAL_TCACHE_TYPES_H */
