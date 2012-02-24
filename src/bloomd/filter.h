#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H
#include "config.h"

/**
 * These are the counters
 * that are maintained for each
 * filter.
 */
typedef struct {
    uint64_t get_hits;
    uint64_t get_misses;
    uint64_t set_hits;
    uint64_t set_misses;
    uint64_t page_ins;
    uint64_t page_outs;
} filter_counters;

typedef struct bloom_filter bloom_filter;

/**
 * Initializes a bloom filter wrapper.
 * @arg config The configuration to use
 * @arg filter Output parameter, the new filter
 * @return 0 on success
 */
int init_bloom_filter(bloom_config *config, char *filter_name, int discover, bloom_filter **filter);

/**
 * Destroys a bloom filter
 * @arg filter The filter to destroy
 * @return 0 on success
 */
int destroy_bloom_filter(bloom_filter *filter);

/**
 * Gets the counters that belong to a filter
 * @arg filter The filter
 * @return A reference to the counters of a filter
 */
filter_counters* bloomf_counters(bloom_filter *filter);

/**
 * Checks if a filter is currectly mapped into
 * memory or if it is proxied.
 * @return 1 if in-memory, 0 if proxied.
 */
int bloomf_in_memory(bloom_filter *filter);

/**
 * Flushes the filter. Idempotent if the
 * filter is proxied or not dirty.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_flush(bloom_filter *filter);

/**
 * Gracefully closes a bloom filter.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_close(bloom_filter *filter);

/**
 * Deletes the bloom filter with
 * extreme prejudice.
 * @arg filter The filter to delete
 * @return 0 on success.
 */
int bloomf_delete(bloom_filter *filter);

/**
 * Checks if the filter contains a given key
 * @arg filter The filter to check
 * @arg key The key to check
 * @return 0 if not contained, 1 if contained.
 */
int bloomf_contains(bloom_filter *filter, char *key);

/**
 * Adds a key to the given filter
 * @arg filter The filter to add to
 * @arg key The key to add
 * @return 0 if not added, 1 if added.
 */
int bloomf_add(bloom_filter *filter, char *key);

/**
 * Gets the size of the filter in keys
 * @arg filter The filter to check
 * @return The total size of the filter
 */
uint64_t bloomf_size(bloom_filter *filter);

/**
 * Gets the maximum capacity of the filter
 * @arg filter The filter to check
 * @return The total capacity of the filter
 */
uint64_t bloomf_capacity(bloom_filter *filter);

/**
 * Gets the current byte size of the filter
 * @arg filter The filter
 * @return The total byte size of the filter
 */
uint64_t bloomf_byte_size(bloom_filter *filter);

#endif
