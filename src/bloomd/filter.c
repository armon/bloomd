#include "spinlock.h"
#include "filter.h"
#include "sbf.h"

/**
 * Representation of a bloom filters
 */
struct bloom_filter {
    bloom_config *config;           // Filter configuration

    int is_proxied;                 // Are we currently proxied
    int dirty;                      // Is the filter dirty
    char *full_path;                // Path to our data

    bloom_sbf *sbf;                 // Underlying SBF

    filter_counters counters;       // Counters
    bloom_spinlock counter_lock;    // Protect the counters
};

/**
 * Initializes a bloom filter wrapper.
 * @arg config The configuration to use
 * @arg filter Output parameter, the new filter
 * @return 0 on success
 */
int init_bloom_filter(bloom_config *config, bloom_filter **filter) {
    return 0;
}

/**
 * Destroys a bloom filter
 * @arg filter The filter to destroy
 * @return 0 on success
 */
int destroy_bloom_filter(bloom_filter *filter) {
    return 0;
}

/**
 * Gets the counters that belong to a filter
 * @arg filter The filter
 * @return A reference to the counters of a filter
 */
filter_counters* bloomf_counters(bloom_filter *filter) {
    return &filter->counters;
}

/**
 * Checks if a filter is currectly mapped into
 * memory or if it is proxied.
 * @return 1 if in-memory, 0 if proxied.
 */
int bloomf_in_memory(bloom_filter *filter) {
    return !(filter->is_proxied);
}

/**
 * Flushes the filter. Idempotent if the
 * filter is proxied or not dirty.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_flush(bloom_filter *filter) {
    return 0;
}

/**
 * Gracefully closes a bloom filter.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_close(bloom_filter *filter) {
    return 0;
}

/**
 * Deletes the bloom filter with
 * extreme prejudice.
 * @arg filter The filter to delete
 * @return 0 on success.
 */
int bloomf_delete(bloom_filter *filter) {
    return 0;
}

/**
 * Checks if the filter contains a given key
 * @arg filter The filter to check
 * @arg key The key to check
 * @return 0 if not contained, 1 if contained.
 */
int bloomf_contains(bloom_filter *filter, char *key) {
    return 0;
}

/**
 * Adds a key to the given filter
 * @arg filter The filter to add to
 * @arg key The key to add
 * @return 0 if not added, 1 if added.
 */
int bloomf_add(bloom_filter *filter, char *key) {
    return 0;
}

/**
 * Gets the maximum capacity of the filter
 * @arg filter The filter to check
 * @return The total capacity of the filter
 */
uint64_t bloomf_capacity(bloom_filter *filter) {
    return 0;
}

/**
 * Gets the current byte size of the filter
 * @arg filter The filter
 * @return The total byte size of the filter
 */
uint64_t bloomf_byte_size() {
    return 0;
}

