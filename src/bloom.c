#include "bloom.h"

/**
 * Creates a new bloom filter using a given bitmap and k-value.
 * @arg map A cbloom_bitmap pointer.
 * @arg k_num The number of hash functions to use.
 * @arg filter The filter to setup
 * @return 0 for success. Negative for error.
 */
int bf_from_bitmap(cbloom_bitmap *map, uint32_t k_num, cbloom_bloomfilter *filter) {
    return 0;
}

/**
 * Adds a new key to the bloom filter.
 * @arg filter The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int bf_add(cbloom_bloomfilter *filter, char* key) {
    return 0;
}

/**
 * Checks the filter for a key
 * @arg filter The filter to check
 * @arg key The key to check 
 * @returns 1 if present, 0 if not present, negative on error.
 */
int bf_contains(cbloom_bloomfilter *filter, char* key) {
    return 0;
}

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t bf_size(cbloom_bloomfilter *filter) {
    return 0;
}

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int bf_flush(cbloom_bloomfilter *filter) {
    return 0;
}

/**
 * Flushes and closes the filter. Does not close the underlying bitmap.
 * @return 0 on success, negative on failure.
 */
int bf_close(cbloom_bloomfilter *filter) {
    return 0;
}

