#ifndef CBLOOM_BLOOM_H
#define CBLOOM_BLOOM_H
#include "bitmap.h"

typedef struct {
    size_t bitmap_size;  // The size of the bitmap to use, minus buffers
    size_t k_num;        // The number of hash functions to use
    size_t offset;       // The offset size between hash regions
    size_t count;        // The number of items set
    cbloom_bitmap *map;   // Underlying bitmap
    size_t *hashes;      // Buffer to store the hashes in
} cbloom_bloomfilter;

/**
 * Creates a new bloom filter using a given bitmap and k-value.
 * @arg map A cbloom_bitmap pointer.
 * @arg k_num The number of hash functions to use.
 */
cbloom_bloomfilter *createBloomFilter(cbloom_bitmap *map, size_t k_num);

/**
 * Adds a new key to the bloom filter.
 * @arg filter The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int bloomFilterAdd(cbloom_bloomfilter *filter, char* key);

/**
 * Checks the filter for a key
 * @arg filter The filter to check
 * @arg key The key to check 
 * @returns 1 if present, 0 if not present, negative on error.
 */
int bloomFilterContains(cbloom_bloomfilter *filter, char* key);

/**
 * Returns the size of the bloom filter in item count
 */
size_t bloomFilterSize(cbloom_bloomfilter *filter);

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int flush(cbloom_bloomfilter *filter);

/**
 * Flushes and closes the filter. Does not close the underlying bitmap.
 * @return 0 on success, negative on failure.
 */
int close(cbloom_bloomfilter *filter);

#endif

