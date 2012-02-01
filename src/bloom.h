#ifndef BLOOM_BLOOM_H
#define BLOOM_BLOOM_H
#include "bitmap.h"
#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>

/**
 * We use a magic header to identify the bloom filters.
 */
const uint32_t MAGIC_HEADER = 0xCB1005DD;  // Vaguely like CBLOOMDD
struct bloom_filter_header {
    uint32_t magic;     // Magic 4 bytes
    uint32_t k_num;     // K_num value
    uint64_t count;     // Count of items
    char __buf[496];     // Pad out to 512 bytes
} __attribute__ ((packed));
typedef struct bloom_filter_header bloom_filter_header;

/*
 * This is the struct we use to represent a bloom filter.
 */
typedef struct {
    unsigned char* mmap;            // Starting address of the bitmap region.
    bloom_filter_header *header;   // Pointer to the header in the bitmap region
    bloom_bitmap *map;             // Underlying bitmap
    uint64_t offset;                // The offset size between hash regions
    uint64_t bitmap_size;           // The size of the bitmap to use, minus buffers
    uint64_t *hashes;               // Pre-allocated buffers for the hashes 
} bloom_bloomfilter;


/**
 * Creates a new bloom filter using a given bitmap and k-value.
 * @arg map A bloom_bitmap pointer.
 * @arg k_num The number of hash functions to use.
 * @arg filter The filter to setup
 * @return 0 for success. Negative for error.
 */
int bf_from_bitmap(bloom_bitmap *map, uint32_t k_num, bloom_bloomfilter *filter);

/**
 * Adds a new key to the bloom filter.
 * @arg filter The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int bf_add(bloom_bloomfilter *filter, char* key);

/**
 * Checks the filter for a key
 * @arg filter The filter to check
 * @arg key The key to check 
 * @returns 1 if present, 0 if not present, negative on error.
 */
int bf_contains(bloom_bloomfilter *filter, char* key);

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t bf_size(bloom_bloomfilter *filter);

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int bf_flush(bloom_bloomfilter *filter);

/**
 * Flushes and closes the filter. Does not close the underlying bitmap.
 * @return 0 on success, negative on failure.
 */
int bf_close(bloom_bloomfilter *filter);

#endif

