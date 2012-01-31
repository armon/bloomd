#ifndef CBLOOM_BLOOM_H
#define CBLOOM_BLOOM_H
#include "bitmap.h"
#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>

/**
 * We use a magic header to see if the bloom filter is
 * new-style, or if it is a legacy filter.
 */
const uint32_t MAGIC_V2_HEADER = 0xCB1005DD;  // Vaguely like CBLOOMDD
struct cbloom_filter_header {
    uint32_t magic;     // Magic 4 bytes
    uint32_t k_num;     // K_num value
    uint64_t count;     // Count of items
    char __buf[496];     // Pad out to 512 bytes
} __attribute__ ((packed));
typedef struct cbloom_filter_header cbloom_filter_header;

struct cbloom_filter_legacy_header {
    uint64_t count;     // Count of items
    uint32_t k_num;     // K_num value
} __attribute__ ((packed));
typedef struct cbloom_filter_legacy_header cbloom_filter_legacy_header;

/*
 * This is the struct we use to represent a bloom filter.
 * We use unions to be flexible to using legacy filters.
 */
typedef struct {
    cbloom_bitmap *map;     // Underlying bitmap
    uint64_t offset;        // The offset size between hash regions
    bool legacy;            // If true, we are in legacy mode.
    union {          // Pointers to the headers
        cbloom_filter_header *header;
        cbloom_filter_legacy_header *legacy_header;
    } header;
    union {          // Buffer to store the hashes in
        uint64_t *hashes;      
        uint32_t *legacy_hash;
    } hashes;
    uint64_t bitmap_size;   // The size of the bitmap to use, minus buffers
    unsigned char* mmap;    // Starting address of the bitmap region. Different based on header.
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
uint64_t bloomFilterSize(cbloom_bloomfilter *filter);

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int bloomFilterFlush(cbloom_bloomfilter *filter);

/**
 * Flushes and closes the filter. Does not close the underlying bitmap.
 * @return 0 on success, negative on failure.
 */
int bloomFilterClose(cbloom_bloomfilter *filter);

#endif

