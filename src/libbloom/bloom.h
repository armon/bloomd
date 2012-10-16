#ifndef BLOOM_BLOOM_H
#define BLOOM_BLOOM_H
#include <stdlib.h>
#include <inttypes.h>
#include <stdbool.h>
#include <errno.h>
#include "bitmap.h"

/**
 * We use a magic header to identify the bloom filters.
 */
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
    bloom_filter_header *header;   // Pointer to the header in the bitmap region
    bloom_bitmap *map;             // Underlying bitmap
    uint64_t offset;                // The offset size between hash regions
    uint64_t bitmap_size;           // The size of the bitmap to use, minus buffers
} bloom_bloomfilter;

/*
 * Structure used to store the parameter information
 * for configuring bloom filters.
 */
typedef struct {
    uint64_t bytes;
    uint32_t k_num;
    uint64_t capacity;
    double   fp_probability;
} bloom_filter_params;


/**
 * Creates a new bloom filter using a given bitmap and k-value.
 * @arg map A bloom_bitmap pointer.
 * @arg k_num The number of hash functions to use. Ignored if the header value is different.
 * @arg new_filter 1 if new, sets the magic byte and does not check it.
 * @arg filter The filter to setup
 * @return 0 for success. Negative for error.
 */
int bf_from_bitmap(bloom_bitmap *map, uint32_t k_num, int new_filter, bloom_bloomfilter *filter);

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
 * Flushes and closes the filter. Closes the underlying bitmap,
 * but does not free it.
 * @return 0 on success, negative on failure.
 */
int bf_close(bloom_bloomfilter *filter);

/*
 * Computes the hashes for a bloom filter
 * @arg k_num the number of hashes to compute
 * @arg key The key to hash
 * @arg hashes Array to write to
 */
void bf_compute_hashes(uint32_t k_num, char *key, uint64_t *hashes);

/*
 * Utility methods for computing parameters
 */

/*
 * Expects capacity and probability to be set,
 * and sets the bytes and k_num that should be used.
 * This byte size accounts for the headers we need.
 * @return 0 on success, negative on error.
 */
int bf_params_for_capacity(bloom_filter_params *params);

/*
 * Expects capacity and probability to be set, computes the
 * minimum byte size required. Does not include header size.
 * @return 0 on success, negative on error.
 */
int bf_size_for_capacity_prob(bloom_filter_params *params);

/*
 * Expects capacity and size to be set, computes the best
 * false positive probability given an ideal k.
 * @return 0 on success, negative on error.
 */
int bf_fp_probability_for_capacity_size(bloom_filter_params *params);

/*
 * Expects bytes and probability to be set,
 * computes the expected capacity.
 * @return 0 on success, negative on error.
 */
int bf_capacity_for_size_prob(bloom_filter_params *params);

/*
 * Expects bytes and capacity to be set,
 * computes the ideal k num.
 * @return 0 on success, negative on error.
 */
int bf_ideal_k_num(bloom_filter_params *params);

#endif

