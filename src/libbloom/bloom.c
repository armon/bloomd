#include <math.h>
#include <iso646.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <syslog.h>
#include "bloom.h"

/*
 * Static definitions
 */
static const uint32_t MAGIC_HEADER = 0xCB1005DD;  // Vaguely like CBLOOMDD
extern void MurmurHash3_x64_128(const void * key, const int len, const uint32_t seed, void *out);
extern void SpookyHash128(const void *key, size_t len, uint64_t seed1, uint64_t seed2,
        uint64_t *hash1, uint64_t *hash2);

/**
 * Creates a new bloom filter using a given bitmap and k-value.
 * @arg map A bloom_bitmap pointer.
 * @arg k_num The number of hash functions to use. Ignored if the header value is different.
 * @arg new_filter 1 if new, sets the magic byte and does not check it.
 * @arg filter The filter to setup
 * @return 0 for success. Negative for error.
 */
int bf_from_bitmap(bloom_bitmap *map, uint32_t k_num, int new_filter, bloom_bloomfilter *filter) {
    // Check our args
    if (map == NULL || k_num < 1) {
        return -EINVAL;
    }

    // Check the size of the map
    if (map->size < sizeof(bloom_filter_header)) {
        return -ENOMEM;
    }

    // Setup the pointers
    filter->map = map;
    filter->header = (bloom_filter_header*)map->mmap;

    // Get the bitmap size
    filter->bitmap_size = (map->size - sizeof(bloom_filter_header)) * 8;

    // Setup the header if it is new
    if (new_filter) {
        filter->header->magic = MAGIC_HEADER;
        filter->header->k_num = k_num;
        filter->header->count = 0;

        // Since this is a new filter, force a flush of
        // the headers. This mainly affects bitmaps that
        // are in the PERSIST mode. Since no flush happens
        // until the first key is set, it can cause filters
        // to be created that have no headers, and thus cannot
        // be loaded.
        bf_flush(filter);

    // Check for the header if not new
    } else if (filter->header->magic != MAGIC_HEADER) {
        syslog(LOG_ERR, "Magic byte for bloom filter is wrong! Aborting load.");
        return -1;
    }

    // Setup the offset
    filter->offset = filter->bitmap_size / filter->header->k_num;

    // Done, return
    return 0;
}


/**
 * Internal bf_contains method.
 * @arg filter The filter
 * @arg key The key to check
 * @arg hashes Contains at least K num hashes
 * @return 0 if not contained, 1 if contained.
 */
static int bf_internal_contains(bloom_bloomfilter *filter, uint64_t *hashes) {
    uint64_t m = filter->offset;
    uint64_t offset;
    uint64_t h;
    uint32_t i;
    uint64_t bit;
    int res;

    for (i=0; i< filter->header->k_num; i++) {
        h = hashes[i];                                  // Get the hash value
        offset = 8*sizeof(bloom_filter_header) + i * m; // Get the partition offset
        bit = offset + (h % m);                         // Compute the bit offset
        res = bitmap_getbit(filter->map, bit);
        if (res == 0) {
            return 0;
        }
    }
    return 1;
}


/**
 * Adds a new key to the bloom filter.
 * @arg filter The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int bf_add(bloom_bloomfilter *filter, char* key) {
    // Allocate the hash space
    uint64_t *hashes = alloca(filter->header->k_num * sizeof(uint64_t));

    // Compute the hashes
    bf_compute_hashes(filter->header->k_num, key, hashes);

    // Check if the item exists
    int res = bf_internal_contains(filter, hashes);
    if (res == 1) {
        return 0;  // Key already present, do not add.
    }

    uint64_t m = filter->offset;
    uint64_t offset;
    uint64_t h;
    uint32_t i;
    uint64_t bit;

    for (i=0; i< filter->header->k_num; i++) {
        h = hashes[i];                                  // Get the hash value
        offset = 8*sizeof(bloom_filter_header) + i * m; // Get the partition offset
        bit = offset + (h % m);                         // Compute the bit offset
        bitmap_setbit(filter->map, bit);
    }

    filter->header->count += 1;
    return 1;
}

/**
 * Checks the filter for a key
 * @arg filter The filter to check
 * @arg key The key to check
 * @returns 1 if present, 0 if not present, negative on error.
 */
int bf_contains(bloom_bloomfilter *filter, char* key) {
    // Allocate the hash space
    uint64_t *hashes = alloca(filter->header->k_num * sizeof(uint64_t));

    // Compute the hashes
    bf_compute_hashes(filter->header->k_num, key, hashes);

    // Use the internal contains method
    return bf_internal_contains(filter, hashes);
}

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t bf_size(bloom_bloomfilter *filter) {
    // Read it from the file header directly
    return filter->header->count;
}

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int bf_flush(bloom_bloomfilter *filter) {
    // Flush the bitmap if we have one
    if (filter == NULL || filter->map == NULL) {
        return -1;
    }
    return bitmap_flush(filter->map);
}

/**
 * Flushes and closes the filter. Closes the underlying bitmap,
 * but does not free it.
 * @return 0 on success, negative on failure.
 */
int bf_close(bloom_bloomfilter *filter) {
    // Make sure we have a filter
    if (filter == NULL || filter->map == NULL) {
        return -1;
    }

    // Flush first
    bf_flush(filter);

    // Clean up the map
    bitmap_close(filter->map);
    filter->map = NULL;

    // Clear all the fields
    filter->header = NULL;
    filter->offset = 0;
    filter->bitmap_size = 0;

    return 0;
}

/*
 * Utility methods
 */

/*
 * Expects capacity and probability to be set,
 * and sets the bytes and k_num that should be used.
 * @return 0 on success, negative on error.
 */
int bf_params_for_capacity(bloom_filter_params *params) {
    // Sets the required size
    int res = bf_size_for_capacity_prob(params);
    if (res != 0) return res;

    // Sets the ideal k
    res = bf_ideal_k_num(params);
    if (res != 0) return res;

    // Adjust for the header size
    params->bytes += sizeof(bloom_filter_header);
    return 0;
}

/*
 * Expects capacity and probability to be set, computes the
 * minimum byte size required.
 * @return 0 on success, negative on error.
 */
int bf_size_for_capacity_prob(bloom_filter_params *params) {
    uint64_t capacity = params->capacity;
    double fp_prob = params->fp_probability;
    if (capacity == 0 || fp_prob == 0) {
        return -1;
    }
    double bits = -(capacity*log(fp_prob)/(log(2)*log(2)));
    uint64_t whole_bits = ceil(bits);
    params->bytes = ceil(whole_bits / 8.0);
    return 0;
}


/*
 * Expects capacity and size to be set, computes the best
 * false positive probability given an ideal k.
 * @return 0 on success, negative on error.
 */
int bf_fp_probability_for_capacity_size(bloom_filter_params *params) {
    uint64_t bits = params->bytes * 8;
    uint64_t capacity = params->capacity;
    if (bits == 0 || capacity == 0) {
        return -1;
    }
    double fp_prob = pow(M_E, -( (double)bits / (double)capacity)*(pow(log(2),2)));
    params->fp_probability = fp_prob;
    return 0;
}

/*
 * Expects bytes and probability to be set,
 * computes the expected capacity.
 * @return 0 on success, negative on error.
 */
int bf_capacity_for_size_prob(bloom_filter_params *params) {
    uint64_t bits = params->bytes * 8;
    double prob = params->fp_probability;
    if (bits == 0 || prob == 0) {
        return -1;
    }
    uint64_t capacity = -(bits / log(prob) * (log(2) * log(2)));
    params->capacity = capacity;
    return 0;
}

/*
 * Expects bytes and capacity to be set,
 * computes the ideal k num.
 * @return 0 on success, negative on error.
 */
int bf_ideal_k_num(bloom_filter_params *params) {
    uint64_t bits = params->bytes * 8;
    uint64_t capacity = params->capacity;
    if (bits == 0 || capacity == 0) {
        return -1;
    }
    uint32_t ideal_k = round(log(2) * bits / capacity);
    params->k_num = ideal_k;
    return 0;
}

// Computes our hashes
void bf_compute_hashes(uint32_t k_num, char *key, uint64_t *hashes) {
    /**
     * We use the results of
     * 'Less Hashing, Same Performance: Building a Better Bloom Filter'
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf, to use
     * g_i(x) = h1(u) + i * h2(u) mod m'
     *
     * This allows us to only use 2 hash functions h1, and h2 but generate
     * k unique hashes using linear combinations. This is a vast speedup
     * over our previous technique of 4 hashes, that used double hashing.
     *
     */

    // Get the length of the key
    uint64_t len = strlen(key);

    // Compute the first hash
    uint64_t out[2];
    MurmurHash3_x64_128(key, len, 0, out);

    // Copy these out
    hashes[0] = out[0];  // Upper 64bits of murmur
    hashes[1] = out[1];  // Lower 64bits of murmur

    // Compute the second hash
    uint64_t *hash1 = out;
    uint64_t *hash2 = hash1+1;
    SpookyHash128(key, len, 0, 0, hash1, hash2);

    // Copy these out
    hashes[2] = out[0];   // Use the upper 64bits of Spooky
    hashes[3] = out[1];   // Use the lower 64bits of Spooky

    // Compute an arbitrary k_num using a linear combination
    // Add a mod by the largest 64bit prime. This only reduces the
    // number of addressable bits by 54 but should make the hashes
    // a bit better.
    for (uint32_t i=4; i < k_num; i++) {
        hashes[i] = hashes[1] + ((i * hashes[3]) % 18446744073709551557U);
    }
}

