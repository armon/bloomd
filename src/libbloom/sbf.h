#ifndef BLOOM_SBF_H
#define BLOOM_SBF_H
#include "bloom.h"

/**
 * Defines a callback function that
 * takes an arbitrary pointer value, the number of bytes required,
 * and a reference to bloom_bitmap for output. Returns an int as
 * status, 0 is success.
 */
typedef int(*bloom_sbf_callback)(void* in, uint64_t bytes, bloom_bitmap *out);

/**
 * The parameters to configure a scalable bloom filter.
 * See DEFAULT_PARAMS and SLOW_GROW_PARAMS.
 */
typedef struct {
    uint64_t initial_capacity;      // Initial size
    double fp_probability;          // FP probability
    uint32_t scale_size;              // Scale size for new filters
    double probability_reduction;   // New filter, fp_prob reduciton
} bloom_sbf_params;

/**
 * These are the default parameters for bloom_sbf_params.
 * Creates an initial capacity for 1 million items, 1/1000
 * false positive rate, 4x scaling, and a 90% false positive
 * probability reduction with each new filter. This works well
 * in most situations.
 */
#define SBF_DEFAULT_PARAMS {1e5, 1e-4, 4, 0.9}

/**
 * These are memory sensitive parameters for bloom_sbf_params.
 * This will grow more slowly than the DEFAULT_PARAMS, but
 * increases the changes of false positives.
 * Creates an initial capacity for 1 million items, 1/1000
 * false positive rate, 2x scaling, and a 80% false positive
 * probability reduction with each new filter.
 */
#define SBF_SLOW_GROW_PARAMS {1e5, 1e-4, 2, 0.8}

/**
 * Represents a scalable bloom filters
 */
typedef struct {
    bloom_sbf_params params;              // Our parameters

    bloom_sbf_callback callback;    // Callback, or NULL for auto
    void *callback_input;           // Callback input if any

    uint32_t num_filters;           // The number of filters
    bloom_bloomfilter **filters;     // Array into the filters

    unsigned char *dirty_filters;   // Used to set a dirty flag

    uint64_t *capacities;            // Tracks the per-filter capacity
} bloom_sbf;

/**
 * Creates a new scalable bloom filter using given bloom filters.
 * @arg params The parameters of the new SBF
 * @arg cb The callback function to invoke. NULL to use anonymous bitmaps.
 * @arg cb_in The opaque pointer to provide to the callback.
 * @arg num_filters The number of fileters in filters. 0 for none.
 * @arg filters Pointer to an array of the existing filters. Will be copied.
 * This array should be ordered from the largest filter to the smallest.
 * @arg sbf The filter to setup
 * @return 0 for success. Negative for error.
 */
int sbf_from_filters(bloom_sbf_params *params,
                     bloom_sbf_callback cb,
                     void *cb_in,
                     uint32_t num_filters,
                     bloom_bloomfilter **filters,
                     bloom_sbf *sbf);

/**
 * Adds a new key to the bloom filter.
 * @arg sbf The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int sbf_add(bloom_sbf *sbf, char* key);

/**
 * Checks the filter for a key
 * @arg sbf The filter to check
 * @arg key The key to check
 * @returns 1 if present, 0 if not present, negative on error.
 */
int sbf_contains(bloom_sbf *sbf, char* key);

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t sbf_size(bloom_sbf *sbf);

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int sbf_flush(bloom_sbf *sbf);

/**
 * Flushes and closes the filter. Closes the underlying bitmap and filters,
 * and frees them.
 * @return 0 on success, negative on failure.
 */

int sbf_close(bloom_sbf *sbf);

/**
 * Returns the total capacity of the SBF currently.
 */
uint64_t sbf_total_capacity(bloom_sbf *sbf);

/**
 * Returns the total bytes size of the SBF currently.
 */
uint64_t sbf_total_byte_size(bloom_sbf *sbf);

#endif
