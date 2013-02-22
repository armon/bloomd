#include <string.h>
#include <math.h>
#include <stdio.h>
#include <iso646.h>
#include "sbf.h"

/**
 * Static declarations
 */
static int sbf_append_filter(bloom_sbf *sbf);
static void sbf_init_capacities(bloom_sbf *sbf);
static double sbf_inital_probability(double fp_prob, double r);

int sbf_from_filters(bloom_sbf_params *params,
                     bloom_sbf_callback cb,
                     void *cb_in,
                     uint32_t num_filters,
                     bloom_bloomfilter **filters,
                     bloom_sbf *sbf)
{
    // Copy the params
    memcpy(&(sbf->params), params, sizeof(bloom_sbf_params));

    // Set the callback and its args
    sbf->callback = cb;
    sbf->callback_input = cb_in;

    // Copy the filters
    if (num_filters > 0) {
        sbf->num_filters = num_filters;
        sbf->filters = calloc(num_filters, sizeof(bloom_bloomfilter*));
        memcpy(sbf->filters, filters, num_filters*sizeof(bloom_bloomfilter*));
        sbf->dirty_filters = calloc(num_filters, sizeof(unsigned char));
        sbf->capacities = calloc(num_filters, sizeof(uint64_t));

        // Compute the capacities of the existing filters
        sbf_init_capacities(sbf);
    } else {
        sbf->num_filters = 0;
        sbf->filters = NULL;
        sbf->dirty_filters = NULL;
        sbf->capacities = NULL;

        int res = sbf_append_filter(sbf);
        if (res != 0) {
            return res;
        }
    }

    return 0;
}

/**
 * Adds a new key to the bloom filter.
 * @arg sbf The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int sbf_add(bloom_sbf *sbf, char* key) {
    // Check if the key is contained first.
    if (sbf_contains(sbf, key) == 1) {
        return 0;
    }

    // Get the largest filter
    bloom_bloomfilter *filter = sbf->filters[0];

    // Check if we are over capacity
    if (bf_size(filter) >= sbf->capacities[0]) {
        int res = sbf_append_filter(sbf);
        if (res != 0) {
            return res;
        }
        filter = sbf->filters[0];
    }

    // Mark as dirty, add to the largest filter
    sbf->dirty_filters[0] = 1;
    int res = bf_add(filter, key);
    return res;
}

/**
 * Checks the filter for a key
 * @arg sbf The filter to check
 * @arg key The key to check
 * @returns 1 if present, 0 if not present, negative on error.
 */
int sbf_contains(bloom_sbf *sbf, char* key) {
    // Check each filter from largest to smallest
    int res;
    for (uint32_t i=0;i<sbf->num_filters;i++) {
        res = bf_contains(sbf->filters[i], key);
        if (res == 1) return 1;
    }
    return 0;
}

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t sbf_size(bloom_sbf *sbf) {
    uint64_t size = 0;
    for (uint32_t i=0;i<sbf->num_filters;i++) {
        size += bf_size(sbf->filters[i]);
    }
    return size;
}

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int sbf_flush(bloom_sbf *sbf) {
    // Check if it has been previously closed
    if (sbf == NULL or sbf->num_filters == 0) {
        return -1;
    }

    int res = 0;
    for (uint32_t i=0;i<sbf->num_filters;i++) {
        if (sbf->dirty_filters[i] == 1) {
            res = bf_flush(sbf->filters[i]);
            if (res != 0) break;
            sbf->dirty_filters[i] = 0;
        }
    }
    return res;
}

/**
 * Flushes and closes the filter. Closes the underlying bitmap and filters,
 * and frees them.
 * @return 0 on success, negative on failure.
 */
int sbf_close(bloom_sbf *sbf) {
    // Check if it has been previously closed
    if (sbf == NULL or sbf->num_filters == 0) {
        return -1;
    }

    // Flush first
    sbf_flush(sbf);

    int res = 0;
    bloom_bitmap *map;
    for (uint32_t i=0;i<sbf->num_filters;i++) {
        map = sbf->filters[i]->map;
        res |= bf_close(sbf->filters[i]);
        free(sbf->filters[i]);
        free(map);
    }

    // Clean up memory
    free(sbf->filters);
    sbf->filters = NULL;
    free(sbf->dirty_filters);
    sbf->dirty_filters = NULL;
    free(sbf->capacities);
    sbf->capacities = NULL;

    // Zero out
    sbf->num_filters = 0;
    sbf->callback = NULL;
    sbf->callback_input = NULL;

    return res;
}

/**
 * Returns the total capacity of the SBF currently.
 */
uint64_t sbf_total_capacity(bloom_sbf *sbf) {
    uint64_t total_capacity = 0;
    for (uint32_t i=0;i<sbf->num_filters;i++) {
        total_capacity += sbf->capacities[i];
    }
    return total_capacity;
}

/**
 * Returns the total bytes size of the SBF currently.
 */
uint64_t sbf_total_byte_size(bloom_sbf *sbf) {
    uint64_t size = 0;
    bloom_bloomfilter *filter;
    for (uint32_t i=0;i<sbf->num_filters;i++) {
        filter = sbf->filters[i];
        size += filter->map->size;
    }
    return size;
}

/**
 * Appends a new filter to the SBF
 */
static int sbf_append_filter(bloom_sbf *sbf) {
    // Start with the initial configs
    uint64_t capacity = sbf->params.initial_capacity;
    double fp_prob = sbf_inital_probability(sbf->params.fp_probability, sbf->params.probability_reduction);

    // Get the settings for the new filter
    capacity *= pow(sbf->params.scale_size, sbf->num_filters);
    fp_prob *= pow(sbf->params.probability_reduction, sbf->num_filters);

    // Compute the new parameters
    bloom_filter_params params = {0, 0, capacity, fp_prob};
    int res = bf_params_for_capacity(&params);
    if (res != 0) {
        return res;
    }

    // Allocate a new bitmap
    bloom_bitmap *map = calloc(1, sizeof(bloom_bitmap));

    // Try to use our call back if we have one
    if (sbf->callback) {
        res = sbf->callback(sbf->callback_input, params.bytes, map);
    } else {
        res = bitmap_from_file(-1, params.bytes, ANONYMOUS, map);
    }
    if (res != 0) {
        free(map);
        return res;
    }

    // Create a new bloom filter
    bloom_bloomfilter *filter = calloc(1, sizeof(bloom_bloomfilter));
    res = bf_from_bitmap(map, params.k_num, 1, filter);
    if (res != 0) {
        free(filter);
        free(map);
        return res;
    }

    // Hold onto the old filters and dirty state
    bloom_bloomfilter **old_filters = sbf->filters;
    unsigned char *old_dirty = sbf->dirty_filters;
    uint64_t *old_capacities = sbf->capacities;

    // Increase the filter count, re-allocate the arrays
    sbf->num_filters++;
    sbf->filters = malloc(sbf->num_filters*sizeof(bloom_bloomfilter*));
    sbf->dirty_filters = calloc(sbf->num_filters, sizeof(unsigned char));
    sbf->capacities = calloc(sbf->num_filters, sizeof(uint64_t));

    // Copy the old filters and release
    if (sbf->num_filters > 1) {
        memcpy(sbf->filters+1, old_filters, (sbf->num_filters-1)*sizeof(bloom_bloomfilter*));
        memcpy(sbf->dirty_filters+1, old_dirty, (sbf->num_filters-1)*sizeof(unsigned char));
        memcpy(sbf->capacities+1, old_capacities, (sbf->num_filters-1)*sizeof(uint64_t));
        free(old_filters);
        free(old_dirty);
        free(old_capacities);
    }

    // Set the new filter, set dirty false
    sbf->filters[0] = filter;
    sbf->dirty_filters[0] = 0;
    sbf->capacities[0] = capacity;

    return 0;
}

/**
 * Based on "Scalable Bloom Filters", Almeida 2007
 * We use : P <= P0 * (1 / (1 - r))
 * To bound the final FP probability. This method calculates P0
 */
static double sbf_inital_probability(double fp_prob, double r) {
    return (1-r) * fp_prob;
}

/**
 * Computes the capacities for the existing filters
 * when we are initialized with filters.
 */
static void sbf_init_capacities(bloom_sbf *sbf) {
    uint64_t init_capacity = sbf->params.initial_capacity;
    uint64_t capacity;

    for (uint32_t i=0;i<sbf->num_filters;i++) {
        // Compute the capacity of the ith filter
        capacity = init_capacity * pow(sbf->params.scale_size, (sbf->num_filters - i - 1));
        sbf->capacities[i] = capacity;
    }
}
