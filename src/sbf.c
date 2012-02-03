#include <string.h>
#include "sbf.h"

/**
 * Static declarations
 */
static int sbf_append_filter(bloom_sbf *sbf);

int sbf_from_filters(bloom_sbf_params *params, 
                     bloom_sbf_callback cb,
                     void *cb_in,
                     uint32_t num_filters,
                     bloom_bloomfilter *filters,
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
        sbf->filters = malloc(num_filters*sizeof(bloom_bloomfilter*));
        memcpy(sbf->filters, filters, num_filters*sizeof(bloom_bloomfilter*));
        sbf->dirty_filters = calloc(num_filters, sizeof(unsigned char));
    } else {
        sbf->num_filters = 0;
        sbf->filters = NULL;
        sbf->dirty_filters = NULL;

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
    return 0;
}

/**
 * Checks the filter for a key
 * @arg sbf The filter to check
 * @arg key The key to check 
 * @returns 1 if present, 0 if not present, negative on error.
 */
int sbf_contains(bloom_sbf *sbf, char* key) {
    return 0;
}

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t sbf_size(bloom_sbf *sbf) {
    return 0;
}

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int sbf_flush(bloom_sbf *sbf) {
    return 0;
}

/**
 * Flushes and closes the filter. Does not close the underlying bitmap.
 * @return 0 on success, negative on failure.
 */
int sbf_close(bloom_sbf *sbf) {
    return 0;
}

/**
 * Returns the total capacity of the SBF currently.
 */
uint64_t sbf_total_capacity(bloom_sbf *sbf) {
    return 0;
}

/**
 * Returns the total bytes size of the SBF currently.
 */
uint64_t sbf_total_byte_size(bloom_sbf *sbf) {
    return 0;
}

/**
 * Appends a new filter to the SBF
 */
static int sbf_append_filter(bloom_sbf *sbf) {
    return 0;
}

