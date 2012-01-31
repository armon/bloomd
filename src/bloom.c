#include "bloom.h"

/*
 * Static definitions
 */
/*
static size_t bloomFilterExtraBuffer();
static unsigned int bloomFilterReadKNum(cbloom_bitmap* map);
static void bloomFilterWriteKNum(cbloom_bitmap* map, unsigned int k_num);
static unsigned long long bloomFilterReadCount(cbloom_bitmap* map);
static void bloomFilterWriteCount(cbloom_bitmap* map, unsigned long long count);
*/

/**
 * Creates a new bloom filter using a given bitmap and k-value.
 * @arg map A cbloom_bitmap pointer.
 * @arg k_num The number of hash functions to use.
 */
cbloom_bloomfilter *createBloomFilter(cbloom_bitmap *map, size_t k_num) {
    // Check our arguments for sanity
    if (map == NULL || k_num < 1) {
        return NULL;
    }

    /*
    // Get the bitmap size, check we have enough room
    size_t bitmap_size = bitmapBitsize(map) - 8 * bloomFilterExtraBuffer();
    if (map->size <= bloomFilterExtraBuffer()) {
        return NULL;
    }

    // Create the bloom filter
    cbloom_bloomfilter *filter = (cbloom_bloomfilter*)calloc(1, sizeof(cbloom_bloomfilter));
    filter->map = map;
    filter->bitmap_size = bitmap_size;

    // Read the existing count
    filter->count = bloomFilterReadCount(map);

    // Read the existing k_num, or set it to k_num if 0
    filter->k_num = bloomFilterReadKNum(map);
    if (filter->k_num == 0) {
        filter->k_num = k_num;
    }

    // Check for compatibility mode, and remove the compatibility bit.
    if (filter->k_num & COMPATIBILITY_MODE) {
        filter->compat_mode = 1;
        filter->k_num = ~COMPATIBILITY_MODE & filter->k_num;
    }

    // Allocate buffer space for the hashes
    filter->hashes = calloc(filter->k_num, sizeof(size_t));

    // Set the offset size
    filter->offset = bitmap_size / filter->k_num;

    // Return the filter
    return filter;
    */
    return NULL;
}

/*
// Returns the extra bytes we need to persist the filter
static size_t bloomFilterExtraBuffer() {
    // We have enough room to store the K num, and the item count.
    return sizeof(int) + sizeof(unsigned long long);
}

// Reads the K Num from the bitmap
static unsigned int bloomFilterReadKNum(cbloom_bitmap* map) {
    unsigned int *slot = (unsigned int*)(map->mmap + (map->size / 8) + sizeof(unsigned long long));
    return *slot;
}

// Write the K Num to the bitmap
static void bloomFilterWriteKNum(cbloom_bitmap* map, unsigned int k_num) {
    unsigned int *slot = (unsigned int*)(map->mmap + (map->size / 8) + sizeof(unsigned long long));
    *slot = k_num;
}

// Reads the count from the bitmap
static unsigned long long bloomFilterReadCount(cbloom_bitmap* map) {
    unsigned long long *slot = (unsigned long long*)(map->mmap + (map->size / 8));
    return *slot;
}

// Write the count to the bitmap
static void bloomFilterWriteCount(cbloom_bitmap* map, unsigned long long count) {
    unsigned long long *slot = (unsigned long long*)(map->mmap + (map->size / 8));
    *slot = count;
}
*/

/**
 * Adds a new key to the bloom filter.
 * @arg filter The filter to add to
 * @arg key The key to add
 * @returns 1 if the key was added, 0 if present. Negative on failure.
 */
int bloomFilterAdd(cbloom_bloomfilter *filter, char* key) {
    return 0;
}

/**
 * Checks the filter for a key
 * @arg filter The filter to check
 * @arg key The key to check 
 * @returns 1 if present, 0 if not present, negative on error.
 */
int bloomFilterContains(cbloom_bloomfilter *filter, char* key) {
    return 0;
}

/**
 * Returns the size of the bloom filter in item count
 */
uint64_t bloomFilterSize(cbloom_bloomfilter *filter) {
    return 0;
}

/**
 * Flushes the filter, and updates the metadata.
 * @return 0 on success, negative on failure.
 */
int bloomFilterFlush(cbloom_bloomfilter *filter) {
    return 0;
}

/**
 * Flushes and closes the filter. Does not close the underlying bitmap.
 * @return 0 on success, negative on failure.
 */
int bloomFilterClose(cbloom_bloomfilter *filter) {
    return 0;
}


