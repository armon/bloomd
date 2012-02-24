#include <string.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include "spinlock.h"
#include "filter.h"
#include "sbf.h"

/**
 * Representation of a bloom filters
 */
struct bloom_filter {
    bloom_config *config;           // Filter configuration

    char *filter_name;              // The name of the filter
    char *full_path;                // Path to our data

    bloom_sbf *sbf;                 // Underlying SBF

    filter_counters counters;       // Counters
    bloom_spinlock counter_lock;    // Protect the counters
};

/*
 * Generates the folder name, given a filter name.
 */
static const char* FILTER_FOLDER_NAME = "bloomd.%s";

/*
 * Static delarations
 */
static int discover_existing_filters(bloom_filter *f);
static int bloomf_sbf_callback(void* in, uint64_t bytes, bloom_bitmap *out);

/**
 * Initializes a bloom filter wrapper.
 * @arg config The configuration to use
 * @arg filter_name The name of the filter
 * @arg discover Should existing data files be discovered
 * @arg filter Output parameter, the new filter
 * @return 0 on success
 */
int init_bloom_filter(bloom_config *config, char *filter_name, int discover, bloom_filter **filter) {
    // Allocate the buffers
    bloom_filter *f = *filter = calloc(1, sizeof(bloom_filter));

    // Store the things
    f->config = config;
    f->filter_name = strdup(filter_name);

    // Get the folder name
    char *folder_name = NULL;
    asprintf(&folder_name, FILTER_FOLDER_NAME, f->filter_name);

    // Compute the full path
    f->full_path = join_path(config->data_dir, folder_name);
    free(folder_name);

    // Initialize the spin lock
    INIT_BLOOM_SPIN(&f->counter_lock);

    // Discover the existing filters if we need to
    if (discover && !config->in_memory) {
        discover_existing_filters(f);
    }

    return 0;
}

/**
 * Destroys a bloom filter
 * @arg filter The filter to destroy
 * @return 0 on success
 */
int destroy_bloom_filter(bloom_filter *filter) {
    // Close first
    bloomf_close(filter);

    // Cleanup
    free(filter->filter_name);
    free(filter->full_path);
    free(filter);
    return 0;
}

/**
 * Gets the counters that belong to a filter
 * @arg filter The filter
 * @return A reference to the counters of a filter
 */
filter_counters* bloomf_counters(bloom_filter *filter) {
    return &filter->counters;
}

/**
 * Checks if a filter is currectly mapped into
 * memory or if it is proxied.
 * @return 1 if in-memory, 0 if proxied.
 */
int bloomf_in_memory(bloom_filter *filter) {
    return !(filter->sbf);
}

/**
 * Flushes the filter. Idempotent if the
 * filter is proxied or not dirty.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_flush(bloom_filter *filter) {
    // Only do things if we are non-proxied
    if (filter->sbf) {
        return sbf_flush(filter->sbf);
    }
    return 0;
}

/**
 * Gracefully closes a bloom filter.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_close(bloom_filter *filter) {
    // Only act if we are non-proxied
    if (filter->sbf) {
        sbf_close(filter->sbf);
        free(filter->sbf);
        filter->sbf = NULL;
    }
    return 0;
}

/**
 * Deletes the bloom filter with
 * extreme prejudice.
 * @arg filter The filter to delete
 * @return 0 on success.
 */
int bloomf_delete(bloom_filter *filter) {
    // Close first
    bloomf_close(filter);

    // Do nothing if we are in memory
    if (filter->config->in_memory) return 0;

    // Delete the files
    struct dirent **namelist;
    int num;

    // Filter only data dirs, in sorted order
    num = scandir(filter->full_path, &namelist, NULL, NULL);
    syslog(LOG_INFO, "Deleting %d files for filter %s.", num, filter->filter_name);

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        char *file_path = join_path(filter->full_path, namelist[i]->d_name);
        syslog(LOG_INFO, "Deleting: %s.", file_path);
        if (unlink(file_path)) {
            syslog(LOG_ERR, "Failed to delete: %s. %s", file_path, strerror(errno));
        }
        free(file_path);
    }

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        free(namelist[i]);
    }
    free(namelist);

    // Delete the directory
    if (unlink(filter->full_path)) {
        syslog(LOG_ERR, "Failed to delete: %s. %s", filter->full_path, strerror(errno));
    }

    return 0;
}

/**
 * Checks if the filter contains a given key
 * @arg filter The filter to check
 * @arg key The key to check
 * @return 0 if not contained, 1 if contained.
 */
int bloomf_contains(bloom_filter *filter, char *key) {
    return 0;
}

/**
 * Adds a key to the given filter
 * @arg filter The filter to add to
 * @arg key The key to add
 * @return 0 if not added, 1 if added.
 */
int bloomf_add(bloom_filter *filter, char *key) {
    return 0;
}

/**
 * Gets the maximum capacity of the filter
 * @arg filter The filter to check
 * @return The total capacity of the filter
 */
uint64_t bloomf_capacity(bloom_filter *filter) {
    return 0;
}

/**
 * Gets the current byte size of the filter
 * @arg filter The filter
 * @return The total byte size of the filter
 */
uint64_t bloomf_byte_size() {
    return 0;
}

/**
 * Discovers existing filters, and faults them in.
 */
static uint64_t get_size(char* filename) {
    struct stat buf;
    buf.st_size = 0;
    stat(filename, &buf);
    return buf.st_size;
}

/**
 * Works with scandir to filter out non-data files.
 */
int filter_data_files(struct dirent *d) {
    // Get the file name
    char *name = d->d_name;

    // Look if it ends in ".data"
    int name_len = strlen(name);

    // Too short
    if (name_len < 6) return 0;

    // Compare the ending
    if (strcmp(name+(name_len-5), ".data") == 0) {
        return 1;
    }

    // Do not store
    return 0;
}

/**
 * This beast mode method scans the data directory
 * belonging to this filter for any existing filters,
 * and restores the SBF
 * @return 0 on success. -1 on error.
 */
static int discover_existing_filters(bloom_filter *f) {
    // Scan through the folder looking for data files
    struct dirent **namelist;
    int num;

    // Filter only data dirs, in sorted order
    num = scandir(f->full_path, &namelist, filter_data_files, alphasort);
    syslog(LOG_INFO, "Found %d files for filter %s.", num, f->filter_name);

    // Allocate space for all the filter
    bloom_bitmap **maps = malloc(num * sizeof(bloom_bitmap*));
    bloom_bloomfilter **filters = malloc(num * sizeof(bloom_bloomfilter*));

    // Initialize the bitmaps and bloom filters
    bloom_bitmap *map;
    int res;
    int err = 0;
    uint64_t size;
    for (int i=0; i < num && !err; i++) {
        // Get the full path to the bitmap
        char *bitmap_path = join_path(f->full_path, namelist[i]->d_name);
        syslog(LOG_INFO, "Discovered bloom filter: %s.", bitmap_path);

        // Get the size
        size = get_size(bitmap_path);
        if (size == 0) {
            err = 1;
            syslog(LOG_ERR, "Failed to get the filesize for: %s. %s", bitmap_path, strerror(errno));
            free(bitmap_path);
            break;
        }

        // Create the bitmap
        bloom_bitmap *bitmap = maps[i] = malloc(sizeof(bloom_bitmap));
        res = bitmap_from_filename(bitmap_path, size, 0, 0, bitmap);
        if (res != 0) {
            err = 1;
            syslog(LOG_ERR, "Failed to load bitmap for: %s.", bitmap_path);
            free(bitmap);
            free(bitmap_path);
            break;
        }

        // Create the bloom filter
        bloom_bloomfilter *filter = filters[i] = malloc(sizeof(bloom_bloomfilter));
        res = bf_from_bitmap(bitmap, 1, 0, filter);
        if (res != 0) {
            err = 1;
            syslog(LOG_ERR, "Failed to load bloom filter for: %s.", bitmap_path);
            free(filter);
            bitmap_close(bitmap);
            free(bitmap);
            free(bitmap_path);
            break;
        }

        // Cleanup
        free(bitmap_path);
    }

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        free(namelist[i]);
    }
    free(namelist);

    // Return if there was an error
    if (err) return -1;

    // Setup the SBF params
    bloom_sbf_params params = {
        f->config->initial_capacity,
        f->config->default_probability,
        f->config->scale_size,
        f->config->probability_reduction
    };

    // Create the SBF
    f->sbf = malloc(sizeof(bloom_sbf));
    res = sbf_from_filters(&params, bloomf_sbf_callback, f, num, filters, f->sbf);

    // Cleanup on err
    if (res != 0) {
        syslog(LOG_ERR, "Failed to make scalable bloom filter for: %s.", f->filter_name);

        // For fucks sake. We need to clean up so much shit now.
        free(f->sbf);
        f->sbf = NULL;
        for (int i=0; i < num; i++) {
            bf_close(filters[i]);
            bitmap_close(maps[i]);
            free(filters[i]);
            free(maps[i]);
        }
    }

    // Remove the filters list
    free(maps);
    free(filters);
    return (err) ? -1 : 0;
}

