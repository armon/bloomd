#include <string.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <sys/time.h>
#include <assert.h>
#include "filter.h"
#include "type_compat.h"

/*
 * Generates the folder name, given a filter name.
 */
static const char* FILTER_FOLDER_NAME = "bloomd.%s";

/**
 * Format for the data file names.
 */
static const char* DATA_FILE_NAME = "data.%03d.mmap";

/*
 * Generates the config file name
 */
static const char* CONFIG_FILENAME = "config.ini";

/*
 * Static delarations
 */
static int thread_safe_fault(bloom_filter *f);
static int discover_existing_filters(bloom_filter *f);
static int create_sbf(bloom_filter *f, int num, bloom_bloomfilter **filters);
static int bloomf_sbf_callback(void* in, uint64_t bytes, bloom_bitmap *out);
static int timediff_msec(struct timeval *t1, struct timeval *t2);

static int filter_out_special(CONST_DIRENT_T *d);

/**
 * Initializes a bloom filter wrapper.
 * @arg config The configuration to use
 * @arg filter_name The name of the filter
 * @arg discover Should existing data files be discovered. Otherwise
 * they will be faulted in on-demand.
 * @arg filter Output parameter, the new filter
 * @return 0 on success
 */
int init_bloom_filter(bloom_config *config, char *filter_name, int discover, bloom_filter **filter) {
    // Allocate the buffers
    bloom_filter *f = *filter = calloc(1, sizeof(bloom_filter));

    // Store the things
    f->config = config;
    f->filter_name = strdup(filter_name);

    // Copy filter configs
    f->filter_config.initial_capacity = config->initial_capacity;
    f->filter_config.capacity = config->initial_capacity;
    f->filter_config.default_probability = config->default_probability;
    f->filter_config.scale_size = config->scale_size;
    f->filter_config.probability_reduction = config->probability_reduction;
    f->filter_config.in_memory = config->in_memory;

    // Get the folder name
    char *folder_name = NULL;
    int res;
    res = asprintf(&folder_name, FILTER_FOLDER_NAME, f->filter_name);
    assert(res != -1);

    // Compute the full path
    f->full_path = join_path(config->data_dir, folder_name);
    free(folder_name);

    // Initialize the locks
    INIT_BLOOM_SPIN(&f->counter_lock);
    pthread_mutex_init(&f->sbf_lock, NULL);

    // Try to create the folder path
    res = mkdir(f->full_path, 0755);
    if (res && errno != EEXIST) {
        syslog(LOG_ERR, "Failed to create filter directory '%s'. Err: %d [%d]", f->full_path, res, errno);
        return res;
    }

    // Read in the filter_config
    char *config_name = join_path(f->full_path, (char*)CONFIG_FILENAME);
    res = filter_config_from_filename(config_name, &f->filter_config);
    free(config_name);
    if (res && res != -ENOENT) {
        syslog(LOG_ERR, "Failed to read filter '%s' configuration. Err: %d [%d]", f->filter_name, res, errno);
        return res;
    }

    // Discover the existing filters if we need to
    res = 0;
    if (discover) {
        res = thread_safe_fault(f);
        if (res) {
            syslog(LOG_ERR, "Failed to fault in the filter '%s'. Err: %d", f->filter_name, res);
        }
    }

    // Trigger a flush on first instantiation. This will create
    // a new ini file for first time filters.
    if (!res) {
        res = bloomf_flush(f);
    }

    return res;
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
 * @notes Thread safe, but may be inconsistent.
 * @arg filter The filter
 * @return A reference to the counters of a filter
 */
filter_counters* bloomf_counters(bloom_filter *filter) {
    return &filter->counters;
}

/**
 * Checks if a filter is currectly mapped into
 * memory or if it is proxied.
 * @notes Thread safe.
 * @return 0 if in-memory, 1 if proxied.
 */
int bloomf_is_proxied(bloom_filter *filter) {
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
        // Time how long this takes
        struct timeval start, end;
        gettimeofday(&start, NULL);

        // If our size has not changed, there is no need to flush
        uint64_t new_size = bloomf_size(filter);
        if (new_size == filter->filter_config.size && filter->filter_config.bytes != 0) {
            return 0;
        }

        // Store our properties for a future unmap
        filter->filter_config.size = new_size;
        filter->filter_config.capacity = bloomf_capacity(filter);
        filter->filter_config.bytes = bloomf_byte_size(filter);

        // Write out filter_config
        char *config_name = join_path(filter->full_path, (char*)CONFIG_FILENAME);
        int res = update_filename_from_filter_config(config_name, &filter->filter_config);
        free(config_name);
        if (res) {
            syslog(LOG_ERR, "Failed to write filter '%s' configuration. Err: %d.",
                    filter->filter_name, res);
        }

        // Flush the filter
        res = 0;
        if (!filter->filter_config.in_memory) {
            res = sbf_flush((bloom_sbf*)filter->sbf);
        }

        // Compute the elapsed time
        gettimeofday(&end, NULL);
        syslog(LOG_INFO, "Flushed filter '%s'. Total time: %d msec.",
                filter->filter_name, timediff_msec(&start, &end));
        return res;
    }
    return 0;
}

/**
 * Gracefully closes a bloom filter.
 * @arg filter The filter to close
 * @return 0 on success.
 */
int bloomf_close(bloom_filter *filter) {
    // Acquire lock
    pthread_mutex_lock(&filter->sbf_lock);

    // Only act if we are non-proxied
    if (filter->sbf) {
        bloomf_flush(filter);

        bloom_sbf *sbf = (bloom_sbf*)filter->sbf;
        filter->sbf = NULL;

        sbf_close(sbf);
        free(sbf);

        filter->counters.page_outs += 1;
    }

    // Release lock
    pthread_mutex_unlock(&filter->sbf_lock);
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

    // Delete the files
    struct dirent **namelist = NULL;
    int num;

    // Filter only data dirs, in sorted order
    num = scandir(filter->full_path, &namelist, filter_out_special, NULL);
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
    if (namelist)
        free(namelist);

    // Delete the directory
    if (rmdir(filter->full_path)) {
        syslog(LOG_ERR, "Failed to delete: %s. %s", filter->full_path, strerror(errno));
    }

    return 0;
}

/**
 * Checks if the filter contains a given key
 * @note Thread safe, as long as bloomf_add is not invoked.
 * @arg filter The filter to check
 * @arg key The key to check
 * @return 0 if not contained, 1 if contained.
 */
int bloomf_contains(bloom_filter *filter, char *key) {
    if (!filter->sbf) {
        if (thread_safe_fault(filter) != 0) return -1;
    }

    // Check the SBF
    int res = sbf_contains((bloom_sbf*)filter->sbf, key);

    // Safely update the counters
    LOCK_BLOOM_SPIN(&filter->counter_lock);
    if (res == 1)
        filter->counters.check_hits += 1;
    else if (res == 0)
        filter->counters.check_misses += 1;
    UNLOCK_BLOOM_SPIN(&filter->counter_lock);

    return res;
}

/**
 * Adds a key to the given filter
 * @arg filter The filter to add to
 * @arg key The key to add
 * @return 0 if not added, 1 if added.
 */
int bloomf_add(bloom_filter *filter, char *key) {
    if (!filter->sbf) {
        if (thread_safe_fault(filter) != 0) return -1;
    }

    // Add the SBF
    int res = sbf_add((bloom_sbf*)filter->sbf, key);

    // Safely update the counters
    LOCK_BLOOM_SPIN(&filter->counter_lock);
    if (res == 1)
        filter->counters.set_hits += 1;
    else if (res == 0)
        filter->counters.set_misses += 1;
    UNLOCK_BLOOM_SPIN(&filter->counter_lock);

    return res;
}

/**
 * Gets the size of the filter in keys
 * @note Thread safe.
 * @arg filter The filter to check
 * @return The total size of the filter
 */
uint64_t bloomf_size(bloom_filter *filter) {
    if (filter->sbf) {
        return sbf_size((bloom_sbf*)filter->sbf);
    } else {
        return filter->filter_config.size;
    }
}

/**
 * Gets the maximum capacity of the filter
 * @note Thread safe.
 * @arg filter The filter to check
 * @return The total capacity of the filter
 */
uint64_t bloomf_capacity(bloom_filter *filter) {
    if (filter->sbf) {
        return sbf_total_capacity((bloom_sbf*)filter->sbf);
    } else {
        return filter->filter_config.capacity;
    }
}

/**
 * Gets the current byte size of the filter
 * @note Thread safe.
 * @arg filter The filter
 * @return The total byte size of the filter
 */
uint64_t bloomf_byte_size(bloom_filter *filter) {
    if (filter->sbf) {
        return sbf_total_byte_size((bloom_sbf*)filter->sbf);
    } else {
        return filter->filter_config.bytes;
    }
}

/**
 * Provides a thread safe faulting of filters.
 * The main use case of this is to allow
 * bloomf_contains to be safe.
 */
static int thread_safe_fault(bloom_filter *f) {
    // Acquire lock
    pthread_mutex_lock(&f->sbf_lock);

    int res = 0;
    if (!f->sbf) {
        if (f->filter_config.in_memory) {
            res = create_sbf(f, 0, NULL);
        } else {
            res = discover_existing_filters(f);
        }
    }

    // Release lock
    pthread_mutex_unlock(&f->sbf_lock);
    return res;
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
 * Works with scandir to filter out special files
 */
static int filter_out_special(CONST_DIRENT_T *d) {
    // Get the file name
    char *name = (char*)d->d_name;

    // Make sure its not speci
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return 0;
    }
    return 1;
}

/**
 * Works with scandir to filter out non-data files.
 */
static int filter_data_files(CONST_DIRENT_T *d) {
    // Get the file name
    char *name = (char*)d->d_name;

    // Look if it ends in ".data"
    int name_len = strlen(name);

    // Too short
    if (name_len < 6) return 0;

    // Compare the ending
    if (strcmp(name+(name_len-5), ".mmap") == 0) {
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
    if (num == -1) {
        syslog(LOG_ERR, "Failed to scan files for filter '%s'. %s",
                f->filter_name, strerror(errno));
        return -1;
    }
    syslog(LOG_INFO, "Found %d files for filter %s.", num, f->filter_name);

    // Speical case when there are no filters
    if (num == 0) {
        int res = create_sbf(f, 0, NULL);
        return res;
    }

    // Allocate space for all the filter
    bloom_bitmap **maps = malloc(num * sizeof(bloom_bitmap*));
    bloom_bloomfilter **filters = malloc(num * sizeof(bloom_bloomfilter*));

    // Initialize the bitmaps and bloom filters
    int res;
    int err = 0;
    uint64_t size;
    bitmap_mode mode = (f->config->use_mmap) ? SHARED : PERSISTENT;
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
        bloom_bitmap *bitmap = maps[num - i - 1] = malloc(sizeof(bloom_bitmap));
        res = bitmap_from_filename(bitmap_path, size, 0, mode, bitmap);
        if (res != 0) {
            err = 1;
            syslog(LOG_ERR, "Failed to load bitmap for: %s. %s", bitmap_path, strerror(errno));
            free(bitmap);
            free(bitmap_path);
            break;
        }

        // Create the bloom filter
        bloom_bloomfilter *filter = filters[num - i - 1] = malloc(sizeof(bloom_bloomfilter));
        res = bf_from_bitmap(bitmap, 1, 0, filter);
        if (res != 0) {
            err = 1;
            syslog(LOG_ERR, "Failed to load bloom filter for: %s. [%d]", bitmap_path, res);
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
    for (int i=0; i < num; i++) free(namelist[i]);
    free(namelist);

    // Return if there was an error
    if (err) return -1;

    // Create the SBF
    res = create_sbf(f, num, filters);

    // Cleanup on err
    if (res != 0) {
        syslog(LOG_ERR, "Failed to make scalable bloom filter for: %s.", f->filter_name);

        // For fucks sake. We need to clean up so much shit now.
        for (int i=0; i < num; i++) {
            bf_close(filters[i]);
            bitmap_close(maps[i]);
            free(filters[i]);
            free(maps[i]);
        }
    }

    // Increase our page ins
    f->counters.page_ins += 1;

    // Remove the filters list
    free(maps);
    free(filters);
    return (err) ? -1 : 0;
}

/**
 * Internal method to create the SBF
 */
static int create_sbf(bloom_filter *f, int num, bloom_bloomfilter **filters) {
    // Setup the SBF params
    bloom_sbf_params params = {
        f->filter_config.initial_capacity,
        f->filter_config.default_probability,
        f->filter_config.scale_size,
        f->filter_config.probability_reduction
    };

    // Create the SBF
    f->sbf = malloc(sizeof(bloom_sbf));
    int res = sbf_from_filters(&params, bloomf_sbf_callback, f, num, filters, (bloom_sbf*)f->sbf);

    // Handle a failure
    if (res != 0) {
        syslog(LOG_ERR, "Failed to create SBF: %s. Err: %d", f->filter_name, res);
        free((bloom_sbf*)f->sbf);
        f->sbf = NULL;
    } else {
        syslog(LOG_INFO, "Loaded SBF: %s. Num filters: %d.", f->filter_name, num);
    }

    return res;
}

/**
 * Callback used with SBF to generate file names.
 */
static int bloomf_sbf_callback(void* in, uint64_t bytes, bloom_bitmap *out) {
    // Cast the input pointer
    bloom_filter *filt = in;

    // Check if we are in-memory
    if (filt->filter_config.in_memory) {
        syslog(LOG_INFO, "Creating new in-memory bitmap for filter %s. Size: %llu",
            filt->filter_name, (unsigned long long)bytes);
        return bitmap_from_file(-1, bytes, ANONYMOUS, out);
    }

    // Scan through the folder looking for data files
    struct dirent **namelist = NULL;
    int num_files;

    // Filter only data dirs, in sorted order
    num_files = scandir(filt->full_path, &namelist, filter_data_files, NULL);
    syslog(LOG_INFO, "Found %d files for filter %s.", num_files, filt->filter_name);
    if (num_files < 0) {
        syslog(LOG_ERR, "Error discovering files for filter '%s'. %s",
                filt->filter_name, strerror(errno));
        return -1;
    }


    // Free the memory associated with scandir
    for (int i=0; i < num_files; i++) {
        free(namelist[i]);
    }
    if (namelist) free(namelist);

    // Generate the new file name
    char *filename = NULL;
    int file_name_len;
    file_name_len = asprintf(&filename, DATA_FILE_NAME, num_files);
    assert(file_name_len != -1);

    // Get the full path
    char *full_path = join_path(filt->full_path, filename);
    free(filename);
    syslog(LOG_INFO, "Creating new file: %s for filter %s. Size: %llu",
            full_path, filt->filter_name, (unsigned long long)bytes);

    // Create the bitmap
    bitmap_mode mode = (filt->config->use_mmap) ? SHARED : PERSISTENT;
    int res = bitmap_from_filename(full_path, bytes, 1, mode, out);
    if (res) {
        syslog(LOG_CRIT, "Failed to create new file: %s for filter %s. Err: %s",
            full_path, filt->filter_name, strerror(errno));
    }
    free(full_path);
    return res;
}

/**
 * Computes the difference in time in milliseconds
 * between two timeval structures.
 */
static int timediff_msec(struct timeval *t1, struct timeval *t2) {
    uint64_t micro1 = t1->tv_sec * 1000000 + t1->tv_usec;
    uint64_t micro2= t2->tv_sec * 1000000 + t2->tv_usec;
    return (micro2-micro1) / 1000;
}

