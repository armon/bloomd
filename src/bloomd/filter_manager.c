#include <stdlib.h>
#include <stdio.h>
#include <syslog.h>
#include <pthread.h>
#include "spinlock.h"
#include "filter_manager.h"
#include "hashmap.h"
#include "filter.h"

/**
 * Wraps a bloom_filter to ensure only a single
 * writer access it at a time. Tracks the outstanding
 * references, to allow a sane close to take place.
 */
typedef struct {
    volatile int is_active;          // Set to 0 when we are trying to delete it
    volatile int32_t ref_count;     // Used to manage outstanding handles

    bloom_filter *filter;    // The actual filter object
    pthread_rwlock_t rwlock; // Protects the filter
} bloom_filter_wrapper;

struct bloom_filtmgr {
    bloom_config *config;

    bloom_hashmap *filter_map;  // Maps key names -> bloom_filter_wrapper
    bloom_spinlock filter_lock; // Protects the filter map

    bloom_hashmap *hot_filters; // Maps key names of hot filters
    bloom_spinlock hot_lock;    // Protects the hot filters
};

/*
 * Static declarations
 */
static void add_hot_filter(bloom_filtmgr *mgr, char *filter_name);
static bloom_filter_wrapper* take_filter(bloom_filtmgr *mgr, char *filter_name);
static void return_filter(bloom_filtmgr *mgr, char *filter_name);
static void delete_filter(bloom_filtmgr *mgr, bloom_filter_wrapper *filt);

/**
 * Initializer
 * @arg config The configuration
 * @arg mgr Output, resulting manager.
 * @return 0 on success.
 */
int init_filter_manager(bloom_config *config, bloom_filtmgr **mgr) {
    // Allocate a new object
    bloom_filtmgr *m = *mgr = calloc(1, sizeof(bloom_filtmgr));

    // Copy the config
    m->config = config;

    // Initialize the locks
    INIT_BLOOM_SPIN(&m->filter_lock);
    INIT_BLOOM_SPIN(&m->hot_lock);

    // Allocate the hash tables
    int res = hashmap_init(0, &m->filter_map);
    if (!res) {
        syslog(LOG_ERR, "Failed to allocate filter hash map!");
        free(m);
        return -1;
    }
    res = hashmap_init(0, &m->hot_filters);
    if (!res) {
        syslog(LOG_ERR, "Failed to allocate hot filter hash map!");
        hashmap_destroy(m->filter_map);
        free(m);
        return -1;
    }

    // TODO: Discover existing filters...

    // Done
    return 0;
}

/**
 * Cleanup
 * @arg mgr The manager to destroy
 * @return 0 on success.
 */
int destroy_filter_manager(bloom_filtmgr *mgr) {
    // TODO: TUBEZ
    return 0;
}

/**
 * Flushes the filter with the given name
 * @arg filter_name The name of the filter to flush
 * @return 0 on success. -1 if the filter does not exist.
 */
int filtmgr_flush_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Acquire the write lock
    pthread_rwlock_rdlock(&filt->rwlock);

    // Flush
    bloomf_flush(filt->filter);

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

    // Mark as hot
    add_hot_filter(mgr, filter_name);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Unmaps all the cold filters.
 * @return 0 on success.
 */
int filtmgr_unmap_cold(bloom_filtmgr *mgr) {
    // TODO: TUBEZ
    return 0;
}

/**
 * Checks the number of mapped filters
 * @return The number of mapped filters.
 */
int filtmgr_num_filters(bloom_filtmgr *mgr) {
    return hashmap_size(mgr->filter_map);
}

/**
 * Checks for the presence of keys in a given filter
 * @arg filter_name The name of the filter containing the keys
 * @arg keys A list of points to character arrays to check
 * @arg num_keys The number of keys to check
 * @arg result Ouput array, stores a 0 if the key does not exist
 * or 1 if the key does exist.
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_check_keys(bloom_filtmgr *mgr, char *filter_name, char **keys, int num_keys, char *result) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Acquire the write lock
    pthread_rwlock_rdlock(&filt->rwlock);

    // TODO: Check the keys

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

    // Mark as hot
    add_hot_filter(mgr, filter_name);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Sets keys in a given filter
 * @arg filter_name The name of the filter
 * @arg keys A list of points to character arrays to add
 * @arg num_keys The number of keys to add
 * @arg result Ouput array, stores a 0 if the key already is set
 * or 1 if the key is set.
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_set_keys(bloom_filtmgr *mgr, char *filter_name, char **keys, int num_keys, char *result) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Acquire the write lock
    pthread_rwlock_wrlock(&filt->rwlock);

    // TODO: Set the keys

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

    // Mark as hot
    add_hot_filter(mgr, filter_name);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Creates a new filter of the given name and parameters.
 * @arg filter_name The name of the filter
 * @arg custom_config Optional, can be null. Configs that override the defaults.
 * @return 0 on success, -1 if the filter already exists.
 */
int filtmgr_create_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *custom_config) {
    // TODO: Create filters
    return 0;
}

/**
 * Deletes the filter entirely. This removes it from the filter
 * manager and deletes it from disk. This is a permanent operation.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_drop_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Decrement the ref count and set to non-active
    LOCK_BLOOM_SPIN(&mgr->filter_lock);
    filt->ref_count--;
    filt->is_active = 0;
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Unmaps the filter from memory, but leaves it
 * registered in the filter manager. This is rarely invoked
 * by a client, as it can be handled automatically by bloomd,
 * but particular clients with specific needs may use it as an
 * optimization.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_unmap_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Acquire the write lock
    pthread_rwlock_wrlock(&filt->rwlock);

    // Close the filter
    bloomf_close(filt->filter);

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Marks a filter as hot. Does it in a thread safe way.
 * @arg mgr The manager
 * @arg filter_name The name of the hot filter
 */
static void add_hot_filter(bloom_filtmgr *mgr, char *filter_name) {
    LOCK_BLOOM_SPIN(&mgr->hot_lock);
    hashmap_put(mgr->hot_filters, filter_name, NULL);
    UNLOCK_BLOOM_SPIN(&mgr->hot_lock);
}

/**
 * Gets the bloom filter in a thread safe way.
 */
static bloom_filter_wrapper* take_filter(bloom_filtmgr *mgr, char *filter_name) {
    bloom_filter_wrapper *filt = NULL;
    LOCK_BLOOM_SPIN(&mgr->filter_lock);
    hashmap_get(mgr->filter_map, filter_name, (void**)&filt);
    if (filt && filt->is_active) {
        filt->ref_count++;
    }
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);
    return (filt && filt->is_active) ? filt : NULL;
}

/**
 * Returns the bloom filter in a thread safe way.
 */
static void return_filter(bloom_filtmgr *mgr, char *filter_name) {
    bloom_filter_wrapper *filt = NULL;
    int delete;

    // Lock the filters
    LOCK_BLOOM_SPIN(&mgr->filter_lock);

    // Lookup the filter
    hashmap_get(mgr->filter_map, filter_name, (void**)&filt);

    // If it exists, decrement the ref count
    if (filt) {
        int ref_count = (--filt->ref_count);

        // If we've hit 0 references, delete from the
        // filter, and prepare to handle it
        if (ref_count <= 0) {
            hashmap_delete(mgr->filter_map, filter_name);
            delete = 1;
        }
    }

    // Unlock
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);

    // Handle the deletion
    if (delete)  {
        delete_filter(mgr, filt);
    }
}

/**
 * Invoked to cleanup a filter once we
 * have hit 0 remaining references.
 */
static void delete_filter(bloom_filtmgr *mgr, bloom_filter_wrapper *filt) {
    // TODO: Delete the shit
    return;
}

