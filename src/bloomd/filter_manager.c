#include <stdlib.h>
#include <stdio.h>
#include <syslog.h>
#include <pthread.h>
#include <dirent.h>
#include <string.h>
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
    volatile int is_active;         // Set to 0 when we are trying to delete it
    volatile int32_t ref_count;     // Used to manage outstanding handles
    volatile int is_hot;            // Used to mark a filter as hot

    bloom_filter *filter;    // The actual filter object
    pthread_rwlock_t rwlock; // Protects the filter
    bloom_config *custom;   // Custom config to cleanup
} bloom_filter_wrapper;

struct bloom_filtmgr {
    bloom_config *config;

    bloom_hashmap *filter_map;  // Maps key names -> bloom_filter_wrapper
    bloom_spinlock filter_lock; // Protects the filter map

    pthread_mutex_t create_lock; // Serializes create operatiosn
};

/*
 * Static declarations
 */
static const char FOLDER_PREFIX[] = "bloomd.";
static const int FOLDER_PREFIX_LEN = sizeof(FOLDER_PREFIX) - 1;

static bloom_filter_wrapper* take_filter(bloom_filtmgr *mgr, char *filter_name);
static void return_filter(bloom_filtmgr *mgr, char *filter_name);
static void delete_filter(bloom_filtmgr *mgr, bloom_filter_wrapper *filt, int should_delete);
static int add_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *config, int is_hot);
static int filter_map_list_cb(void *data, const char *key, void *value);
static int filter_map_list_cold_cb(void *data, const char *key, void *value);
static int filter_map_delete_cb(void *data, const char *key, void *value);
static int load_existing_filters(bloom_filtmgr *mgr);

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
    pthread_mutex_init(&m->create_lock, NULL);

    // Allocate the hash tables
    int res = hashmap_init(0, &m->filter_map);
    if (res) {
        syslog(LOG_ERR, "Failed to allocate filter hash map!");
        free(m);
        return -1;
    }

    // Discover existing filters
    load_existing_filters(m);

    // Done
    return 0;
}

/**
 * Cleanup
 * @arg mgr The manager to destroy
 * @return 0 on success.
 */
int destroy_filter_manager(bloom_filtmgr *mgr) {
    // Nuke all the keys
    hashmap_iter(mgr->filter_map, filter_map_delete_cb, mgr);

    // Destroy the hashmaps
    hashmap_destroy(mgr->filter_map);

    // Free the manager
    free(mgr);
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

    // Flush
    bloomf_flush(filt->filter);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Checks for the presence of keys in a given filter
 * @arg filter_name The name of the filter containing the keys
 * @arg keys A list of points to character arrays to check
 * @arg num_keys The number of keys to check
 * @arg result Ouput array, stores a 0 if the key does not exist
 * or 1 if the key does exist.
 * @return 0 on success, -1 if the filter does not exist.
 * -2 on internal error.
 */
int filtmgr_check_keys(bloom_filtmgr *mgr, char *filter_name, char **keys, int num_keys, char *result) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Acquire the write lock
    pthread_rwlock_rdlock(&filt->rwlock);

    // Check the keys, store the results
    int res = 0;
    for (int i=0; i<num_keys; i++) {
        res = bloomf_contains(filt->filter, keys[i]);
        if (res == -1) break;
        *(result+i) = res;
    }

    // Mark as hot
    filt->is_hot = 1;

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

    // Return the filter
    return_filter(mgr, filter_name);
    return (res == -1) ? -2 : 0;
}

/**
 * Sets keys in a given filter
 * @arg filter_name The name of the filter
 * @arg keys A list of points to character arrays to add
 * @arg num_keys The number of keys to add
 * @arg result Ouput array, stores a 0 if the key already is set
 * or 1 if the key is set.
 * * @return 0 on success, -1 if the filter does not exist.
 * -2 on internal error.
 */
int filtmgr_set_keys(bloom_filtmgr *mgr, char *filter_name, char **keys, int num_keys, char *result) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Acquire the write lock
    pthread_rwlock_wrlock(&filt->rwlock);

    // Set the keys, store the results
    int res = 0;
    for (int i=0; i<num_keys; i++) {
        res = bloomf_add(filt->filter, keys[i]);
        if (res == -1) break;
        *(result+i) = res;
    }

    // Mark as hot
    filt->is_hot = 1;

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

    // Return the filter
    return_filter(mgr, filter_name);
    return (res == -1) ? -2 : 0;
}

/**
 * Creates a new filter of the given name and parameters.
 * @arg filter_name The name of the filter
 * @arg custom_config Optional, can be null. Configs that override the defaults.
 * @return 0 on success, -1 if the filter already exists.
 * -2 for internal error.
 */
int filtmgr_create_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *custom_config) {
    // Store our result
    int res = 0;

    // Lock the creation
    pthread_mutex_lock(&mgr->create_lock);

    /*
     * Check if it already exists.
     * Don't use take_filter, since we don't want to increment
     * the ref count or check is_active
     */
    bloom_filter_wrapper *filt = NULL;
    LOCK_BLOOM_SPIN(&mgr->filter_lock);
    hashmap_get(mgr->filter_map, filter_name, (void**)&filt);
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);

    // Only continue if it does not exist
    if (!filt) {
        // Use a custom config if provided, else the default
        bloom_config *config = (custom_config) ? custom_config : mgr->config;

        // Add the filter
        res = add_filter(mgr, filter_name, config, 1);
        if (res != 0) res = -2; // Internal error
    } else
        res = -1; // Already exists

    // Release the lock
    pthread_mutex_unlock(&mgr->create_lock);
    return res;
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

    // Only do it if we are not in memory
    if (!filt->filter->filter_config.in_memory) {
        // Acquire the write lock
        pthread_rwlock_wrlock(&filt->rwlock);

        // Close the filter
        bloomf_close(filt->filter);

        // Release the lock
        pthread_rwlock_unlock(&filt->rwlock);
    }

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}

/**
 * Allocates space for and returns a linked
 * list of all the filters.
 * @arg mgr The manager to list from
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int filtmgr_list_filters(bloom_filtmgr *mgr, bloom_filter_list_head **head) {
    // Allocate the head
    bloom_filter_list_head *h = *head = calloc(1, sizeof(bloom_filter_list_head));

    // Iterate through a callback to append
    LOCK_BLOOM_SPIN(&mgr->filter_lock);
    hashmap_iter(mgr->filter_map, filter_map_list_cb, h);
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);

    return 0;
}


/**
 * Allocates space for and returns a linked
 * list of all the cold filters. This has the side effect
 * of clearing the list of cold filters!
 * @arg mgr The manager to list from
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int filtmgr_list_cold_filters(bloom_filtmgr *mgr, bloom_filter_list_head **head) {
    // Allocate the head of a new hashmap
    bloom_filter_list_head *h = *head = calloc(1, sizeof(bloom_filter_list_head));

    // Scan for the cold filters
    LOCK_BLOOM_SPIN(&mgr->filter_lock);
    hashmap_iter(mgr->filter_map, filter_map_list_cold_cb, h);
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);

    return 0;
}


/**
 * This method allows a callback function to be invoked with bloom filter.
 * The purpose of this is to ensure that a bloom filter is not deleted or
 * otherwise destroyed while being referenced. The filter is not locked
 * so clients should under no circumstance use this to read/write to the filter.
 * It should be used to read metrics, size information, etc.
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_filter_cb(bloom_filtmgr *mgr, char *filter_name, filter_cb cb, void* data) {
    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) return -1;

    // Callback
    cb(data, filter_name, filt->filter);

    // Return the filter
    return_filter(mgr, filter_name);
    return 0;
}


/**
 * Convenience method to cleanup a filter list.
 */
void filtmgr_cleanup_list(bloom_filter_list_head *head) {
    bloom_filter_list *next, *current = head->head;
    while (current) {
        next = current->next;
        free(current->filter_name);
        free(current);
        current = next;
    }
    free(head);
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
    int delete = 0;

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
        delete_filter(mgr, filt, 1);
    }
}

/**
 * Invoked to cleanup a filter once we
 * have hit 0 remaining references.
 * @arg should_delete Use bloomf_delete() to remove all the files
 */
static void delete_filter(bloom_filtmgr *mgr, bloom_filter_wrapper *filt, int should_delete) {
    // Delete or Close the filter
    if (should_delete)
        bloomf_delete(filt->filter);
    else
        bloomf_close(filt->filter);

    // Cleanup the filter
    destroy_bloom_filter(filt->filter);

    // Release any custom configs
    if (filt->custom) {
        free(filt->custom);
    }

    // Release the struct
    free(filt);
    return;
}

/**
 * Creates a new filter and adds it to the filter set.
 * @arg mgr The manager to add to
 * @arg filter_name The name of the filter
 * @arg config The configuration for the filter
 * @arg is_hot Is the filter hot. False for existing.
 * @return 0 on success, -1 on error
 */
static int add_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *config, int is_hot) {
    // Create the filter
    bloom_filter_wrapper *filt = calloc(1, sizeof(bloom_filter_wrapper));
    filt->ref_count = 1;
    filt->is_active = 1;
    filt->is_hot = is_hot;
    pthread_rwlock_init(&filt->rwlock, NULL);

    // Set the custom filter if its not the same
    if (mgr->config != config) {
        filt->custom = config;
    }

    // Try to create the underlying filter. Only discover if it is hot.
    int res = init_bloom_filter(config, filter_name, is_hot, &filt->filter);
    if (res != 0) {
        free(filt);
        return -1;
    }

    // Add to the hash map
    LOCK_BLOOM_SPIN(&mgr->filter_lock);
    hashmap_put(mgr->filter_map, filter_name, filt);
    UNLOCK_BLOOM_SPIN(&mgr->filter_lock);

    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list all the filters. Only works if value is
 * not NULL.
 */
static int filter_map_list_cb(void *data, const char *key, void *value) {
    // Filter out the non-active nodes
    bloom_filter_wrapper *filt = value;
    if (!filt->is_active) return 0;

    // Cast the inputs
    bloom_filter_list_head *head = data;

    // Allocate a new entry
    bloom_filter_list *node = malloc(sizeof(bloom_filter_list));

    // Setup
    node->filter_name = strdup(key);
    node->next = head->head;

    // Inject
    head->head = node;
    head->size++;
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list cold filters. Only works if value is
 * not NULL.
 */
static int filter_map_list_cold_cb(void *data, const char *key, void *value) {
    // Cast the inputs
    bloom_filter_list_head *head = data;
    bloom_filter_wrapper *filt = value;

    // Check if hot, turn off and skip
    if (filt->is_hot) {
        filt->is_hot = 0;
        return 0;
    }

    // Check if proxied
    if (bloomf_is_proxied(filt->filter)) {
        return 0;
    }

    // Allocate a new entry
    bloom_filter_list *node = malloc(sizeof(bloom_filter_list));

    // Setup
    node->filter_name = strdup(key);
    node->next = head->head;

    // Inject
    head->head = node;
    head->size++;
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to cleanup the filters.
 */
static int filter_map_delete_cb(void *data, const char *key, void *value) {
    // Cast the inputs
    bloom_filtmgr *mgr = data;
    bloom_filter_wrapper *filt = value;

    // Delete, but not the underlying files
    delete_filter(mgr, filt, 0);
    return 0;
}

/**
 * Works with scandir to filter out non-bloomd folders.
 */
#ifndef __linux__
static int filter_bloomd_folders(struct dirent *d) {
#else
static int filter_bloomd_folders(const struct dirent *d) {
#endif
    // Get the file name
    char *name = (char*)d->d_name;

    // Look if it ends in ".data"
    int name_len = strlen(name);

    // Too short
    if (name_len < 8) return 0;

    // Compare the prefix
    if (strncmp(name, FOLDER_PREFIX, FOLDER_PREFIX_LEN) == 0) {
        return 1;
    }

    // Do not store
    return 0;
}

/**
 * Loads the existing filters. This is not thread
 * safe and assumes that we are being initialized.
 */
static int load_existing_filters(bloom_filtmgr *mgr) {
    struct dirent **namelist;
    int num;

    num = scandir(mgr->config->data_dir, &namelist, filter_bloomd_folders, NULL);
    if (num == -1) {
        syslog(LOG_ERR, "Failed to scan files for existing filters!");
        return -1;
    }
    syslog(LOG_INFO, "Found %d existing filters", num);

    // Add all the filters
    for (int i=0; i< num; i++) {
        char *folder_name = namelist[i]->d_name;
        char *filter_name = folder_name + FOLDER_PREFIX_LEN;
        add_filter(mgr, filter_name, mgr->config, 0);
    }

    for (int i=0; i < num; i++) free(namelist[i]);
    free(namelist);
    return 0;
}

