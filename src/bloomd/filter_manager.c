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

#ifdef DARWIN
#include <Availability.h>
#if __MAC_OS_X_VERSION_MIN_REQUIRED < __MAC_10_8
#define CONST_DIRENT_T struct dirent
#else
#define CONST_DIRENT_T const struct dirent
#endif
#else
#define CONST_DIRENT_T const struct dirent
#endif

/**
 * Wraps a bloom_filter to ensure only a single
 * writer access it at a time. Tracks the outstanding
 * references, to allow a sane close to take place.
 */
typedef struct {
    volatile int is_active;         // Set to 0 when we are trying to delete it
    volatile int is_hot;            // Used to mark a filter as hot
    volatile int should_delete;     // Used to control deletion

    bloom_filter *filter;    // The actual filter object
    pthread_rwlock_t rwlock; // Protects the filter
    bloom_config *custom;   // Custom config to cleanup
} bloom_filter_wrapper;

/**
 * We use a linked list of filtmgr_vsn structs
 * as a simple form of Multi-Version Concurrency Controll (MVCC).
 * The latest version is always the head of the list, and older
 * versions are maintained as a linked list. A separate vacuum thread
 * is used to clean out the old version. This allows reads against the
 * head to be non-blocking.
 */
typedef struct filtmgr_vsn {
    unsigned long long vsn;

    // Maps key names -> bloom_filter_wrapper
    bloom_hashmap *filter_map;

    // Holds a reference to the deleted filter, since
    // it is no longer in the hash map
    bloom_filter_wrapper *deleted;
    struct filtmgr_vsn *prev;
} filtmgr_vsn;

struct bloom_filtmgr {
    bloom_config *config;

    filtmgr_vsn *latest;
    pthread_mutex_t write_lock; // Serializes destructive operations

    volatile int should_run;  // Used to stop the vacuum thread
    pthread_t vacuum_thread;

    /*
     * To support vacuuming of old versions, we require that
     * workers 'periodically' checkpoint. This just updates an
     * index to match the current version. The vacuum thread
     * can scan for the minimum seen version and clean all older
     * versions.
     */
    pthread_t *threads;
    unsigned long long *vsn_checkpoint;
};

/**
 * We warn if there are this many outstanding versions
 * that cannot be vacuumed
 */
#define WARN_THRESHOLD 32

/*
 * Static declarations
 */
static const char FOLDER_PREFIX[] = "bloomd.";
static const int FOLDER_PREFIX_LEN = sizeof(FOLDER_PREFIX) - 1;

static bloom_filter_wrapper* take_filter(filtmgr_vsn *vsn, char *filter_name);
static void delete_filter(bloom_filter_wrapper *filt);
static int add_filter(bloom_filtmgr *mgr, filtmgr_vsn *vsn, char *filter_name, bloom_config *config, int is_hot);
static int filter_map_list_cb(void *data, const char *key, void *value);
static int filter_map_list_cold_cb(void *data, const char *key, void *value);
static int filter_map_delete_cb(void *data, const char *key, void *value);
static int load_existing_filters(bloom_filtmgr *mgr);
static filtmgr_vsn* create_new_version(bloom_filtmgr *mgr);
static void destroy_version(filtmgr_vsn *vsn);
static int copy_hash_entries(void *data, const char *key, void *value);
static void* filtmgr_thread_main(void *in);

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

    // Initialize the write lock
    pthread_mutex_init(&m->write_lock, NULL);

    // Allocate the initial version and hash table
    filtmgr_vsn *vsn = calloc(1, sizeof(filtmgr_vsn));
    m->latest = vsn;
    int res = hashmap_init(0, &vsn->filter_map);
    if (res) {
        syslog(LOG_ERR, "Failed to allocate filter hash map!");
        free(m);
        return -1;
    }

    // Make room for the checkpoints
    m->vsn_checkpoint = calloc(config->worker_threads, sizeof(unsigned long long));

    // Discover existing filters
    load_existing_filters(m);

    // Start the vacuum thread
    m->should_run = 1;
    if (pthread_create(&m->vacuum_thread, NULL, filtmgr_thread_main, m)) {
        perror("Failed to start vacuum thread!");
        destroy_filter_manager(m);
        return 1;
    }

    // Done
    return 0;
}

/**
 * Cleanup
 * @arg mgr The manager to destroy
 * @return 0 on success.
 */
int destroy_filter_manager(bloom_filtmgr *mgr) {
    // Stop the vacuum thread
    mgr->should_run = 0;
    if (mgr->vacuum_thread) pthread_join(mgr->vacuum_thread, NULL);

    // Nuke all the keys in the current version
    filtmgr_vsn *current = mgr->latest;
    hashmap_iter(current->filter_map, filter_map_delete_cb, mgr);

    // Destroy the versions
    filtmgr_vsn *next, *vsn = mgr->latest;
    while (vsn) {
        // Handle any lingering deletes
        if (vsn->deleted) delete_filter(vsn->deleted);
        next = vsn->prev;
        destroy_version(vsn);
        vsn = next;
    }

    // Free the manager
    free(mgr->vsn_checkpoint);
    free(mgr);
    return 0;
}

/**
 * Provides a list of worker threads to the filter manager
 * @arg mgr The manager
 * @arg threads A list of thread IDs, should be `worker_threads` long
 */
void filtmgr_provide_workers(bloom_filtmgr *mgr, pthread_t *threads) {
    // Maintain a reference to the threads, nothing crazy...
    mgr->threads = threads;
}

/**
 * Should be invoked periodically by worker threads to allow
 * the vacuum thread to cleanup garbage state.
 * @arg mgr The manager
 */
void filtmgr_worker_checkpoint(bloom_filtmgr *mgr) {
    // Skip if we don't have the thread list yet
    if (!mgr->threads) return;

    // Get a reference to ourself
    pthread_t id = pthread_self();

    // Look for the matching index
    // This is O(n), but N is small, and this should be done
    // relatively infrequently...
    for (int idx=0; idx < mgr->config->worker_threads; idx++) {
        if (pthread_equal(id, mgr->threads[idx])) {
            // Update the checkpoint version
            mgr->vsn_checkpoint[idx] = mgr->latest->vsn;
            break;
        }
    }
}

/**
 * Flushes the filter with the given name
 * @arg filter_name The name of the filter to flush
 * @return 0 on success. -1 if the filter does not exist.
 */
int filtmgr_flush_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Get the filter
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
    if (!filt) return -1;

    // Flush
    bloomf_flush(filt->filter);
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
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
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
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
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
    // Lock the creation
    pthread_mutex_lock(&mgr->write_lock);

    // Bail if the filter already exists
    bloom_filter_wrapper *filt = NULL;
    filtmgr_vsn *latest = mgr->latest;
    hashmap_get(latest->filter_map, filter_name, (void**)&filt);
    if (filt) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -1;
    }

    // Create a new version
    filtmgr_vsn *new_vsn = create_new_version(mgr);

    // Use a custom config if provided, else the default
    bloom_config *config = (custom_config) ? custom_config : mgr->config;

    // Add the filter to the new version
    int res = add_filter(mgr, new_vsn, filter_name, config, 1);
    if (res != 0) {
        destroy_version(new_vsn);
        res = -2; // Internal error
    } else {
        // Install the new version
        mgr->latest = new_vsn;
    }

    // Release the lock
    pthread_mutex_unlock(&mgr->write_lock);
    return res;
}

/**
 * Deletes the filter entirely. This removes it from the filter
 * manager and deletes it from disk. This is a permanent operation.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_drop_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Lock the deletion
    pthread_mutex_lock(&mgr->write_lock);

    // Get the filter
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
    if (!filt) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -1;
    }

    // Set the filter to be non-active and mark for deletion
    filt->is_active = 0;
    filt->should_delete = 1;

    // Create a new version without this filter
    filtmgr_vsn *new_vsn = create_new_version(mgr);
    hashmap_delete(new_vsn->filter_map, filter_name);
    current->deleted = filt;

    // Install the new version
    mgr->latest = new_vsn;

    // Unlock
    pthread_mutex_unlock(&mgr->write_lock);
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
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
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

    return 0;
}


/**
 * Clears the filter from the internal data stores. This can only
 * be performed if the filter is proxied.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist, -2
 * if the filter is not proxied.
 */
int filtmgr_clear_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Lock the deletion
    pthread_mutex_lock(&mgr->write_lock);

    // Get the filter
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
    if (!filt) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -1;
    }

    // Check if the filter is proxied
    if (!bloomf_is_proxied(filt->filter)) {
        pthread_mutex_unlock(&mgr->write_lock);
        return -2;
    }

    // This is critical, as it prevents it from
    // being deleted. Instead, it is merely closed.
    filt->is_active = 0;
    filt->should_delete = 0;

    // Create a new version without this filter
    filtmgr_vsn *new_vsn = create_new_version(mgr);
    hashmap_delete(new_vsn->filter_map, filter_name);
    current->deleted = filt;

    // Install the new version
    mgr->latest = new_vsn;

    // Unlock
    pthread_mutex_unlock(&mgr->write_lock);
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
    filtmgr_vsn *current = mgr->latest;
    hashmap_iter(current->filter_map, filter_map_list_cb, h);
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
    filtmgr_vsn *current = mgr->latest;
    hashmap_iter(current->filter_map, filter_map_list_cold_cb, h);
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
    filtmgr_vsn *current = mgr->latest;
    bloom_filter_wrapper *filt = take_filter(current, filter_name);
    if (!filt) return -1;

    // Callback
    cb(data, filter_name, filt->filter);
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
static bloom_filter_wrapper* take_filter(filtmgr_vsn *vsn, char *filter_name) {
    bloom_filter_wrapper *filt = NULL;
    hashmap_get(vsn->filter_map, filter_name, (void**)&filt);
    return (filt && filt->is_active) ? filt : NULL;
}


/**
 * Invoked to cleanup a filter once we
 * have hit 0 remaining references.
 */
static void delete_filter(bloom_filter_wrapper *filt) {
    // Delete or Close the filter
    if (filt->should_delete)
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
 * @arg vsn The version to add to
 * @arg filter_name The name of the filter
 * @arg config The configuration for the filter
 * @arg is_hot Is the filter hot. False for existing.
 * @return 0 on success, -1 on error
 */
static int add_filter(bloom_filtmgr *mgr, filtmgr_vsn *vsn, char *filter_name, bloom_config *config, int is_hot) {
    // Create the filter
    bloom_filter_wrapper *filt = calloc(1, sizeof(bloom_filter_wrapper));
    filt->is_active = 1;
    filt->is_hot = is_hot;
    filt->should_delete = 0;
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
    if (!hashmap_put(vsn->filter_map, filter_name, filt)) {
        destroy_bloom_filter(filt->filter);
        free(filt);
        return -1;
    }
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
    (void)data;
    (void)key;

    // Cast the inputs
    bloom_filter_wrapper *filt = value;

    // Delete, but not the underlying files
    filt->should_delete = 0;
    delete_filter(filt);
    return 0;
}

/**
 * Works with scandir to filter out non-bloomd folders.
 */
static int filter_bloomd_folders(CONST_DIRENT_T *d) {
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
        if (add_filter(mgr, mgr->latest, filter_name, mgr->config, 0)) {
            syslog(LOG_ERR, "Failed to load filter '%s'!", filter_name);
        }
    }

    for (int i=0; i < num; i++) free(namelist[i]);
    free(namelist);
    return 0;
}


/**
 * Creates a new version struct from the current version.
 * Does not install the new version in place. This should
 * be guarded by the write lock to prevent conflicting versions.
 */
static filtmgr_vsn* create_new_version(bloom_filtmgr *mgr) {
    // Create a new blank version
    filtmgr_vsn *vsn = calloc(1, sizeof(filtmgr_vsn));

    // Increment the version number
    filtmgr_vsn *current = mgr->latest;
    vsn->vsn = mgr->latest->vsn + 1;

    // Set the previous pointer
    vsn->prev = current;

    // Initialize the hashmap
    int res = hashmap_init(hashmap_size(current->filter_map), &vsn->filter_map);
    if (res) {
        syslog(LOG_ERR, "Failed to allocate new filter hash map!");
        free(vsn);
        return NULL;
    }

    // Copy old keys, this is likely a bottle neck...
    // We need to make this more efficient in the future.
    res = hashmap_iter(current->filter_map, copy_hash_entries, vsn->filter_map);
    if (res != 0) {
        syslog(LOG_ERR, "Failed to copy filter hash map!");
        hashmap_destroy(vsn->filter_map);
        free(vsn);
        return NULL;
    }

    // Return the new version
    syslog(LOG_DEBUG, "(FiltMgr) Created new version %llu", vsn->vsn);
    return vsn;
}

// Destroys a version. Does basic cleanup.
static void destroy_version(filtmgr_vsn *vsn) {
    hashmap_destroy(vsn->filter_map);
    free(vsn);
}

// Copies entries from an existing map into a new one
static int copy_hash_entries(void *data, const char *key, void *value) {
    bloom_hashmap *new = data;
    return (hashmap_put(new, (char*)key, value) ? 0 : 1);
}


// Recursively waits and cleans up old versions
static int clean_old_versions(filtmgr_vsn *v, unsigned long long min_vsn) {
    // Recurse if possible
    if (v->prev && clean_old_versions(v->prev, min_vsn))
        v->prev = NULL;

    // Abort if this version cannot be cleaned
    if (v->vsn >= min_vsn) return 0;

    // Log about the cleanup
    syslog(LOG_DEBUG, "(FiltMgr) Destroying version %llu", v->vsn);

    // Handle the cleanup
    if (v->deleted) {
        delete_filter(v->deleted);
    }

    // Destroy this version
    destroy_version(v);
    return 1;
}


/**
 * This thread is started after initialization to maintain
 * the state of the filter manager. It's current use is to
 * cleanup the garbage created by our MVCC model. We do this
 * by making use of periodic 'checkpoints'. Our worker threads
 * report the version they are currently using, and we are always
 * able to delete versions that are strictly less than the minimum.
 */
static void* filtmgr_thread_main(void *in) {
    // Extract our arguments
    bloom_filtmgr *mgr = in;

    // Store the oldest version
    filtmgr_vsn *current;
    while (mgr->should_run) {
        sleep(1);
        if (!mgr->should_run) break;

        // Do nothing if there is no older versions
        current = mgr->latest;
        if (!current->prev) continue;

        // Determine the minimum version
        unsigned long long thread_vsn, min_vsn = current->vsn;
        for (int i=0; i < mgr->config->worker_threads; i++) {
            thread_vsn = mgr->vsn_checkpoint[i];
            if (thread_vsn < min_vsn) {
                min_vsn = thread_vsn;
            }
        }

        // Check if it is too old
        if (current->vsn - min_vsn > WARN_THRESHOLD) {
            syslog(LOG_WARNING, "Many concurrent versions detected! Either slow operations, or too many changes! Current: %llu, Minimum: %llu", current->vsn, min_vsn);
        }

        // Cleanup the old versions
        clean_old_versions(current, min_vsn);
    }
    return NULL;
}

