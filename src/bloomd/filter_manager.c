#include <stdlib.h>
#include <stdio.h>
#include <syslog.h>
#include <pthread.h>
#include <dirent.h>
#include <string.h>
#include "spinlock.h"
#include "filter_manager.h"
#include "art.h"
#include "filter.h"
#include "type_compat.h"

/**
 * This defines how log we sleep between vacuum poll
 * iterations in microseconds
 */
#define VACUUM_POLL_USEC 500000

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
 * We use a linked list of filtmgr_client
 * structs to track any clients of the filter manager.
 * Each client maintains a thread ID as well as the
 * last known version they used. The vacuum thread
 * uses this information to safely garbage collect
 * old versions.
 */
typedef struct filtmgr_client {
    pthread_t id;
    unsigned long long vsn;
    struct filtmgr_client *next;
} filtmgr_client;

// Enum of possible delta updates
typedef enum {
    CREATE,
    DELETE,
    BARRIER
} delta_type;

// Simple linked list of filter wrappers
typedef struct filter_list {
    unsigned long long vsn;
    delta_type type;
    bloom_filter_wrapper *filter;
    struct filter_list *next;
} filter_list;

/**
 * We use a a simple form of Multi-Version Concurrency Controll (MVCC)
 * to prevent locking on access to the map of filter name -> bloom_filter_wrapper.
 *
 * The way it works is we have 2 ART trees, the "primary" and an alternate.
 * All the clients of the filter manager have reads go through the primary
 * without any locking. We also maintain a delta list of new and deleted filters,
 * which is a simple linked list.
 *
 * We use a separate vacuum thread to merge changes from the delta lists
 * into the alternate tree, and then do a pointer swap to rotate the
 * primary tree for the alternate.
 *
 * This mechanism ensures we have at most 2 ART trees, reads are lock-free,
 * and performance does not degrade with the number of filters.
 *
 */
struct bloom_filtmgr {
    bloom_config *config;

    int should_run;  // Used to stop the vacuum thread
    pthread_t vacuum_thread;

    /*
     * To support vacuuming of old versions, we require that
     * workers 'periodically' checkpoint. This just updates an
     * index to match the current version. The vacuum thread
     * can scan for the minimum seen version and clean all older
     * versions.
     */
    filtmgr_client *clients;
    bloom_spinlock clients_lock;

    // This is the current version. Should be used under the write lock.
    unsigned long long vsn;
    pthread_mutex_t write_lock;

    // Maps key names -> bloom_filter_wrapper
    unsigned long long primary_vsn; // This is the version that filter_map represents
    art_tree *filter_map;
    art_tree *alt_filter_map;

    /**
     * List of pending deletes. This is necessary
     * because the filter may reflect that a delete has
     * taken place, while the vacuum thread has not yet performed the
     * delete. This allows create to return a "Delete in progress".
     */
    bloom_filter_list *pending_deletes;
    bloom_spinlock pending_lock;

    // Delta lists for non-merged operations
    filter_list *delta;
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

static bloom_filter_wrapper* find_filter(bloom_filtmgr *mgr, char *filter_name);
static bloom_filter_wrapper* take_filter(bloom_filtmgr *mgr, char *filter_name);
static void delete_filter(bloom_filter_wrapper *filt);
static int add_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *config, int is_hot, int delta);
static int filter_map_list_cb(void *data, const unsigned char *key, uint32_t key_len, void *value);
static int filter_map_list_cold_cb(void *data, const unsigned char *key, uint32_t key_len, void *value);
static int filter_map_delete_cb(void *data, const unsigned char *key, uint32_t key_len, void *value);
static int load_existing_filters(bloom_filtmgr *mgr);
static unsigned long long create_delta_update(bloom_filtmgr *mgr, delta_type type, bloom_filter_wrapper *filt);
static void* filtmgr_thread_main(void *in);

/**
 * Initializer
 * @arg config The configuration
 * @arg vacuum Should vacuuming be enabled. True unless in a
 * test or embedded environment using filtmgr_vacuum()
 * @arg mgr Output, resulting manager.
 * @return 0 on success.
 */
int init_filter_manager(bloom_config *config, int vacuum, bloom_filtmgr **mgr) {
    // Allocate a new object
    bloom_filtmgr *m = *mgr = calloc(1, sizeof(bloom_filtmgr));

    // Copy the config
    m->config = config;

    // Initialize the locks
    pthread_mutex_init(&m->write_lock, NULL);
    INIT_BLOOM_SPIN(&m->clients_lock);
    INIT_BLOOM_SPIN(&m->pending_lock);

    // Allocate storage for the art trees
    art_tree *trees = calloc(2, sizeof(art_tree));
    m->filter_map = trees;
    m->alt_filter_map = trees+1;

    // Allocate the initial art tree
    int res = init_art_tree(m->filter_map);
    if (res) {
        syslog(LOG_ERR, "Failed to allocate filter map!");
        free(m);
        return -1;
    }

    // Discover existing filters
    load_existing_filters(m);

    // Initialize the alternate map
    res = art_copy(m->alt_filter_map, m->filter_map);
    if (res) {
        syslog(LOG_ERR, "Failed to copy filter map to alternate!");
        destroy_filter_manager(m);
        return -1;
    }

    // Start the vacuum thread
    m->should_run = vacuum;
    if (vacuum && pthread_create(&m->vacuum_thread, NULL, filtmgr_thread_main, m)) {
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

    // Nuke all the keys in the current version.
    art_iter(mgr->filter_map, filter_map_delete_cb, mgr);

    // Handle any delta operations
    filter_list *next, *current = mgr->delta;
    while (current) {
        // Only delete pending creates, pending
        // deletes are still in the primary tree
        if (current->type == CREATE)
            delete_filter(current->filter);
        next = current->next;
        free(current);
        current = next;
    }

    // Free the clients
    filtmgr_client *cl_next, *cl = mgr->clients;
    while (cl) {
        cl_next = cl->next;
        free(cl);
        cl = cl_next;
    }

    // Destroy the ART trees
    destroy_art_tree(mgr->filter_map);
    destroy_art_tree(mgr->alt_filter_map);
    free((mgr->filter_map < mgr->alt_filter_map) ? mgr->filter_map : mgr->alt_filter_map);

    // Free the manager
    free(mgr);
    return 0;
}

/**
 * Should be invoked periodically by client threads to allow
 * the vacuum thread to cleanup garbage state. It should also
 * be called before making other calls into the filter manager
 * so that it is aware of a client making use of the current
 * state.
 * @arg mgr The manager
 */
void filtmgr_client_checkpoint(bloom_filtmgr *mgr) {
    // Get a reference to ourself
    pthread_t id = pthread_self();

    // Look for our ID, and update the version
    // This is O(n), but N is small and its done infrequently
    filtmgr_client *cl = mgr->clients;
    while (cl) {
        if (cl->id == id) {
            cl->vsn = mgr->vsn;
            return;
        }
        cl = cl->next;
    }

    // If we make it here, we are not a client yet
    // so we need to safely add ourself
    cl = malloc(sizeof(filtmgr_client));
    cl->id = id;
    cl->vsn = mgr->vsn;

    // Critical section for the flip
    LOCK_BLOOM_SPIN(&mgr->clients_lock);

    cl->next = mgr->clients;
    mgr->clients = cl;

    UNLOCK_BLOOM_SPIN(&mgr->clients_lock);
}

/**
 * Should be invoked by clients when they no longer
 * need to make use of the filter manager. This
 * allows the vacuum thread to cleanup garbage state.
 * @arg mgr The manager
 */
void filtmgr_client_leave(bloom_filtmgr *mgr) {
    // Get a reference to ourself
    pthread_t id = pthread_self();

    // Critical section
    LOCK_BLOOM_SPIN(&mgr->clients_lock);

    // Look for our ID, and update the version
    // This is O(n), but N is small and its done infrequently
    filtmgr_client **last_next = &mgr->clients;
    filtmgr_client *cl = mgr->clients;
    while (cl) {
        if (cl->id == id) {
            // Set the last prev pointer to skip the current entry
            *last_next = cl->next;

            // Cleanup the memory associated
            free(cl);
            break;
        }
        last_next = &cl->next;
        cl = cl->next;
    }
    UNLOCK_BLOOM_SPIN(&mgr->clients_lock);
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
    pthread_rwlock_wrlock(&filt->rwlock);

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
    return (res == -1) ? -2 : 0;
}

/**
 * Creates a new filter of the given name and parameters.
 * @arg filter_name The name of the filter
 * @arg custom_config Optional, can be null. Configs that override the defaults.
 * @return 0 on success, -1 if the filter already exists.
 * -2 for internal error. -3 if there is a pending delete.
 */
int filtmgr_create_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *custom_config) {
    int res = 0;
    pthread_mutex_lock(&mgr->write_lock);

    // Bail if the filter already exists.
    bloom_filter_wrapper *filt = find_filter(mgr, filter_name);
    if (filt) {
        res = (filt->is_active) ? -1 : -3;
        goto LEAVE;
    }

    // Scan the pending delete queue
    LOCK_BLOOM_SPIN(&mgr->pending_lock);
    if (mgr->pending_deletes) {
        bloom_filter_list *node = mgr->pending_deletes;
        while (node) {
            if (!strcmp(node->filter_name, filter_name)) {
                res = -3; // Pending delete
                UNLOCK_BLOOM_SPIN(&mgr->pending_lock);
                goto LEAVE;
            }
            node = node->next;
        }
    }
    UNLOCK_BLOOM_SPIN(&mgr->pending_lock);

    // Use a custom config if provided, else the default
    bloom_config *config = (custom_config) ? custom_config : mgr->config;

    // Add the filter to the new version
    if (add_filter(mgr, filter_name, config, 1, 1)) {
        res = -2; // Internal error
    }

LEAVE:
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
    int res = 0;
    pthread_mutex_lock(&mgr->write_lock);

    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) {
        res = -1;
        goto LEAVE;
    }

    // Set the filter to be non-active and mark for deletion
    filt->is_active = 0;
    filt->should_delete = 1;
    create_delta_update(mgr, DELETE, filt);

LEAVE:
    pthread_mutex_unlock(&mgr->write_lock);
    return res;
}

/**
 * Clears the filter from the internal data stores. This can only
 * be performed if the filter is proxied.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist, -2
 * if the filter is not proxied.
 */
int filtmgr_clear_filter(bloom_filtmgr *mgr, char *filter_name) {
    int res = 0;
    pthread_mutex_lock(&mgr->write_lock);

    // Get the filter
    bloom_filter_wrapper *filt = take_filter(mgr, filter_name);
    if (!filt) {
        res = -1;
        goto LEAVE;
    }

    // Check if the filter is proxied
    if (!bloomf_is_proxied(filt->filter)) {
        res = -2;
        goto LEAVE;
    }

    // This is critical, as it prevents it from
    // being deleted. Instead, it is merely closed.
    filt->is_active = 0;
    filt->should_delete = 0;
    create_delta_update(mgr, DELETE, filt);

LEAVE:
    pthread_mutex_unlock(&mgr->write_lock);
    return res;
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

    // Skip if we are in memory
    if (filt->filter->filter_config.in_memory)
        goto LEAVE;

    // Acquire the write lock
    pthread_rwlock_wrlock(&filt->rwlock);

    // Close the filter
    bloomf_close(filt->filter);

    // Release the lock
    pthread_rwlock_unlock(&filt->rwlock);

LEAVE:
    return 0;
}

/**
 * Allocates space for and returns a linked
 * list of all the filters.
 * @arg mgr The manager to list from
 * @arg prefix The prefix to list or NULL
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int filtmgr_list_filters(bloom_filtmgr *mgr, char *prefix, bloom_filter_list_head **head) {
    // Allocate the head
    bloom_filter_list_head *h = *head = calloc(1, sizeof(bloom_filter_list_head));

    // Iterate through a callback to append
    int prefix_len = 0;
    if (prefix) {
        prefix_len = strlen(prefix);
        art_iter_prefix(mgr->filter_map, (unsigned char*)prefix, prefix_len, filter_map_list_cb, h);
    } else
        art_iter(mgr->filter_map, filter_map_list_cb, h);

    // Joy... we have to potentially handle the delta updates
    if (mgr->primary_vsn == mgr->vsn) return 0;

    filter_list *current = mgr->delta;
    bloom_filter_wrapper *f;
    while (current) {
        // Check if this is a match (potential prefix)
        if (current->type == CREATE) {
            f = current->filter;
            if (!prefix_len || !strncmp(f->filter->filter_name, prefix, prefix_len)) {
                f = current->filter;
                filter_map_list_cb(h, (unsigned char*)f->filter->filter_name, 0, f);
            }
        }

        // Don't seek past what the primary map incorporates
        if (current->vsn == mgr->primary_vsn + 1)
            break;
        current = current->next;
    }

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

    // Scan for the cold filters. Ignore deltas, since they are either
    // new (e.g. hot), or being deleted anyways.
    art_iter(mgr->filter_map, filter_map_list_cold_cb, h);
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

// Searches the primary tree and the delta list for a filter
static bloom_filter_wrapper* find_filter(bloom_filtmgr *mgr, char *filter_name) {
    // Search the tree first
    bloom_filter_wrapper *filt = art_search(mgr->filter_map,
            (unsigned char*)filter_name, strlen(filter_name)+1);
    if (filt) return filt;

    // Check if the primary has all delta changes
    if (mgr->primary_vsn == mgr->vsn) return NULL;

    // Search the delta list
    filter_list *current = mgr->delta;
    while (current) {
        // Check if this is a match
        if (current->type != BARRIER &&
            strcmp(current->filter->filter->filter_name, filter_name) == 0) {
            return current->filter;
        }

        // Don't seek past what the primary map incorporates
        if (current->vsn == mgr->primary_vsn + 1)
            break;
        current = current->next;
    }

    // Not found
    return NULL;
}

// Gets the bloom filter in a thread safe way.
static bloom_filter_wrapper* take_filter(bloom_filtmgr *mgr, char *filter_name) {
    bloom_filter_wrapper *filt = find_filter(mgr, filter_name);
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
 * Creates a new filter and adds it to the filter map.
 * @arg mgr The manager to add to
 * @arg filter_name The name of the filter
 * @arg config The configuration for the filter
 * @arg is_hot Is the filter hot. False for existing.
 * @arg delta Is this a delta update or should the map be updated
 * @return 0 on success, -1 on error
 */
static int add_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *config, int is_hot, int delta) {
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

    // Check if we are adding a delta value or directly updating ART tree
    if (delta)
        create_delta_update(mgr, CREATE, filt);
    else
        art_insert(mgr->filter_map, (unsigned char*)filter_name, strlen(filter_name)+1, filt);
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list all the filters. Only works if value is
 * not NULL.
 */
static int filter_map_list_cb(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    (void)key_len;
    // Filter out the non-active nodes
    bloom_filter_wrapper *filt = value;
    if (!filt->is_active) return 0;

    // Cast the inputs
    bloom_filter_list_head *head = data;

    // Allocate a new entry
    bloom_filter_list *node = malloc(sizeof(bloom_filter_list));

    // Setup
    node->filter_name = strdup((char*)key);
    node->next = NULL;

    // Inject at head if first node
    if (!head->head) {
        head->head = node;
        head->tail = node;

    // Inject at tail
    } else {
        head->tail->next = node;
        head->tail = node;
    }
    head->size++;
    return 0;
}

/**
 * Called as part of the hashmap callback
 * to list cold filters. Only works if value is
 * not NULL.
 */
static int filter_map_list_cold_cb(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    (void)key_len;
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
    node->filter_name = strdup((char*)key);
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
static int filter_map_delete_cb(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    (void)data;
    (void)key;
    (void)key_len;

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
        if (add_filter(mgr, filter_name, mgr->config, 0, 0)) {
            syslog(LOG_ERR, "Failed to load filter '%s'!", filter_name);
        }
    }

    for (int i=0; i < num; i++) free(namelist[i]);
    free(namelist);
    return 0;
}


/**
 * Creates a new delta update and adds to the head of the list.
 * This must be invoked with the write lock as it is unsafe.
 * @arg mgr The manager
 * @arg type The type of delta
 * @arg filt The filter that is affected
 * @return The new version we created
 */
static unsigned long long create_delta_update(bloom_filtmgr *mgr, delta_type type, bloom_filter_wrapper *filt) {
    filter_list *delta = malloc(sizeof(filter_list));
    delta->vsn = ++mgr->vsn;
    delta->type = type;
    delta->filter = filt;
    delta->next = mgr->delta;
    mgr->delta = delta;
    return delta->vsn;
}

/**
 * Merges changes into the alternate tree from the delta lists
 * Safety: Safe ONLY if no other thread is using alt_filter_map
 */
static void merge_old_versions(bloom_filtmgr *mgr, filter_list *delta, unsigned long long min_vsn) {
    // Handle older delta first (bottom up)
    if (delta->next) merge_old_versions(mgr, delta->next, min_vsn);

    // Check if we should skip this update
    if (delta->vsn > min_vsn) return;

    // Handle current update
    bloom_filter_wrapper *s = delta->filter;
    switch (delta->type) {
        case CREATE:
            art_insert(mgr->alt_filter_map, (unsigned char*)s->filter->filter_name, strlen(s->filter->filter_name)+1, s);
            break;
        case DELETE:
            art_delete(mgr->alt_filter_map, (unsigned char*)s->filter->filter_name, strlen(s->filter->filter_name)+1);
            break;
        case BARRIER:
            // Ignore the barrier...
            break;
    }
}

/**
 * Updates the pending deletes list with a list of pending deletes
 */
static void mark_pending_deletes(bloom_filtmgr *mgr, unsigned long long min_vsn) {
    bloom_filter_list *tmp, *pending = NULL;

    // Add each delete
    filter_list *delta = mgr->delta;
    while (delta) {
        if (delta->vsn <= min_vsn && delta->type == DELETE) {
            tmp = malloc(sizeof(bloom_filter_list));
            tmp->filter_name = strdup(delta->filter->filter->filter_name);
            tmp->next = pending;
            pending = tmp;
        }
        delta = delta->next;
    }

    LOCK_BLOOM_SPIN(&mgr->pending_lock);
    mgr->pending_deletes = pending;
    UNLOCK_BLOOM_SPIN(&mgr->pending_lock);
}

/**
 * Clears the pending deletes
 */
static void clear_pending_deletes(bloom_filtmgr *mgr) {
    // Get a reference to the head
    bloom_filter_list *pending = mgr->pending_deletes;
    if (!pending) return;

    // filter the pending list to NULL safely
    LOCK_BLOOM_SPIN(&mgr->pending_lock);
    mgr->pending_deletes = NULL;
    UNLOCK_BLOOM_SPIN(&mgr->pending_lock);

    // Free the nodes
    bloom_filter_list *next;
    while (pending) {
        free(pending->filter_name);
        next = pending->next;
        free(pending);
        pending = next;
    }
}

/**
 * Swap the alternate / primary maps, filters the primary_vsn
 * This is always safe, since its just a pointer swap.
 */
static void swap_filter_maps(bloom_filtmgr *mgr, unsigned long long primary_vsn) {
    art_tree *tmp = mgr->filter_map;
    mgr->filter_map = mgr->alt_filter_map;
    mgr->alt_filter_map = tmp;
    mgr->primary_vsn = primary_vsn;
}

/*
 * Scans a filter_list* until it finds an entry with a version
 * less than min_vsn. It NULLs the pointer to that version
 * and returns a pointer to that node.
 *
 * Safety: This is ONLY safe if the minimum client version
 * and the primary_vsn is strictly greater than the min_vsn argument.
 * This ensures access to older delta entries will not happen.
 */
static filter_list* remove_delta_versions(filter_list *init, filter_list **ref, unsigned long long min_vsn) {
    filter_list *current = init;
    filter_list **prev = ref;
    while (current && current->vsn > min_vsn) {
        prev = &current->next;
        current = current->next;
    }

    // NULL out the reference pointer to current node if any
    if (current) *prev = NULL;
    return current;
}

/**
 * Deletes old versions from the delta lists, and calls
 * delete_filter on the filters in the destroyed list.
 *
 * Safety: Same as remove_delta_versions
 */
static void delete_old_versions(bloom_filtmgr *mgr, unsigned long long min_vsn) {
    // Get the merged in pending ops, lock to avoid a race
    pthread_mutex_lock(&mgr->write_lock);
    filter_list *old = remove_delta_versions(mgr->delta, &mgr->delta, min_vsn);
    pthread_mutex_unlock(&mgr->write_lock);

    // Delete the filters now that we have merged into both trees
    filter_list *next, *current = old;
    while (current) {
        if (current->type == DELETE) delete_filter(current->filter);
        next = current->next;
        free(current);
        current = next;
    }
}

/**
 * Determines the minimum visible version from the client list
 * Safety: Always safe
 */
static unsigned long long client_min_vsn(bloom_filtmgr *mgr) {
    // Determine the minimum version
    unsigned long long thread_vsn, min_vsn = mgr->vsn;
    for (filtmgr_client *cl=mgr->clients; cl != NULL; cl=cl->next) {
        thread_vsn = cl->vsn;
        if (thread_vsn < min_vsn) min_vsn = thread_vsn;
    }
    return min_vsn;
}

/**
 * Creates a barrier that is implicit by adding a
 * new version, and waiting for all clients to reach
 * that version. This can be used as a non-locking
 * syncronization mechanism.
 */
static void version_barrier(bloom_filtmgr *mgr) {
    // Create a new delta
    pthread_mutex_lock(&mgr->write_lock);
    unsigned long long vsn = create_delta_update(mgr, BARRIER, NULL);
    pthread_mutex_unlock(&mgr->write_lock);

    // Wait until we converge on the version
    while (mgr->should_run && client_min_vsn(mgr) < vsn)
        usleep(VACUUM_POLL_USEC);
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
    unsigned long long min_vsn, mgr_vsn;
    while (mgr->should_run) {
        // Do nothing if there is no changes
        if (mgr->vsn == mgr->primary_vsn) {
            usleep(VACUUM_POLL_USEC);
            continue;
        }

        /*
         * Because we use a version barrier, we always
         * end up creating a new version when we try to
         * apply delta updates. We need to handle the special case
         * where we are 1 version behind and the only delta is
         * a barrier. Do this by just updating primary_vsn.
         */
        mgr_vsn = mgr->vsn;
        if ((mgr_vsn - mgr->primary_vsn) == 1) {
            pthread_mutex_lock(&mgr->write_lock);

            // Ensure no version created in the mean time
            int should_continue = 0;
            if (mgr_vsn == mgr->vsn && mgr->delta->type == BARRIER) {
                mgr->primary_vsn = mgr_vsn;
                should_continue = 1;
            }

            // Release the lock and see if we should loop back
            pthread_mutex_unlock(&mgr->write_lock);
            if (should_continue) {
                syslog(LOG_INFO, "All updates applied. (vsn: %llu)", mgr_vsn);
                continue;
            }
        }

        // Determine the minimum version
        min_vsn = client_min_vsn(mgr);

        // Warn if there are a lot of outstanding deltas
        if (mgr->vsn - min_vsn > WARN_THRESHOLD) {
            syslog(LOG_WARNING, "Many delta versions detected! min: %llu (vsn: %llu)",
                    min_vsn, mgr->vsn);
        } else {
            syslog(LOG_DEBUG, "Applying delta update up to: %llu (vsn: %llu)",
                    min_vsn, mgr->vsn);
        }

        // Merge the old versions into the alternate three
        merge_old_versions(mgr, mgr->delta, min_vsn);

        /*
         * Mark any pending deletes so that create does not allow
         * a filter to be created before we manage to call delete_old_versions.
         * There is an unforunate race that can happen if a client
         * does a create/drop/create cycle, where the create/drop are
         * reflected in the filter_map, and thus the second create is allowed
         * BEFORE we have had a chance to actually handle the delete.
         */
        mark_pending_deletes(mgr, min_vsn);

        // Swap the maps
        swap_filter_maps(mgr, min_vsn);

        // Wait on a barrier until nobody is using the old tree
        version_barrier(mgr);

        // Merge the changes into the other tree now that its safe
        merge_old_versions(mgr, mgr->delta, min_vsn);

        // Both trees have the changes incorporated, safe to delete
        delete_old_versions(mgr, min_vsn);

        // Clear the pending delete list, since delete_old_versions() will
        // block untill all deletes are completed.
        clear_pending_deletes(mgr);

        // Log that we finished
        syslog(LOG_INFO, "Finished delta updates up to: %llu (vsn: %llu)",
                min_vsn, mgr->vsn);
    }
    return NULL;
}


/**
 * This method is used to force a vacuum up to the current
 * version. It is generally unsafe to use in bloomd,
 * but can be used in an embeded or test environment.
 */
void filtmgr_vacuum(bloom_filtmgr *mgr) {
    unsigned long long vsn = mgr->vsn;
    merge_old_versions(mgr, mgr->delta, vsn);
    swap_filter_maps(mgr, vsn);
    merge_old_versions(mgr, mgr->delta, vsn);
    delete_old_versions(mgr, vsn);
}

