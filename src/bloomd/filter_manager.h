#ifndef BLOOM_FILTER_MANAGER_H
#define BLOOM_FILTER_MANAGER_H
#include <pthread.h>
#include "config.h"
#include "filter.h"

/**
 * Opaque handle to the filter manager
 */
typedef struct bloom_filtmgr bloom_filtmgr;

/**
 * Lists of filters
 */
typedef struct bloom_filter_list {
    char *filter_name;
    struct bloom_filter_list *next;
} bloom_filter_list;

typedef struct {
   int size;
   bloom_filter_list *head;
   bloom_filter_list *tail;
} bloom_filter_list_head;

/**
 * Initializer
 * @arg config The configuration
 * @arg vacuum Should vacuuming be enabled. True unless in a
 * test or embedded environment using filtmgr_vacuum()
 * @arg mgr Output, resulting manager.
 * @return 0 on success.
 */
int init_filter_manager(bloom_config *config, int vacuum, bloom_filtmgr **mgr);

/**
 * Cleanup
 * @arg mgr The manager to destroy
 * @return 0 on success.
 */
int destroy_filter_manager(bloom_filtmgr *mgr);

/**
 * Should be invoked periodically by client threads to allow
 * the vacuum thread to cleanup garbage state. It should also
 * be called before making other calls into the filter manager
 * so that it is aware of a client making use of the current
 * state.
 * @arg mgr The manager
 */
void filtmgr_client_checkpoint(bloom_filtmgr *mgr);

/**
 * Should be invoked by clients when they no longer
 * need to make use of the filter manager. This
 * allows the vacuum thread to cleanup garbage state.
 * @arg mgr The manager
 */
void filtmgr_client_leave(bloom_filtmgr *mgr);

/**
 * Flushes the filter with the given name
 * @arg filter_name The name of the filter to flush
 * @return 0 on success. -1 if the filter does not exist.
 */
int filtmgr_flush_filter(bloom_filtmgr *mgr, char *filter_name);

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
int filtmgr_check_keys(bloom_filtmgr *mgr, char *filter_name, char **keys, int num_keys, char *result);

/**
 * Sets keys in a given filter
 * @arg filter_name The name of the filter
 * @arg keys A list of points to character arrays to add
 * @arg num_keys The number of keys to add
 * @arg result Ouput array, stores a 0 if the key already is set
 * or 1 if the key is set.
 * @return 0 on success, -1 if the filter does not exist.
 * -2 on internal error.
 */
int filtmgr_set_keys(bloom_filtmgr *mgr, char *filter_name, char **keys, int num_keys, char *result);

/**
 * Creates a new filter of the given name and parameters.
 * @arg filter_name The name of the filter
 * @arg custom_config Optional, can be null. Configs that override the defaults.
 * @return 0 on success, -1 if the filter already exists.
 * -2 for internal error.
 */
int filtmgr_create_filter(bloom_filtmgr *mgr, char *filter_name, bloom_config *custom_config);

/**
 * Deletes the filter entirely. This removes it from the filter
 * manager and deletes it from disk. This is a permanent operation.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_drop_filter(bloom_filtmgr *mgr, char *filter_name);

/**
 * Unmaps the filter from memory, but leaves it
 * registered in the filter manager. This is rarely invoked
 * by a client, as it can be handled automatically by bloomd,
 * but particular clients with specific needs may use it as an
 * optimization.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist.
 */
int filtmgr_unmap_filter(bloom_filtmgr *mgr, char *filter_name);

/**
 * Clears the filter from the internal data stores. This can only
 * be performed if the filter is proxied.
 * @arg filter_name The name of the filter to delete
 * @return 0 on success, -1 if the filter does not exist, -2
 * if the filter is not proxied.
 */
int filtmgr_clear_filter(bloom_filtmgr *mgr, char *filter_name);

/**
 * Allocates space for and returns a linked
 * list of all the filters. The memory should be free'd by
 * the caller.
 * @arg mgr The manager to list from
 * @arg prefix The prefix to list or NULL
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int filtmgr_list_filters(bloom_filtmgr *mgr, char *prefix, bloom_filter_list_head **head);

/**
 * Allocates space for and returns a linked
 * list of all the cold filters. This has the side effect
 * of clearing the list of cold filters! The memory should
 * be free'd by the caller.
 * @arg mgr The manager to list from
 * @arg head Output, sets to the address of the list header
 * @return 0 on success.
 */
int filtmgr_list_cold_filters(bloom_filtmgr *mgr, bloom_filter_list_head **head);

/**
 * Convenience method to cleanup a filter list.
 */
void filtmgr_cleanup_list(bloom_filter_list_head *head);

/**
 * This method allows a callback function to be invoked with bloom filter.
 * The purpose of this is to ensure that a bloom filter is not deleted or
 * otherwise destroyed while being referenced. The filter is not locked
 * so clients should under no circumstance use this to read/write to the filter.
 * It should be used to read metrics, size information, etc.
 * @return 0 on success, -1 if the filter does not exist.
 */
typedef void(*filter_cb)(void* in, char *filter_name, bloom_filter *filter);
int filtmgr_filter_cb(bloom_filtmgr *mgr, char *filter_name, filter_cb cb, void* data);

/**
 * This method is used to force a vacuum up to the current
 * version. It is generally unsafe to use in bloomd,
 * but can be used in an embeded or test environment.
 */
void filtmgr_vacuum(bloom_filtmgr *mgr);

#endif
