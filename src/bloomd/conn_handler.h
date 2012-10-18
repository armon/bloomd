#ifndef BLOOM_CONN_HANDLER_H
#define BLOOM_CONN_HANDLER_H
#include "config.h"
#include "networking.h"
#include "filter_manager.h"

/**
 * This structure is used to communicate
 * between the connection handlers and the
 * networking layer.
 */
typedef struct {
    bloom_config *config;     // Global bloom configuration
    bloom_filtmgr *mgr;       // Filter manager
    bloom_conn_info *conn;    // Opaque handle into the networking stack
} bloom_conn_handler;

/**
 * Invoked to initialize the conn handler layer.
 */
void init_conn_handler();

/**
 * Invoked by the networking layer when there is new
 * data to be handled. The connection handler should
 * consume all the input possible, and generate responses
 * to all requests.
 * @arg handle The connection related information
 * @return 0 on success.
 */
int handle_client_connect(bloom_conn_handler *handle);

/**
 * Invoked by the networking layer periodically to
 * handle state updates. Does not provide
 * a connection object as part of the handle.
 * @arg handle The connection related information
 * @return 0 on success.
 */
void periodic_update(bloom_conn_handler *handle);

#endif
