#ifndef BLOOM_CONN_HANDLER_H
#define BLOOM_CONN_HANDLER_H
#include "config.h"
#include "networking.h"

/**
 * This structure is used to communicate
 * between the connection handlers and the
 * networking layer.
 */
typedef struct {
    bloom_config *config;     // Global bloom configuration
    bloom_conn_info *conn;    // Opaque handle into the networking stack
} bloom_conn_handler;

/**
 * Invoked by the networking layer when there is new
 * data to be handled. The connection handler should
 * consume all the input possible, and generate responses
 * to all requests.
 * @arg handle The connection related information
 * @return 0 on success.
 */
int handle_client_connect(bloom_conn_handler *handle);

#endif
