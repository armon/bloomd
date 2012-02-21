#ifndef BLOOM_NETWORKING_H
#define BLOOM_NETWORKING_H
#include "config.h"

// Network configuration struct
typedef struct bloom_networking bloom_networking;
typedef struct conn_info bloom_conn_info;

/**
 * Initializes the networking interfaces
 * @arg config Takes the bloom server configuration
 * @arg netconf Output. The configuration for the networking stack.
 */
int init_networking(bloom_config *config, bloom_networking **netconf);

/**
 * Entry point for threads to join the networking
 * stack. This method blocks indefinitely until the
 * network stack is shutdown.
 * @arg netconf The configuration for the networking stack.
 */
void start_networking_worker(bloom_networking *netconf);

/**
 * Shuts down all the connections
 * and listeners and prepares to exit.
 * @arg netconf The config for the networking stack.
 */
int shutdown_networking(bloom_networking *netconf);

/*
 * Connection related methods. These are exposed so
 * that the connection handlers can manipulate the buffers.
 */

/**
 * Closes the client connection.
 */
void close_client_connection(bloom_conn_info *conn);

/**
 * Sends a response to a client.
 * @arg conn The client connection
 * @arg response_buffers A list of response buffers to send
 * @arg buf_sizes A list of the buffer sizes
 * @arg num_bufs The number of response buffers
 * @return 0 on success.
 */
int send_client_response(bloom_conn_info *conn, char **response_buffers, int *buf_sizes, int num_bufs);

/**
 * Returns the number of bytes ready to be read.
 * @arg conn The client connection
 * @return The number of bytes available to read
 */
int available_client_bytes(bloom_conn_info *conn);

#endif
