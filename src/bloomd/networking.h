#ifndef BLOOM_NETWORKING_H
#define BLOOM_NETWORKING_H
#include "config.h"

/**
 * Defines a structure that is
 * used to store the state of the networking
 * stack.
 */
typedef struct {
    bloom_config *config;
    int tcp_listener_fd;
    int udp_listener_fd;
} bloom_networking;

/**
 * Initializes the networking interfaces
 * @arg config Takes the bloom server configuration
 * @arg netconf Output. The configuration for the networking stack.
 */
int init_networking(bloom_config *config, bloom_networking *netconf);

/**
 * Entry point for threads to join the networking
 * stack. This method blocks indefinitely until the
 * network stack is shutdown.
 * @arg netconf The configuration for the networking stack.
 */
int start_networking_worker(bloom_networking *netconf);

/**
 * Shuts down all the connections
 * and listeners and prepares to exit.
 * @arg netconf The config for the networking stack.
 */
int shutdown_networking(bloom_networking *netconf);

#endif
