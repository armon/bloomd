#include "networking.h"
#define EV_STANDALONE 1
#define EV_COMPAT3 0
#ifdef __linux__
#define EV_USE_EPOLL 1
#endif
#ifdef __APPLE__
#define EV_USE_KQUEUE 1
#endif
#include "ev.c"
#include <syslog.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define BACKLOG_SIZE 64

static int setup_tcp_listener(bloom_networking *netconf) {
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = PF_INET;
    addr.sin_port = htons(netconf->config->tcp_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    // Make the socket, bind and listen
    netconf->tcp_listener_fd = socket(PF_INET, SOCK_STREAM, 0);
    if (bind(netconf->tcp_listener_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        syslog(LOG_ERR, "Failed to bind on TCP socket! Err: %s", strerror(errno));
        close(netconf->tcp_listener_fd);
        return 1;
    }
    if (listen(netconf->tcp_listener_fd, BACKLOG_SIZE) != 0) {
        syslog(LOG_ERR, "Failed to listen on TCP socket! Err: %s", strerror(errno));
        close(netconf->tcp_listener_fd);
        return 1;
    }
    return 0;
}

static int setup_udp_listener(bloom_networking *netconf) {
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = PF_INET;
    addr.sin_port = htons(netconf->config->udp_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    // Make the socket, bind and listen
    netconf->udp_listener_fd = socket(PF_INET, SOCK_DGRAM, 0);
    if (bind(netconf->udp_listener_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        syslog(LOG_ERR, "Failed to bind on UDP socket! Err: %s", strerror(errno));
        close(netconf->udp_listener_fd);
        return 1;
    }
    if (listen(netconf->udp_listener_fd, BACKLOG_SIZE) != 0) {
        syslog(LOG_ERR, "Failed to listen on UDP socket! Err: %s", strerror(errno));
        close(netconf->udp_listener_fd);
        return 1;
    }
    return 0;
}

/**
 * Initializes the networking interfaces
 * @arg config Takes the bloom server configuration
 * @arg netconf Output. The configuration for the networking stack.
 */
int init_networking(bloom_config *config, bloom_networking *netconf) {
    // Store the config
    netconf->config = config;

    // Setup the TCP listener
    int res = setup_tcp_listener(netconf);
    if (res != 0) return 1;

    // Setup the UDP listener
    res = setup_udp_listener(netconf);
    if (res != 0) {
        close(netconf->tcp_listener_fd);
        return 1;
    }

    // Success!
    return 0;
}

/**
 * Entry point for threads to join the networking
 * stack. This method blocks indefinitely until the
 * network stack is shutdown.
 * @arg netconf The configuration for the networking stack.
 */
int start_networking_worker(bloom_networking *netconf) {
    return 0;
}

/**
 * Shuts down all the connections
 * and listeners and prepares to exit.
 * @arg netconf The config for the networking stack.
 */
int shutdown_networking(bloom_networking *netconf) {
    return 0;
}


