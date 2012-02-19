#include "networking.h"
#define EV_STANDALONE 1
#define EV_COMPAT3 0
#define EV_MULTIPLICITY 0
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

// Static typedefs
static void accept_cb(ev_io *watcher, int revents);

/**
 * Defines a structure that is
 * used to store the state of the networking
 * stack.
 */
struct bloom_networking {
    bloom_config *config;
    int tcp_listener_fd;
    int udp_listener_fd;
    ev_io tcp_client;
    ev_io udp_client;
};


/**
 * Initializes the TCP listener
 * @arg netconf The network configuration
 * @return 0 on success.
 */
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

    // Create the libev objects
    ev_io_init(&netconf->tcp_client, accept_cb,
                netconf->tcp_listener_fd, EV_READ);
    ev_io_start(&netconf->tcp_client);
    return 0;
}

/**
 * Initializes the UDP Listener.
 * @arg netconf The network configuration
 * @return 0 on success.
 */
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

    // Create the libev objects
    ev_io_init(&netconf->udp_client, accept_cb,
                netconf->udp_listener_fd, EV_READ);
    ev_io_start(&netconf->udp_client);
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
        ev_io_stop(&netconf->tcp_client);
        close(netconf->tcp_listener_fd);
        return 1;
    }

    // Success!
    return 0;
}

/**
 * Called when our listeners are ready to accept
 */
static void accept_cb(ev_io *watcher, int revents) {

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


