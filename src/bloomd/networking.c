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
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/**
 * Default listen backlog size for
 * our TCP listener.
 */
#define BACKLOG_SIZE 64

/**
 * How big should the initial conns
 * array be in terms of slots. For most
 * cases 1024 is more than we will need,
 * and will fit nicely in 1 page on 32bits,
 * and 2 pages on 64bits.
 */
#define INIT_CONN_LIST_SIZE 1024

/**
 * How big should the default connection
 * buffer size be. One page seems reasonable
 * since most requests will not be this large
 */
#define INIT_CONN_BUF_SIZE 4096


/**
 * Stores the thread specific user data.
 */
typedef struct {
    bloom_networking *netconf;
    ev_io *watcher;
    int ready_events;
} worker_ev_userdata;


/**
 * Stores the connection specific data.
 * We initialize one of these per connection
 */
typedef struct {
    ev_io client;
    uint32_t buf_size;
    char *buffer;
} conn_info;


/**
 * Represents the various types
 * of async events we could be
 * processing.
 */
typedef enum {
    EXIT,               // ev_break should be invoked
    SCHEDULE_WATCHER,   // watcher should be started
} ASYNC_EVENT_TYPE;

/**
 * Structure used to store async events
 * that need to processed when we trigger
 * the loop_async watcher.
 */
struct async_event {
    ASYNC_EVENT_TYPE event_type;
    ev_io *watcher;
    struct async_event *next;
};
typedef struct async_event async_event;

/**
 * Defines a structure that is
 * used to store the state of the networking
 * stack.
 */
struct bloom_networking {
    volatile int should_run;  // Should the workers continue to run
    bloom_config *config;
    pthread_mutex_t leader_lock; // Serializes the leaders

    int tcp_listener_fd;
    int udp_listener_fd;
    ev_io tcp_client;
    ev_io udp_client;

    ev_async loop_async;      // Allows async interrupts
    async_event *events;      // List of pending events
    pthread_mutex_t event_lock; // Protects the events

    volatile int num_threads; // Number of threads in the threads list
    pthread_t *threads;       // Array of thread references

    int conn_list_size;       // Maximum size of conns list
    conn_info **conns;        // An array of pointers to conn_info objects
    pthread_mutex_t conns_lock; // Protects conns and conn_list_size
};


// Static typedefs
static void schedule_async(bloom_networking *netconf,
                            ASYNC_EVENT_TYPE event_type,
                            ev_io *watcher);
static void prepare_event(ev_io *watcher, int revents);
static void handle_async_event(ev_async *watcher, int revents);
static void handle_new_client(int listen_fd, worker_ev_userdata* data);
static void invoke_event_handler(worker_ev_userdata* data);


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
    ev_io_init(&netconf->tcp_client, prepare_event,
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

    // Create the libev objects
    ev_io_init(&netconf->udp_client, prepare_event,
                netconf->udp_listener_fd, EV_READ);
    ev_io_start(&netconf->udp_client);
    return 0;
}

/**
 * Initializes the networking interfaces
 * @arg config Takes the bloom server configuration
 * @arg netconf Output. The configuration for the networking stack.
 */
int init_networking(bloom_config *config, bloom_networking **netconf_out) {
    // Make the netconf structure
    bloom_networking *netconf = calloc(1, sizeof(struct bloom_networking));

    // Initialize
    pthread_mutex_init(&netconf->leader_lock, NULL);
    pthread_mutex_init(&netconf->conns_lock, NULL);
    pthread_mutex_init(&netconf->event_lock, NULL);
    netconf->events = NULL;
    netconf->config = config;
    netconf->should_run = 1;
    netconf->num_threads = 0;
    netconf->threads = calloc(config->worker_threads, sizeof(pthread_t));
    netconf->conn_list_size = INIT_CONN_LIST_SIZE;
    netconf->conns = calloc(INIT_CONN_LIST_SIZE, sizeof(conn_info*));

    // Setup the TCP listener
    int res = setup_tcp_listener(netconf);
    if (res != 0) {
        free(netconf);
        return 1;
    }

    // Setup the UDP listener
    res = setup_udp_listener(netconf);
    if (res != 0) {
        ev_io_stop(&netconf->tcp_client);
        close(netconf->tcp_listener_fd);
        free(netconf);
        return 1;
    }

    // Setup the async handler
    ev_async_init(&netconf->loop_async, handle_async_event);
    ev_async_start(&netconf->loop_async);

    // Success!
    *netconf_out = netconf;
    return 0;
}


/**
 * Called to schedule an async event. Mostly a convenience
 * method to wrap some of the logic.
 */
static void schedule_async(bloom_networking *netconf,
                            ASYNC_EVENT_TYPE event_type,
                            ev_io *watcher) {
    // Make a new async event
    async_event *event = calloc(1, sizeof(async_event));

    // Initialize
    event->event_type = event_type;
    event->watcher = watcher;

    // Always lock for safety!
    pthread_mutex_lock(&netconf->event_lock);

    // Set the next pointer, and add us to the head
    event->next = netconf->events;
    netconf->events = event;

    // Unlock
    pthread_mutex_unlock(&netconf->event_lock);
}

/**
 * Called when an event is ready to be processed by libev.
 * We need to do _very_ little work here. Basically just
 * setup the userdata to process the event and return.
 * This is so we can release the leader lock and let another
 * thread take over.
 */
static void prepare_event(ev_io *watcher, int revents) {
    // Get the user data
    worker_ev_userdata *data = ev_userdata();

    // Set everything
    data->watcher = watcher;
    data->ready_events = revents;

    // Stop listening for now
    ev_io_stop(watcher);
}


/**
 * Called when a message is sent to netconf->loop_async.
 * This is usually to signal that some internal control
 * flow related to the event loop needs to take place.
 * For example, we might need to re-enable some ev_io* watchers,
 * or exit the loop.
 */
static void handle_async_event(ev_async *watcher, int revents) {
    // Get the user data
    worker_ev_userdata *data = ev_userdata();

    // Lock the events
    pthread_mutex_lock(&data->netconf->event_lock);

    async_event *event = data->netconf->events;
    async_event *next;
    while (event != NULL) {
        // Handle based on the event
        switch (event->event_type) {
            case EXIT:
                ev_break(EVBREAK_ALL);
                break;

            case SCHEDULE_WATCHER:
                ev_io_start(event->watcher);
                break;

            default:
                syslog(LOG_ERR, "Unknown async event type!");
                break;
        }

        // Grab the next event, free this one, and repeat
        next = event->next;
        free(event);
        event = next;
    }

    // Release the lock
    pthread_mutex_unlock(&data->netconf->event_lock);
}


/**
 * Invoked when a TCP listening socket fd is ready
 * to accept a new client. Accepts the client, initializes
 * the connection buffers, and prepares to start listening
 * for client data
 */
static void handle_new_client(int listen_fd, worker_ev_userdata* data) {
    // Accept the client connection
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    int client_fd = accept(listen_fd,
                        (struct sockaddr*)&client_addr,
                        &client_addr_len);

    // Debug info
    syslog(LOG_DEBUG, "Accepted client connection: %s %d",
            inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

    /**
     * Check if we have a cached conn_info object for this fd
     * in the conns list already. If not, then we need to allocate
     * a conn_info object and possibly resize the conns array.
     */
    conn_info *conn = NULL;
    if (client_fd < data->netconf->conn_list_size) {
        conn = data->netconf->conns[client_fd];
    }

    if (conn == NULL) {
        conn = calloc(1, sizeof(conn_info));
        conn->buf_size = INIT_CONN_BUF_SIZE;
        conn->buffer = calloc(INIT_CONN_BUF_SIZE, sizeof(char));

        // Lock the conns list
        pthread_mutex_lock(&data->netconf->conns_lock);

        // Resize if necessary
        if (client_fd >= data->netconf->conn_list_size) {
            // Keep doubling until we have enough space
            int new_size = 2*data->netconf->conn_list_size;
            while (new_size <= client_fd) {
                new_size *= 2;
            }

            // Allocate a new list and copy the old entries
            conn_info **new_conns = calloc(new_size, sizeof(conn_info*));
            memcpy(new_conns,
                   data->netconf->conns,
                   sizeof(conn_info*)*data->netconf->conn_list_size);

            // Flip to the new list
            free(data->netconf->conns);
            data->netconf->conns = new_conns;
            data->netconf->conn_list_size = new_size;
        }

        // Store a reference to this connection buffer
        data->netconf->conns[client_fd] = conn;

        // Release
        pthread_mutex_unlock(&data->netconf->conns_lock);
    }

    // Initialize the libev stuff
    ev_io_init(&conn->client, prepare_event, client_fd, EV_READ);
}

/**
 * Reads the thread specific userdata to figure out what
 * we need to handle. Things that purely effect the network
 * stack should be handled here, but otherwise we should defer
 * to the connection handlers.
 */
static void invoke_event_handler(worker_ev_userdata* data) {
    // Get the offending handle
    ev_io *watcher = data->watcher;
    int fd = watcher->fd;

    // Check if this is either of the listeners
    if (fd == data->netconf->tcp_listener_fd) {
        // Accept the new client
        handle_new_client(fd, data);
        return;

    } else if (fd == data->netconf->udp_listener_fd) {

        return;
    }

    /*
     * If it is not a listener, it must be a connected
     * client. We should just read all the available data,
     * append it to the buffers, and then invoke the
     * connection handlers.
     */

}


/**
 * Entry point for threads to join the networking
 * stack. This method blocks indefinitely until the
 * network stack is shutdown.
 * @arg netconf The configuration for the networking stack.
 */
void start_networking_worker(bloom_networking *netconf) {
    // Allocate our user data
    worker_ev_userdata data;
    data.netconf = netconf;
    data.watcher = NULL;
    data.ready_events = 0;
    int registered = 0;

    // Run forever until we are told to halt
    while (netconf->should_run) {
        // Become the leader
        pthread_mutex_lock(&netconf->leader_lock);

        // Register if we need to
        if (!registered) {
            netconf->threads[netconf->num_threads] = pthread_self();
            netconf->num_threads++;
            registered = 1;
        }

        // Check again if we should run
        if (netconf->should_run) {
            pthread_mutex_unlock(&netconf->leader_lock);
            break;
        }

        // Set the user data to be for this thread
        ev_set_userdata(&data);

        // Run one iteration of the event loop
        ev_run(EVRUN_ONCE);

        // Release the leader lock
        pthread_mutex_unlock(&netconf->leader_lock);

        // Process the event
        invoke_event_handler(&data);
    }
    return;
}

/**
 * Shuts down all the connections
 * and listeners and prepares to exit.
 * @arg netconf The config for the networking stack.
 */
int shutdown_networking(bloom_networking *netconf) {
    // Instruct the threads to shutdown
    netconf->should_run = 0;

    // Break the EV loop
    ev_break(EVBREAK_ALL);

    // Wait for the threads to return
    pthread_t thread;
    for (int i=0; i < netconf->num_threads; i++) {
        thread = netconf->threads[i];
        if (thread != NULL) pthread_join(thread, NULL);
    }

    // Stop listening for new connections
    ev_io_stop(&netconf->tcp_client);
    close(netconf->tcp_listener_fd);
    ev_io_stop(&netconf->udp_client);
    close(netconf->udp_listener_fd);

    // Close all the client connections
    conn_info *conn;
    for (int i=0; i < netconf->conn_list_size; i++) {
        // Check if the connection is non-null
        conn = netconf->conns[i];
        if (conn == NULL) continue;

        // Stop listening in libev and close the socket
        ev_io_stop(&conn->client);
        close(conn->client.fd);

        // Free all the buffers
        free(conn->buffer);
        free(conn);
    }

    // Free the netconf
    free(netconf->threads);
    free(netconf->conns);
    free(netconf);
    return 0;
}


