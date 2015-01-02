#include "networking.h"
#define EV_STANDALONE 1
#define EV_API_STATIC 1
#define EV_COMPAT3 0
#define EV_MULTIPLICITY 1
#define EV_USE_MONOTONIC 1
#ifdef __linux__
#define EV_USE_CLOCK_SYSCALL 0
#define EV_USE_EPOLL 1
#endif
#ifdef __MACH__
#define EV_USE_KQUEUE 1
#endif
#include "ev.c"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <syslog.h>
#include <unistd.h>
#include <limits.h>
#include <math.h>
#include <errno.h>
#include "conn_handler.h"
#include "spinlock.h"
#include "barrier.h"


/**
 * Default listen backlog size for
 * our TCP listener.
 */
#define BACKLOG_SIZE 64

/**
 * How big should the default connection
 * buffer size be. One page seems reasonable
 * since most requests will not be this large
 */
#define INIT_CONN_BUF_SIZE 4096

/**
 * This is the scale factor we use when
 * we are growing our connection buffers.
 * We want this to be aggressive enough to reduce
 * the number of resizes, but to also avoid wasted
 * space. With this, we will go from:
 * 4K -> 32K -> 256K -> 2MB -> 16MB
 */
#define CONN_BUF_MULTIPLIER 8


/**
 * This defines how often we invoke the
 * 'periodic' callback of the connection handler.
 * This allows certain cleanups and state updates
 * to take place.
 */
#define PERIODIC_TIME_SEC 0.25


/**
 * Stores the worker thread specific user data.
 */
typedef struct conn_info conn_info;
typedef struct {
    bloom_networking *netconf;
    ev_loop *loop;
    int pipefd[2];
    ev_io pipe_client;
    ev_timer periodic;
    int should_run;

    // Used to free inactive connections
    conn_info *inactive;
} worker_ev_userdata;

/**
 * Represents a simple circular buffer
 */
typedef struct {
    int write_cursor;
    int read_cursor;
    uint32_t buf_size;
    char *buffer;
} circular_buffer;

/**
 * Stores the connection specific data.
 * We initialize one of these per connection
 * Output is handled in a special way.
 * If use_write_buf is off, then we make
 * the writes directly, otherwise we need to
 * acquire a the buffer lock and write to our
 * circular buffer. Once the buffer is depleted,
 * we switch use_write_buf back off, and go back
 * to writing directly.
 *
 * The logic is that most clients have a quick
 * check/set command pair which fits in the TCP
 * buffers. Some bulk operations with tons of checks
 * or sets may overwhelm our buffers however. This
 * allows us to minimize copies and latency for most
 * clients, while still supporting the massive bulk
 * loads.
 */
struct conn_info {
    worker_ev_userdata *thread_ev;
    int active;

    ev_io client;
    circular_buffer input;

    int use_write_buf;
    ev_io write_client;
    circular_buffer output;

    struct conn_info *next;
};


/**
 * Defines a structure that is
 * used to store the state of the networking
 * stack.
 */
struct bloom_networking {
    bloom_config *config;
    bloom_filtmgr *mgr;

    int ev_mode;
    ev_loop *default_loop;
    ev_io tcp_client;
    ev_io udp_client;

    barrier_t thread_barrier;
    pthread_t *threads; // Reference to all the workers
    worker_ev_userdata **workers;
    unsigned last_assign;    // Last thread we assigned to
};


// Static typedefs
static void handle_new_client(ev_loop *lp, ev_io *watcher, int ready_events);
static void handle_new_udp_mesg(ev_loop *lp, ev_io *watcher, int ready_events);
static void invoke_event_handler(ev_loop *lp, ev_io *watcher, int ready_events);
static void handle_client_writebuf(ev_loop *lp, ev_io *watcher, int ready_events);
static int read_client_data(conn_info *conn);
static void handle_worker_notification(ev_loop *lp, ev_io *watcher, int ready_events);
static void handle_periodic_timeout(ev_loop *lp, ev_timer *t, int ready_events);

static void close_client_connection(conn_info *conn);
static void deactivate_client_connection(conn_info *conn);

// Helpers for send_client_response
static int send_client_response_buffered(conn_info *conn, char **response_buffers, int *buf_sizes, int num_bufs);
static int send_client_response_direct(conn_info *conn, char **response_buffers, int *buf_sizes, int num_bufs);


// Utility methods
static int set_client_sockopts(int client_fd);
static conn_info* get_conn();


// Circular buffer method
static void circbuf_init(circular_buffer *buf);
static void circbuf_free(circular_buffer *buf);
static uint64_t circbuf_avail_buf(circular_buffer *buf);
static void circbuf_grow_buf(circular_buffer *buf);
static void circbuf_setup_readv_iovec(circular_buffer *buf, struct iovec *vectors, int *num_vectors);
static void circbuf_setup_writev_iovec(circular_buffer *buf, struct iovec *vectors, int *num_vectors);
static void circbuf_advance_write(circular_buffer *buf, uint64_t bytes);
static void circbuf_advance_read(circular_buffer *buf, uint64_t bytes);
static int circbuf_write(circular_buffer *buf, char *in, uint64_t bytes);

/**
 * Initializes the TCP listener
 * @arg netconf The network configuration
 * @return 0 on success.
 */
static int setup_tcp_listener(bloom_networking *netconf) {
    struct sockaddr_in addr;
    struct in_addr bind_addr;
    bzero(&addr, sizeof(addr));
    bzero(&bind_addr, sizeof(bind_addr));
    addr.sin_family = PF_INET;
    addr.sin_port = htons(netconf->config->tcp_port);

    int ret = inet_pton(AF_INET, netconf->config->bind_address, &bind_addr);
    if (ret != 1) {
        syslog(LOG_ERR, "Invalid IPv4 address '%s'!", netconf->config->bind_address);
        return 1;
    }
    addr.sin_addr = bind_addr;

    // Make the socket, bind and listen
    int tcp_listener_fd = socket(PF_INET, SOCK_STREAM, 0);
    int optval = 1;
    if (setsockopt(tcp_listener_fd, SOL_SOCKET,
                SO_REUSEADDR, &optval, sizeof(optval))) {
        syslog(LOG_ERR, "Failed to set SO_REUSEADDR! Err: %s", strerror(errno));
        close(tcp_listener_fd);
        return 1;
    }
    if (bind(tcp_listener_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        syslog(LOG_ERR, "Failed to bind on TCP socket! Err: %s", strerror(errno));
        close(tcp_listener_fd);
        return 1;
    }
    if (listen(tcp_listener_fd, BACKLOG_SIZE) != 0) {
        syslog(LOG_ERR, "Failed to listen on TCP socket! Err: %s", strerror(errno));
        close(tcp_listener_fd);
        return 1;
    }

    // Create the libev objects
    ev_io_init(&netconf->tcp_client, handle_new_client,
                tcp_listener_fd, EV_READ);
    ev_io_start(netconf->default_loop, &netconf->tcp_client);
    return 0;
}

/**
 * Initializes the UDP Listener.
 * @arg netconf The network configuration
 * @return 0 on success.
 */
static int setup_udp_listener(bloom_networking *netconf) {
    struct sockaddr_in addr;
    struct in_addr bind_addr;
    bzero(&addr, sizeof(addr));
    bzero(&bind_addr, sizeof(bind_addr));
    addr.sin_family = PF_INET;
    addr.sin_port = htons(netconf->config->udp_port);

    int ret = inet_pton(AF_INET, netconf->config->bind_address, &bind_addr);
    if (ret != 1) {
        syslog(LOG_ERR, "Invalid IPv4 address '%s'!", netconf->config->bind_address);
        return 1;
    }
    addr.sin_addr = bind_addr;

    // Make the socket, bind and listen
    int udp_listener_fd = socket(PF_INET, SOCK_DGRAM, 0);
    int optval = 1;
    if (setsockopt(udp_listener_fd, SOL_SOCKET,
                SO_REUSEADDR, &optval, sizeof(optval))) {
        syslog(LOG_ERR, "Failed to set SO_REUSEADDR! Err: %s", strerror(errno));
        close(udp_listener_fd);
        return 1;
    }
    if (bind(udp_listener_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        syslog(LOG_ERR, "Failed to bind on UDP socket! Err: %s", strerror(errno));
        close(udp_listener_fd);
        return 1;
    }

    // Create the libev objects
    ev_io_init(&netconf->udp_client, handle_new_udp_mesg,
                udp_listener_fd, EV_READ);
    ev_io_start(netconf->default_loop, &netconf->udp_client);
    return 0;
}

/**
 * Initializes the networking interfaces
 * @arg config Takes the bloom server configuration
 * @arg mgr The filter manager to pass up to the connection handlers
 * @arg netconf Output. The configuration for the networking stack.
 */
int init_networking(bloom_config *config, bloom_filtmgr *mgr, bloom_networking **netconf_out) {
    // Make the netconf structure
    bloom_networking *netconf = calloc(1, sizeof(struct bloom_networking));

    // Initialize
    netconf->config = config;
    netconf->mgr = mgr;
    netconf->workers = calloc(config->worker_threads, sizeof(worker_ev_userdata*));
    if (!netconf->workers) {
        free(netconf);
        perror("Failed to calloc() for worker threads");
        return 1;
    }

    // Setup the barrier
    if (barrier_init(&netconf->thread_barrier, config->worker_threads + 1)) {
        free(netconf->workers);
        free(netconf);
        return 1;
    }

    /**
     * Check if we can use kqueue instead of select.
     * By default, libev will not use kqueue since it has
     * certain limitations that select doesn't, but which
     * we don't need.
     */
    int ev_mode = EVFLAG_AUTO;
    if (ev_supported_backends () & ~ev_recommended_backends () & EVBACKEND_KQUEUE) {
        ev_mode = EVBACKEND_KQUEUE;
    }
    netconf->ev_mode = ev_mode;

    if (!(netconf->default_loop = ev_loop_new (ev_mode))) {
        syslog(LOG_CRIT, "Failed to initialize libev!");
        free(netconf);
        return 1;
    }

    // Setup the TCP listener
    int res = setup_tcp_listener(netconf);
    if (res != 0) {
        free(netconf);
        return 1;
    }

    // Setup the UDP listener
    res = setup_udp_listener(netconf);
    if (res != 0) {
        ev_io_stop(netconf->default_loop, &netconf->tcp_client);
        close(netconf->tcp_client.fd);
        free(netconf);
        return 1;
    }

    // Prepare the conn handlers
    init_conn_handler();

    // Success!
    *netconf_out = netconf;
    return 0;
}


/**
 * Invoked when a TCP listening socket fd is ready
 * to accept a new client. Accepts the client, initializes
 * the connection buffers, and prepares to start listening
 * for client data
 */
static void handle_new_client(ev_loop *lp, ev_io *watcher, int ready_events) {
    // Get the network configuration
    bloom_networking *netconf = ev_userdata(lp);

    // Accept the client connection
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    int client_fd = accept(watcher->fd,
                        (struct sockaddr*)&client_addr,
                        &client_addr_len);

    // Check for an error
    if (client_fd == -1) {
        syslog(LOG_ERR, "Failed to accept() connection! %s.", strerror(errno));
        return;
    }

    // Setup the socket
    if (set_client_sockopts(client_fd)) {
        return;
    }

    // Debug info
    syslog(LOG_DEBUG, "Accepted client connection: %s %d [%d]",
            inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port), client_fd);

    // Get the associated conn object
    conn_info *conn = get_conn();

    // Initialize the libev stuff
    ev_io_init(&conn->client, invoke_event_handler, client_fd, EV_READ);
    ev_io_init(&conn->write_client, handle_client_writebuf, client_fd, EV_WRITE);

    // Dispatch this client to a worker thread
    int next_thread = netconf->last_assign++ % netconf->config->worker_threads;
    worker_ev_userdata *data = netconf->workers[next_thread];

    // Sent accept along with the connection
    write(data->pipefd[1], "a", 1);
    write(data->pipefd[1], &conn, sizeof(conn_info*));
}


/**
 * Invoked to handle new UDP messages being available.
 */
static void handle_new_udp_mesg(ev_loop *lp, ev_io *watcher, int ready_events) {
    // TODO: Handle UDP clients
    syslog(LOG_WARNING, "UDP clients not currently supported!");
}


/**
 * Invoked when a client connection has data ready to be read.
 * We need to take care to add the data to our buffers, and then
 * invoke the connection handlers who have the business logic
 * of what to do.
 */
static int read_client_data(conn_info *conn) {
    /**
     * Figure out how much space we have to write.
     * If we have < 50% free, we resize the buffer using
     * a multiplier.
     */
    int avail_buf = circbuf_avail_buf(&conn->input);
    if (avail_buf < conn->input.buf_size / 2) {
        circbuf_grow_buf(&conn->input);
    }

    // Build the IO vectors to perform the read
    struct iovec vectors[2];
    int num_vectors;
    circbuf_setup_readv_iovec(&conn->input, (struct iovec*)&vectors, &num_vectors);

    // Issue the read
    ssize_t read_bytes = readv(conn->client.fd, (struct iovec*)&vectors, num_vectors);

    // Make sure we actually read something
    if (read_bytes == 0) {
        syslog(LOG_DEBUG, "Closed client connection. [%d]\n", conn->client.fd);
        return 1;
    } else if (read_bytes == -1) {
        if (errno != EAGAIN && errno != EINTR) {
            syslog(LOG_ERR, "Failed to read() from connection [%d]! %s.",
                    conn->client.fd, strerror(errno));
        }
        return 1;
    }

    // Update the write cursor
    circbuf_advance_write(&conn->input, read_bytes);
    return 0;
}


/**
 * Invoked when a client connection is ready to be written to.
 */
static void handle_client_writebuf(ev_loop *lp, ev_io *watcher, int ready_events) {
    // Get the associated connection struct
    conn_info *conn = watcher->data;

    // Bail if inactive
    if (!conn->active) return;

    // Build the IO vectors to perform the write
    struct iovec vectors[2];
    int num_vectors;
    circbuf_setup_writev_iovec(&conn->output, (struct iovec*)&vectors, &num_vectors);

    // Issue the write
    ssize_t write_bytes = writev(watcher->fd, (struct iovec*)&vectors, num_vectors);

    if (write_bytes > 0) {
        // Update the cursor
        circbuf_advance_read(&conn->output, write_bytes);

        // Check if we should reset the use_write_buf.
        // This is done when the buffer size is 0.
        if (conn->output.read_cursor == conn->output.write_cursor) {
            conn->use_write_buf = 0;
            ev_io_stop(lp, &conn->write_client);
        }
    }

    // Handle any errors
    if (write_bytes <= 0 && (errno != EAGAIN && errno != EINTR)) {
        syslog(LOG_ERR, "Failed to write() to connection [%d]! %s.",
                conn->client.fd, strerror(errno));
        deactivate_client_connection(conn);
        return;
    }
}


/*
 * Invoked when client read data is ready.
 * We just read all the available data,
 * append it to the buffers, and then invoke the
 * connection handlers.
 */
static void invoke_event_handler(ev_loop *lp, ev_io *watcher, int ready_events) {
    // Get the user data
    worker_ev_userdata *data = ev_userdata(lp);
    conn_info *conn = watcher->data;

    // Bail if inactive
    if (!conn->active) return;

    // Read in the data, and close on issues
    if (read_client_data(conn)) {
        deactivate_client_connection(conn);
        return;
    }

    // Prepare to invoke the handler
    bloom_conn_handler handle;
    handle.config = data->netconf->config;
    handle.mgr = data->netconf->mgr;
    handle.conn = conn;

    // Reschedule the watcher, unless it's non-active now
    if (handle_client_connect(&handle))
        deactivate_client_connection(conn);
}


/**
 * Invoked to handle async notifications via the thread pipes
 */
static void handle_worker_notification(ev_loop *lp, ev_io *watcher, int ready_events) {
    // Get the user data
    worker_ev_userdata *data = ev_userdata(lp);

    // Attempt to read a single character from the pipe
    char cmd;
    if (read(data->pipefd[0], &cmd, 1) != 1)
        return;

    // Handle the command
    conn_info *conn;
    switch (cmd) {
        // Accept new connection
        case 'a':
            // Read the address of conn from the pipe
            if (read(data->pipefd[0], &conn, sizeof(conn_info*)) < 0) {
                perror("Failed to read from async pipe");
                return;
            }

            // Schedule this connection on this thread
            conn->thread_ev = data;
            ev_io_start(data->loop, &conn->client);
            break;

        // Quit
        case 'q':
            data->should_run = 0;
            ev_break(lp, EVBREAK_ALL);
            break;

        default:
            syslog(LOG_WARNING, "Received unknown comand: %c", cmd);
    }
}


/**
 * Invoked periodically to give the connection handlers
 * time to cleanup and handle state updates
 */
static void handle_periodic_timeout(ev_loop *lp, ev_timer *t, int ready_events) {
    // Get the user data
    worker_ev_userdata *data = ev_userdata(lp);

    // Prepare to invoke the handler
    bloom_conn_handler handle;
    handle.config = data->netconf->config;
    handle.mgr = data->netconf->mgr;
    handle.conn = NULL;

    // Invoke the connection handler layer
    periodic_update(&handle);
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
    data.should_run = 1;
    data.inactive = NULL;

    // Allocate our pipe
    if (pipe(data.pipefd)) {
        perror("failed to allocate worker pipes!");
        return;
    }

    // Create the event loop
    if (!(data.loop = ev_loop_new(netconf->ev_mode))) {
        syslog(LOG_ERR, "Failed to create event loop for worker!");
        return;
    }

    // Set the user data to be for this thread
    ev_set_userdata(data.loop, &data);

    // Setup the pipe listener
    ev_io_init(&data.pipe_client, handle_worker_notification,
                data.pipefd[0], EV_READ);
    ev_io_start(data.loop, &data.pipe_client);

    // Setup the periodic timers,
    ev_timer_init(&data.periodic, handle_periodic_timeout,
                PERIODIC_TIME_SEC, 1);
    ev_timer_start(data.loop, &data.periodic);

    // Syncronize until netconf->threads is available
    barrier_wait(&netconf->thread_barrier);

    // Register this thread so we can accept connections
    assert(netconf->threads);
    pthread_t id = pthread_self();
    for (int i=0; i < netconf->config->worker_threads; i++) {
        if (pthread_equal(id, netconf->threads[i])) {
            // Provide a pointer to our data
            netconf->workers[i] = &data;
            break;
        }
    }

    // Wait for everybody to be registered
    barrier_wait(&netconf->thread_barrier);

    // Run the event loop
    while (data.should_run) {
        ev_run(data.loop, EVRUN_ONCE);

        // Free inactive connections
        conn_info *c = data.inactive;
        while (c) {
            conn_info *n = c->next;
            close_client_connection(c);
            c = n;
        }
        data.inactive = NULL;
    }

    // Cleanup after exit
    ev_timer_stop(data.loop, &data.periodic);
    ev_io_stop(data.loop, &data.pipe_client);
    close(data.pipefd[0]);
    close(data.pipefd[1]);
    ev_loop_destroy(data.loop);
}


/**
 * Entry point for the main thread to start accepting
 * @arg netconf The configuration for the networking stack.
 * @arg should_run A flag checked to see if we should run
 * @arg threads The list of worker threads
 */
void enter_main_loop(bloom_networking *netconf, int *should_run, pthread_t *threads) {
    // Store a reference to the threads
    netconf->threads = threads;

    // Set the user data of the main loop to netconf
    ev_set_userdata(netconf->default_loop, netconf);

    // Syncronize now that netconf->threads are ready
    barrier_wait(&netconf->thread_barrier);

    // Syncronize until threads are registered
    barrier_wait(&netconf->thread_barrier);

    // Run forever
    while (*should_run) {
        ev_run(netconf->default_loop, EVRUN_ONCE);
    }
}


/**
 * Shuts down all the connections
 * and listeners and prepares to exit.
 * @arg netconf The config for the networking stack.
 * @arg threads A list of worker threads
 */
int shutdown_networking(bloom_networking *netconf, pthread_t *threads) {
    // Stop listening for new connections
    ev_io_stop(netconf->default_loop, &netconf->tcp_client);
    ev_io_stop(netconf->default_loop, &netconf->udp_client);
    close(netconf->tcp_client.fd);
    close(netconf->udp_client.fd);

    // Tell the threads to quit, async signal
    for (int i=0; i < netconf->config->worker_threads; i++) {
        write(netconf->workers[i]->pipefd[1], "q", 1);
    }

    // Wait for the threads to return
    pthread_t thread;
    for (int i=0; i < netconf->config->worker_threads; i++) {
        thread = threads[i];
        if (thread) pthread_join(thread, NULL);
    }

    // TODO: Close all the client connections
    // ??? For now, we just leak the memory
    // since we are shutdown down anyways...

    // Shutdown the event loo
    ev_loop_destroy(netconf->default_loop);

    // Free the netconf
    free(netconf->workers);
    free(netconf);
    return 0;
}

/*
 * These are externally visible methods for
 * interacting with the connection buffers.
 */

/**
 * Called to close and cleanup a client connection.
 * Must be called when the connection is not already
 * scheduled. e.g. After ev_io_stop() has been called.
 * Leaves the connection in the conns list so that it
 * can be re-used.
 * @arg conn The connection to close
 */
static void close_client_connection(conn_info *conn) {
    // Stop the libev clients
    ev_io_stop(conn->thread_ev->loop, &conn->client);
    ev_io_stop(conn->thread_ev->loop, &conn->write_client);

    // Clear everything out
    circbuf_free(&conn->input);
    circbuf_free(&conn->output);

    // Close the fd
    syslog(LOG_DEBUG, "Closed connection. [%d]", conn->client.fd);
    close(conn->client.fd);
    free(conn);
}

/**
 * Marks a client connection as 'inactive' and
 * to be closed when the event loop is finished.
 */
static void deactivate_client_connection(conn_info *conn) {
    if (!conn->active) return;
    conn->active = 0;
    conn->next = conn->thread_ev->inactive;
    conn->thread_ev->inactive = conn;
}

/**
 * Sends a response to a client.
 * @arg conn The client connection
 * @arg response_buffers A list of response buffers to send
 * @arg buf_sizes A list of the buffer sizes
 * @arg num_bufs The number of response buffers
 * @return 0 on success.
 */
int send_client_response(conn_info *conn, char **response_buffers, int *buf_sizes, int num_bufs) {
    // Silently bail of the connection is not active
    if (!conn->active) return 0;

    int send_bufs, res = 0;
    for (int offset=0; offset < num_bufs && res == 0; offset += IOV_MAX) {
        // Determine how many buffers to send
        send_bufs = ((num_bufs - offset) <= IOV_MAX) ? (num_bufs - offset) : IOV_MAX;

        // Check if we are doing buffered writes
        if (conn->use_write_buf) {
            res = send_client_response_buffered(conn, response_buffers + offset, buf_sizes + offset, send_bufs);
        } else {
            res = send_client_response_direct(conn, response_buffers + offset, buf_sizes + offset, send_bufs);
        }
    }

    // Disable the connection on error
    if (res) deactivate_client_connection(conn);
    return res;
}


static int send_client_response_buffered(conn_info *conn, char **response_buffers, int *buf_sizes, int num_bufs) {
    // Copy the buffers to the output buffer
    int res = 0;
    for (int i=0; i< num_bufs; i++) {
        res = circbuf_write(&conn->output, response_buffers[i], buf_sizes[i]);
        if (res) break;
    }
    return res;
}


static int send_client_response_direct(conn_info *conn, char **response_buffers, int *buf_sizes, int num_bufs) {
    // Stack allocate the iovectors
    struct iovec *vectors = alloca(num_bufs * sizeof(struct iovec));

    // Setup all the pointers
    ssize_t total_bytes = 0;
    for (int i=0; i < num_bufs; i++) {
        vectors[i].iov_base = response_buffers[i];
        vectors[i].iov_len = buf_sizes[i];
        total_bytes += buf_sizes[i];
    }

    // Perform the write
    ssize_t sent = writev(conn->client.fd, vectors, num_bufs);
    if (sent == total_bytes) return 0;

    // Check for a fatal error
    if (sent == -1) {
        if (errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK) {
            syslog(LOG_ERR, "Failed to send() to connection [%d]! %s.",
                    conn->client.fd, strerror(errno));
            return 1;
        }
    }

    // Figure out which buffer we left off on
    int skip_bytes = 0;
    int index = 0;
    for (index; index < num_bufs; index++) {
        skip_bytes += buf_sizes[index];
        if (skip_bytes > sent) {
            skip_bytes -= buf_sizes[index];
            break;
        }
    }

    // Copy the buffers
    int res, offset;
    for (int i=index; i < num_bufs; i++) {
        offset = 0;
        if (i == index && skip_bytes < sent) {
            offset = sent - skip_bytes;
        }
        res = circbuf_write(&conn->output, response_buffers[i] + offset, buf_sizes[i] - offset);
        if (res) return 1;
    }

    // Setup the async write
    conn->use_write_buf = 1;
    ev_io_start(conn->thread_ev->loop, &conn->write_client);

    // Done
    return 0;
}


/**
 * This method is used to conveniently extract commands from the
 * command buffer. It scans up to a terminator, and then sets the
 * buf to the start of the buffer, and buf_len to the length
 * of the buffer. The output param should_free indicates that
 * the caller should free the buffer pointed to by buf when it is finished.
 * This method consumes the bytes from the underlying buffer, freeing
 * space for later reads.
 * @arg conn The client connection
 * @arg terminator The terminator charactor to look for. Replaced by null terminator.
 * @arg buf Output parameter, sets the start of the buffer.
 * @arg buf_len Output parameter, the length of the buffer.
 * @arg should_free Output parameter, should the buffer be freed by the caller.
 * @return 0 on success, -1 if the terminator is not found.
 */
int extract_to_terminator(bloom_conn_info *conn, char terminator, char **buf, int *buf_len, int *should_free) {
    // First we need to find the terminator...
    char *term_addr = NULL;
    if (conn->input.write_cursor < conn->input.read_cursor) {
        /*
         * We need to scan from the read cursor to the end of
         * the buffer, and then from the start of the buffer to
         * the write cursor.
        */
        term_addr = memchr(conn->input.buffer+conn->input.read_cursor,
                           terminator,
                           conn->input.buf_size - conn->input.read_cursor);

        // If we've found the terminator, we can just move up
        // the read cursor
        if (term_addr) {
            *buf = conn->input.buffer + conn->input.read_cursor;
            *buf_len = term_addr - *buf + 1;    // Difference between the terminator and location
            *term_addr = '\0';              // Add a null terminator
            *should_free = 0;               // No need to free, in the buffer
            conn->input.read_cursor = term_addr - conn->input.buffer + 1; // Push the read cursor forward
            return 0;
        }

        // Wrap around
        term_addr = memchr(conn->input.buffer,
                           terminator,
                           conn->input.write_cursor);

        // If we've found the terminator, we need to allocate
        // a contiguous buffer large enough to store everything
        // and provide a linear buffer
        if (term_addr) {
            int start_size = term_addr - conn->input.buffer + 1;
            int end_size = conn->input.buf_size - conn->input.read_cursor;
            *buf_len = start_size + end_size;
            *buf = malloc(*buf_len);

            // Copy from the read cursor to the end
            memcpy(*buf, conn->input.buffer+conn->input.read_cursor, end_size);

            // Copy from the start to the terminator
            *term_addr = '\0';              // Add a null terminator
            memcpy(*buf+end_size, conn->input.buffer, start_size);

            *should_free = 1;               // Must free, not in the buffer
            conn->input.read_cursor = start_size; // Push the read cursor forward
        }

    } else {
        /*
         * We need to scan from the read cursor to write buffer.
         */
        term_addr = memchr(conn->input.buffer+conn->input.read_cursor,
                           terminator,
                           conn->input.write_cursor - conn->input.read_cursor);

        // If we've found the terminator, we can just move up
        // the read cursor
        if (term_addr) {
            *buf = conn->input.buffer + conn->input.read_cursor;
            *buf_len = term_addr - *buf + 1; // Difference between the terminator and location
            *term_addr = '\0';               // Add a null terminator
            *should_free = 0;                // No need to free, in the buffer
            conn->input.read_cursor = term_addr - conn->input.buffer + 1; // Push the read cursor forward
        }
    }

    // Minor optimization, if our read-cursor has caught up
    // with the write cursor, reset them to the beginning
    // to avoid wrapping in the future
    if (conn->input.read_cursor == conn->input.write_cursor) {
        conn->input.read_cursor = 0;
        conn->input.write_cursor = 0;
    }

    // Return success if we have a term address
    return ((term_addr) ? 0 : -1);
}


/**
 * Sets the client socket options.
 * @return 0 on success, 1 on error.
 */
static int set_client_sockopts(int client_fd) {
    // Setup the socket to be non-blocking
    int sock_flags = fcntl(client_fd, F_GETFL, 0);
    if (sock_flags < 0) {
        syslog(LOG_ERR, "Failed to get socket flags on connection! %s.", strerror(errno));
        close(client_fd);
        return 1;
    }
    if (fcntl(client_fd, F_SETFL, sock_flags | O_NONBLOCK)) {
        syslog(LOG_ERR, "Failed to set O_NONBLOCK on connection! %s.", strerror(errno));
        close(client_fd);
        return 1;
    }

    /**
     * Set TCP_NODELAY. This will allow us to send small response packets more
     * quickly, since our responses are rarely large enough to consume a packet.
     */
    int flag = 1;
    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int))) {
        syslog(LOG_WARNING, "Failed to set TCP_NODELAY on connection! %s.", strerror(errno));
    }

    // Set keep alive
    if(setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(int))) {
        syslog(LOG_WARNING, "Failed to set SO_KEEPALIVE on connection! %s.", strerror(errno));
    }

    return 0;
}


/**
 * Returns a new conn_info struct
 */
static conn_info* get_conn() {
    // Allocate space
    conn_info *conn = malloc(sizeof(conn_info));

    // Setup variables
    conn->active = 1;
    conn->use_write_buf = 0;

    // Prepare the buffers
    circbuf_init(&conn->input);
    circbuf_init(&conn->output);

    // Store a reference to the conn object
    conn->client.data = conn;
    conn->write_client.data = conn;

    return conn;
}

/*
 * Methods for manipulating our circular buffers
 */

// Conditionally allocates if there is no buffer
static void circbuf_init(circular_buffer *buf) {
    buf->read_cursor = 0;
    buf->write_cursor = 0;
    buf->buf_size = INIT_CONN_BUF_SIZE * sizeof(char);
    buf->buffer = malloc(buf->buf_size);
}

// Frees a buffer
static void circbuf_free(circular_buffer *buf) {
    if (buf->buffer) free(buf->buffer);
    buf->buffer = NULL;
}

// Calculates the available buffer size
static uint64_t circbuf_avail_buf(circular_buffer *buf) {
    uint64_t avail_buf;
    if (buf->write_cursor < buf->read_cursor) {
        avail_buf = buf->read_cursor - buf->write_cursor - 1;
    } else {
        avail_buf = buf->buf_size - buf->write_cursor + buf->read_cursor - 1;
    }
    return avail_buf;
}

// Grows the circular buffer to make room for more data
static void circbuf_grow_buf(circular_buffer *buf) {
    int new_size = buf->buf_size * CONN_BUF_MULTIPLIER * sizeof(char);
    char *new_buf = malloc(new_size);
    int bytes_written = 0;

    // Check if the write has wrapped around
    if (buf->write_cursor < buf->read_cursor) {
        // Copy from the read cursor to the end of the buffer
        bytes_written = buf->buf_size - buf->read_cursor;
        memcpy(new_buf,
               buf->buffer+buf->read_cursor,
               bytes_written);

        // Copy from the start to the write cursor
        memcpy(new_buf+bytes_written,
               buf->buffer,
               buf->write_cursor);
        bytes_written += buf->write_cursor;

    // We haven't wrapped yet...
    } else {
        // Copy from the read cursor up to the write cursor
        bytes_written = buf->write_cursor - buf->read_cursor;
        memcpy(new_buf,
               buf->buffer + buf->read_cursor,
               bytes_written);
    }

    // Update the buffer locations and everything
    free(buf->buffer);
    buf->buffer = new_buf;
    buf->buf_size = new_size;
    buf->read_cursor = 0;
    buf->write_cursor = bytes_written;
}


// Initializes a pair of iovectors to be used for readv
static void circbuf_setup_readv_iovec(circular_buffer *buf, struct iovec *vectors, int *num_vectors) {
    // Check if we've wrapped around
    *num_vectors = 1;
    if (buf->write_cursor < buf->read_cursor) {
        vectors[0].iov_base = buf->buffer + buf->write_cursor;
        vectors[0].iov_len = buf->read_cursor - buf->write_cursor - 1;
    } else {
        vectors[0].iov_base = buf->buffer + buf->write_cursor;
        vectors[0].iov_len = buf->buf_size - buf->write_cursor - 1;
        if (buf->read_cursor > 0)  {
            vectors[0].iov_len += 1;
            vectors[1].iov_base = buf->buffer;
            vectors[1].iov_len = buf->read_cursor - 1;
            *num_vectors = 2;
        }
    }
}

// Initializes a pair of iovectors to be used for writev
static void circbuf_setup_writev_iovec(circular_buffer *buf, struct iovec *vectors, int *num_vectors) {
    // Check if we've wrapped around
    if (buf->write_cursor < buf->read_cursor) {
        *num_vectors = 2;
        vectors[0].iov_base = buf->buffer + buf->read_cursor;
        vectors[0].iov_len = buf->buf_size - buf->read_cursor;
        vectors[1].iov_base = buf->buffer;
        vectors[1].iov_len = buf->write_cursor;
    } else {
        *num_vectors = 1;
        vectors[0].iov_base = buf->buffer + buf->read_cursor;
        vectors[0].iov_len = buf->write_cursor - buf->read_cursor;
    }
}

// Advances the cursors
static void circbuf_advance_write(circular_buffer *buf, uint64_t bytes) {
    buf->write_cursor = (buf->write_cursor + bytes) % buf->buf_size;
}

static void circbuf_advance_read(circular_buffer *buf, uint64_t bytes) {
    buf->read_cursor = (buf->read_cursor + bytes) % buf->buf_size;

    // Optimization, reset the cursors if they catchup with each other
    if (buf->read_cursor == buf->write_cursor) {
        buf->read_cursor = 0;
        buf->write_cursor = 0;
    }
}

/**
 * Writes the data from a given input buffer
 * into the circular buffer.
 * @return 0 on success.
 */
static int circbuf_write(circular_buffer *buf, char *in, uint64_t bytes) {
    // Check for available space
    uint64_t avail = circbuf_avail_buf(buf);
    while (avail < bytes) {
        circbuf_grow_buf(buf);
        avail = circbuf_avail_buf(buf);
    }

    if (buf->write_cursor < buf->read_cursor) {
        memcpy(buf->buffer+buf->write_cursor, in, bytes);
        buf->write_cursor += bytes;

    } else {
        uint64_t end_size = buf->buf_size - buf->write_cursor;
        if (end_size >= bytes) {
            memcpy(buf->buffer+buf->write_cursor, in, bytes);
            buf->write_cursor += bytes;

        } else {
            // Copy the first end_size bytes
            memcpy(buf->buffer+buf->write_cursor, in, end_size);

            // Copy the remaining data
            memcpy(buf->buffer, in+end_size, (bytes - end_size));
            buf->write_cursor = (bytes - end_size);
        }
    }

    return 0;
}

