/**
 * This is the main entry point into bloomd.
 * We are responsible for parsing any commmand line
 * flags, reading the configuration, starting
 * the filter manager, and finally starting the
 * front ends.
 */
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>
#include <signal.h>
#include "config.h"
#include "networking.h"
#include "filter_manager.h"
#include "background.h"

// Simple struct that holds args for the workers
typedef struct {
    bloom_filtmgr *mgr;
    bloom_networking *netconf;
} worker_args;
static void *worker_main(worker_args *args);

/**
 * By default we should run. Our signal
 * handler updates this variable to allow the
 * program to gracefully terminate.
 */
static int SHOULD_RUN = 1;

/**
 * Prints our usage to stderr
 */
void show_usage() {
    fprintf(stderr, "usage: bloomd [-h] [-f filename] [-w num]\n\
\n\
    -h : Displays this help info\n\
    -f : Reads the bloomd configuration from this file\n\
    -w : Sets the number of worker threads\n\
\n");
}

/**
 * Invoked to parse the command line options
 */
int parse_cmd_line_args(int argc, char **argv, char **config_file, int *workers) {
    int enable_help = 0;

    int c;
    long w;
    opterr = 0;
    while ((c = getopt(argc, argv, "hf:w:")) != -1) {
        switch (c) {
            case 'h':
                enable_help = 1;
                break;
            case 'f':
                *config_file = optarg;
                break;
            case 'w':
                w = strtol(optarg, NULL, 10);
                if (w == 0 && errno == EINVAL) {
                    fprintf(stderr, "Option -%c requires a number.\n", optopt);
                    break;
                }
                *workers = w;
                break;
            case '?':
                if (optopt == 'f')
                    fprintf(stderr, "Option -%c requires a filename.\n", optopt);
                if (optopt == 'w')
                    fprintf(stderr, "Option -%c requires a positive integer.\n", optopt);
                else if (isprint(optopt))
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                else
                    fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
                return 1;
        }
    }

    // Check if we need to show usage
    if (enable_help) {
        show_usage();
        return 1;
    }

    return 0;
}


/**
 * Initializes the syslog configuration
 */
void setup_syslog() {
    // If we are on a tty, log the errors out
    int flags = LOG_CONS|LOG_NDELAY|LOG_PID;
    if (isatty(1)) {
        flags |= LOG_PERROR;
    }
    openlog("bloomd", flags, LOG_LOCAL0);
}


/**
 * Our registered signal handler, invoked
 * when we get signals such as SIGINT, SIGTERM.
 */
void signal_handler(int signum) {
    SHOULD_RUN = 0;  // Stop running now
    syslog(LOG_WARNING, "Received signal [%s]! Exiting...", strsignal(signum));
}


int main(int argc, char **argv) {
    // Initialize syslog
    setup_syslog();

    // Parse the command line
    char *config_file = NULL;
    int workers = 0;
    int parse_res = parse_cmd_line_args(argc, argv, &config_file, &workers);
    if (parse_res) return 1;

    // Parse the config file
    bloom_config *config = calloc(1, sizeof(bloom_config));
    int config_res = config_from_filename(config_file, config);
    if (config_res != 0) {
        syslog(LOG_ERR, "Failed to read the configuration file!");
        return 1;
    }

    // Set the workers if specified
    if (workers) config->worker_threads = workers;

    // Validate the config file
    int validate_res = validate_config(config);
    if (validate_res != 0) {
        syslog(LOG_ERR, "Invalid configuration!");
        return 1;
    }

    // Set the syslog mask
    setlogmask(config->syslog_log_level);

    // Log that we are starting up
    syslog(LOG_INFO, "Starting bloomd.");

    // Initialize the filters
    bloom_filtmgr *mgr;
    int mgr_res = init_filter_manager(config, 1, &mgr);
    if (mgr_res != 0) {
        syslog(LOG_ERR, "Failed to initialize bloomd filter manager!");
        return 1;
    }

    // Start the background tasks
    int flush_on, unmap_on;
    pthread_t flush_thread, unmap_thread;
    flush_on = start_flush_thread(config, mgr, &SHOULD_RUN, &flush_thread);
    unmap_on = start_cold_unmap_thread(config, mgr, &SHOULD_RUN, &unmap_thread);

    // Initialize the networking
    bloom_networking *netconf = NULL;
    int net_res = init_networking(config, mgr, &netconf);
    if (net_res != 0) {
        syslog(LOG_ERR, "Failed to initialize bloomd networking!");
        return 1;
    }

    // Start the network workers
    worker_args wargs = {mgr, netconf};
    pthread_t *threads = calloc(config->worker_threads, sizeof(pthread_t));
    for (int i=0; i < config->worker_threads; i++) {
        pthread_create(&threads[i], NULL, (void*(*)(void*))worker_main, &wargs);
    }

    // Prepare our signal handlers to loop until we are signaled to quit
    signal(SIGPIPE, SIG_IGN);       // Ignore SIG_IGN
    signal(SIGHUP, SIG_IGN);        // Ignore SIG_IGN
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Loop forever
    enter_main_loop(netconf, &SHOULD_RUN, threads);

    // Begin the shutdown/cleanup
    shutdown_networking(netconf, threads);

    // Shutdown the background tasks
    if (flush_on) pthread_join(flush_thread, NULL);
    if (unmap_on) pthread_join(unmap_thread, NULL);

    // Cleanup the filters
    destroy_filter_manager(mgr);

    // Free our memory
    free(threads);
    free(config);

    // Done
    return 0;
}

// Main entry point for the worker threads
static void *worker_main(worker_args *args) {
    // Perform the initial checkpoint with the manager
    filtmgr_client_checkpoint(args->mgr);

    // Enter the networking event loop forever
    start_networking_worker(args->netconf);

    return NULL;
}

