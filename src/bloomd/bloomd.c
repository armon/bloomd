/**
 * This is the main entry point into bloomd.
 * We are responsible for parsing any commmand line
 * flags, reading the configuration, starting
 * the filter manager, and finally starting the
 * front ends.
 */
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <syslog.h>
#include <pthread.h>
#include "config.h"
#include "networking.h"

/**
 * Prints our usage to stderr
 */
void show_usage() {
    fprintf(stderr, "usage: bloomd [-h] [-f filename]\n\
\n\
    -h : Displays this help info\n\
    -f : Reads the bloomd configuration from this file\n\
\n");
}

/**
 * Invoked to parse the command line options
 */
int parse_cmd_line_args(int argc, char **argv, char **config_file) {
    int enable_help = 0;

    int c;
    opterr = 0;
    while ((c = getopt(argc, argv, "hf:")) != -1) {
        switch (c) {
            case 'h':
                enable_help = 1;
                break;
            case 'f':
                *config_file = optarg;
                break;
            case '?':
                if (optopt == 'f')
                    fprintf(stderr, "Option -%c requires a filename.\n", optopt);
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


int main(int argc, char **argv) {
    // Initialize syslog
    setup_syslog();

    // Parse the command line
    char *config_file = NULL;
    int parse_res = parse_cmd_line_args(argc, argv, &config_file);
    if (parse_res) return 1;

    // Parse the config file
    bloom_config *config = calloc(1, sizeof(bloom_config));
    int config_res = config_from_filename(config_file, config);
    if (config_res != 0) {
        syslog(LOG_ERR, "Failed to read the configuration file!");
        return 1;
    }

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

    // TODO: Initialize the filters
    //

    // Initialize the networking
    bloom_networking *netconf = NULL;
    int net_res = init_networking(config, &netconf);
    if (net_res != 0) {
        syslog(LOG_ERR, "Failed to initialize bloomd networking!");
        return 1;
    }

    // Start the network workers
    pthread_t thread;
    for (int i; i < config->worker_threads; i++) {
        pthread_create(&thread, NULL, (void*(*)(void*))start_networking_worker, netconf);
    }

    // TODO: MAIN... Loop forevaaaar

    // Begin the shutdown/cleanup
    shutdown_networking(netconf);

    // TODO: Cleanup the filters

    // Free our memory
    free(config);

    // Done
    return 0;
}
