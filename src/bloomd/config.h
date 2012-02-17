#ifndef BLOOM_CONFIG_H
#define BLOOM_CONFIG_H
#include <stdint.h>
#include <syslog.h>

/**
 * Stores our configuration
 */
typedef struct {
    int tcp_port;
    int udp_port;
    char *data_dir;
    char *log_level;
    int syslog_log_level;
    uint64_t initial_capacity;
    double default_probability;
    int scale_size;
    double probability_reduction;
    int flush_interval;
    int cold_interval;
    int in_memory;
} bloom_config;

/**
 * Initializes the configuration from a filename.
 * Reads the file as an INI configuration, and sets up the
 * config object.
 * @arg filename The name of the file to read. NULL for defaults.
 * @arg config Output. The config object to initialize.
 * @return 0 on success, negative on error.
 */
int config_from_filename(char *filename, bloom_config *config);

/**
 * Validates the configuration
 * @arg config The config object to validate.
 * @return 0 on success, negative on error.
 */
int validate_config(bloom_config *config);

// Configuration validation methods
int sane_data_dir(char *data_dir);
int sane_log_level(char *log_level, int *syslog_level);
int sane_initial_capacity(int64_t initial_capacity);
int sane_default_probability(double prob);
int sane_scale_size(int scale_size);
int sane_probability_reduction(double reduction);
int sane_flush_interval(int intv);
int sane_cold_interval(int intv);
int sane_in_memory(int in_mem);

/**
 * Joins two strings as part of a path,
 * and adds a separating slash if needed.
 * @param path Part one of the path
 * @param part2 The second part of the path
 * @return A new string, that uses a malloc()'d buffer.
 */
char* join_path(char *path, char *part2);

#endif
