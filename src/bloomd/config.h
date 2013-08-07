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
    char *bind_address;
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
    int worker_threads;
    int use_mmap;
} bloom_config;

/**
 * This structure is used to persist
 * filter specific settings to an INI file.
 */
typedef struct {
    uint64_t initial_capacity;
    double default_probability;
    int scale_size;
    double probability_reduction;
    int in_memory;
    uint64_t size;          // Total size
    uint64_t capacity;      // Total capacity
    uint64_t bytes;         // Total byte size
} bloom_filter_config;


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
 * Updates the configuration from a filename.
 * Reads the file as an INI configuration and updates the config.
 * @arg filename The name of the file to read.
 * @arg config Output. The config object to update. Does not initialize!
 * @return 0 on success, negative on error.
 */
int filter_config_from_filename(char *filename, bloom_filter_config *config);

/**
 * Writes the configuration to a filename.
 * Writes the file as an INI configuration
 * @arg filename The name of the file to write.
 * @arg config The config object to write out.
 * @return 0 on success, negative on error.
 */
int update_filename_from_filter_config(char *filename, bloom_filter_config *config);

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
int sane_use_mmap(int use_mmap);
int sane_worker_threads(int threads);

/**
 * Joins two strings as part of a path,
 * and adds a separating slash if needed.
 * @param path Part one of the path
 * @param part2 The second part of the path
 * @return A new string, that uses a malloc()'d buffer.
 */
char* join_path(char *path, char *part2);

#endif
