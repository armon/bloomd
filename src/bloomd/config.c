#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <syslog.h>
#include <unistd.h>
#include "config.h"
#include "ini.h"

/**
 * Default bloom_config values. Should create
 * filters that are about 300KB initially, and suited
 * to grow quickly.
 */
static const bloom_config DEFAULT_CONFIG = {
    8673,               // TCP defaults to 8673
    8674,               // UDP on 8674
    "0.0.0.0",          // Listen on all IPv4 addresses
    "/tmp/bloomd",      // Tmp data dir, until configured
    "DEBUG",            // DEBUG level
    LOG_DEBUG,
    100000,             // 100K items by default.
    1e-4,               // Default 1/10K probability.
    4,                  // Scale 4x, SBF_DEFAULT_PARAMS
    0.9,                // SBF_DEFAULT_PARAMS reduction
    60,                 // Flush once a minute
    3600,               // Cold after an hour
    0,                  // Persist to disk by default
    1,                  // Only a single worker thread by default
    0                   // Do NOT use mmap by default
};

/**
 * Attempts to convert a string to an integer,
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 1 on success, 0 on error.
 */
static int value_to_int(const char *val, int *result) {
    long res = strtol(val, NULL, 10);
    if (res == 0 && errno == EINVAL) {
        return 0;
    }
    *result = res;
    return 1;
}

/**
 * Attempts to convert a string to an integer (64bit),
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 0 on success, 0 on error.
 */
static int value_to_int64(const char *val, uint64_t *result) {
    long long res = strtoll(val, NULL, 10);
    if (res == 0 && errno == EINVAL) {
        return 0;
    }
    *result = res;
    return 1;
}

/**
 * Attempts to convert a string to a double,
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 0 on success, -EINVAL on error.
 */
static int value_to_double(const char *val, double *result) {
    double res = strtod(val, NULL);
    if (res == 0) {
        return 0;
    }
    *result = res;
    return 0;
}

/**
 * Callback function to use with INI-H.
 * @arg user Opaque user value. We use the bloom_config pointer
 * @arg section The INI seciton
 * @arg name The config name
 * @arg value The config value
 * @return 1 on success.
 */
static int config_callback(void* user, const char* section, const char* name, const char* value) {
    // Ignore any non-bloomd sections
    if (strcasecmp("bloomd", section) != 0) {
        return 0;
    }

    // Cast the user handle
    bloom_config *config = (bloom_config*)user;

    // Switch on the config
    #define NAME_MATCH(param) (strcasecmp(param, name) == 0)

    // Handle the int cases
    if (NAME_MATCH("port")) {
        return value_to_int(value, &config->tcp_port);
    } else if (NAME_MATCH("tcp_port")) {
        return value_to_int(value, &config->tcp_port);
    } else if (NAME_MATCH("udp_port")) {
        return value_to_int(value, &config->udp_port);
    } else if (NAME_MATCH("scale_size")) {
        return value_to_int(value, &config->scale_size);
    } else if (NAME_MATCH("flush_interval")) {
         return value_to_int(value, &config->flush_interval);
    } else if (NAME_MATCH("cold_interval")) {
         return value_to_int(value, &config->cold_interval);
    } else if (NAME_MATCH("in_memory")) {
         return value_to_int(value, &config->in_memory);
    } else if (NAME_MATCH("use_mmap")) {
         return value_to_int(value, &config->use_mmap);
    } else if (NAME_MATCH("workers")) {
         return value_to_int(value, &config->worker_threads);

    // Handle the int64 cases
    } else if (NAME_MATCH("initial_capacity")) {
         return value_to_int64(value, &config->initial_capacity);

    // Handle the double cases
    } else if (NAME_MATCH("default_probability")) {
         return value_to_double(value, &config->default_probability);
    } else if (NAME_MATCH("probability_reduction")) {
         return value_to_double(value, &config->probability_reduction);

    // Copy the string values
    } else if (NAME_MATCH("data_dir")) {
        config->data_dir = strdup(value);
    } else if (NAME_MATCH("log_level")) {
        config->log_level = strdup(value);
    } else if (NAME_MATCH("bind_address")) {
        config->bind_address = strdup(value);

    // Unknown parameter?
    } else {
        // Log it, but ignore
        syslog(LOG_NOTICE, "Unrecognized config parameter: %s", value);
    }

    // Success
    return 1;
}

/**
 * Initializes the configuration from a filename.
 * Reads the file as an INI configuration, and sets up the
 * config object.
 * @arg filename The name of the file to read. NULL for defaults.
 * @arg config Output. The config object to initialize.
 * @return 0 on success, negative on error.
 */
int config_from_filename(char *filename, bloom_config *config) {
    // Initialize to the default values
    memcpy(config, &DEFAULT_CONFIG, sizeof(bloom_config));

    // If there is no filename, return now
    if (filename == NULL)
        return 0;

    // Try to open the file
    int res = ini_parse(filename, config_callback, config);
    if (res == -1) {
        return -ENOENT;
    }

    return 0;
}

/**
 * Joins two strings as part of a path,
 * and adds a separating slash if needed.
 * @param path Part one of the path
 * @param part2 The second part of the path
 * @return A new string, that uses a malloc()'d buffer.
 */
char* join_path(char *path, char *part2) {
    // Check for the end slash
    int len = strlen(path);
    int has_end_slash = path[len-1] == '/';

    // Use the proper format string
    char *buf;
    int res;
    if (has_end_slash)
        res = asprintf(&buf, "%s%s", path, part2);
    else
        res = asprintf(&buf, "%s/%s", path, part2);
    assert(res != -1);

    // Return the new buffer
    return buf;
}

int sane_data_dir(char *data_dir) {
    // Check if the path exists, and it is not a dir
    struct stat buf;
    int res = stat(data_dir, &buf);
    if (res == 0) {
        if ((buf.st_mode & S_IFDIR) == 0) {
            syslog(LOG_ERR,
                   "Provided data directory exists and is not a directory!");
            return 1;
        }
    } else  {
        // Try to make the directory
        res = mkdir(data_dir, 0775);
        if (res != 0) {
            syslog(LOG_ERR,
                   "Failed to make the data directory! Err: %s", strerror(errno));
            return 1;
        }
    }

    // Try to test we have permissions to write
    char *test_path = join_path(data_dir, "PERMTEST");
    int fh = open(test_path, O_CREAT|O_RDWR, 0644);

    // Cleanup
    if (fh != -1) close(fh);
    unlink(test_path);
    free(test_path);

    // If we failed to open the file, error
    if (fh == -1) {
        syslog(LOG_ERR,
               "Failed to write to data directory! Err: %s", strerror(errno));
        return 1;
    }

    return 0;
}

int sane_log_level(char *log_level, int *syslog_level) {
    #define LOG_MATCH(lvl) (strcasecmp(lvl, log_level) == 0)
    if (LOG_MATCH("DEBUG")) {
        *syslog_level = LOG_UPTO(LOG_DEBUG);
    } else if (LOG_MATCH("INFO")) {
        *syslog_level = LOG_UPTO(LOG_INFO);
    } else if (LOG_MATCH("WARN")) {
        *syslog_level = LOG_UPTO(LOG_WARNING);
    } else if (LOG_MATCH("ERROR")) {
        *syslog_level = LOG_UPTO(LOG_ERR);
    } else if (LOG_MATCH("CRITICAL")) {
        *syslog_level = LOG_UPTO(LOG_CRIT);
    } else {
        syslog(LOG_ERR, "Unknown log level!");
        return 1;
    }
    return 0;
}

int sane_initial_capacity(int64_t initial_capacity) {
    if (initial_capacity <= 10000) {  // 1e4, 10K
        syslog(LOG_ERR,
               "Initial capacity cannot be less than 10K!");
        return 1;
    } else if (initial_capacity > 1000000000) {  // 1e9, 1G
        syslog(LOG_WARNING, "Initial capacity set very high!");
    }
    return 0;
}

int sane_default_probability(double prob) {
    if (prob >= 1) {
        syslog(LOG_ERR,
               "Probability cannot be equal-to or greater than 1!");
        return 1;
    } else if (prob >= 0.10) {  // prob must < 0.10
        syslog(LOG_ERR, "Default probability too high!");
        return 1;
    } else if (prob > 0.01) {
        syslog(LOG_WARNING, "Default probability very high!");
    } else if (prob <= 0) {
        syslog(LOG_ERR,
               "Probability cannot less than or equal to 0!");
        return 1;
    }
    return 0;
}

int sane_scale_size(int scale_size) {
    if (scale_size != 2 && scale_size != 4) {
        syslog(LOG_ERR,
               "Scale size must be 2 or 4!");
        return 1;
    }
    return 0;
}

int sane_probability_reduction(double reduction) {
    if (reduction >= 1) {
        syslog(LOG_ERR,
               "Probability reduction cannot be equal-to or greater than 1!");
        return 1;
    } else if (reduction <= 0.1) {
        syslog(LOG_ERR, "Probability drop off is set too steep!");
        return 1;
    } else if (reduction <= 0.5)  {
        syslog(LOG_WARNING,
               "Probability drop off is very steep!");
    }
    return 0;
}

int sane_flush_interval(int intv) {
    if (intv == 0) {
        syslog(LOG_WARNING,
               "Flushing is disabled! Increased risk of data loss.");
    } else if (intv < 0) {
        syslog(LOG_ERR, "Flush interval cannot be negative!");
        return 1;
    } else if (intv >= 600)  {
        syslog(LOG_WARNING,
               "Flushing set to be very infrequent! Increased risk of data loss.");
    }
    return 0;
}

int sane_cold_interval(int intv) {
    if (intv == 0) {
        syslog(LOG_WARNING,
               "Cold data unmounting is disabled! Memory usage may be high.");
    } else if (intv < 0) {
        syslog(LOG_ERR, "Cold interval cannot be negative!");
        return 1;
    } else if (intv < 300) {
        syslog(LOG_ERR, "Cold interval is less than 5 minutes. \
This may cause excessive unmapping to occur.");
    }

    return 0;
}

int sane_in_memory(int in_mem) {
    if (in_mem != 0) {
        syslog(LOG_WARNING,
               "Default filters are in-memory only! Filters not persisted by default.");
    }
    if (in_mem != 0 && in_mem != 1) {
        syslog(LOG_ERR,
               "Illegal value for in-memory. Must be 0 or 1.");
        return 1;
    }

    return 0;
}

int sane_use_mmap(int use_mmap) {
    if (use_mmap != 1) {
        syslog(LOG_WARNING,
               "Without use_mmap, a crash of bloomd can result in data loss.");
    }
    if (use_mmap != 0 && use_mmap != 1) {
        syslog(LOG_ERR,
               "Illegal value for use_mmap. Must be 0 or 1.");
        return 1;
    }
    return 0;
}

int sane_worker_threads(int threads) {
    if (threads <= 0) {
        syslog(LOG_ERR,
               "Cannot have fewer than one worker thread!");
        return 1;
    }
    return 0;
}


/**
 * Validates the configuration
 * @arg config The config object to validate.
 * @return 0 on success.
 */
int validate_config(bloom_config *config) {
    int res = 0;

    res |= sane_data_dir(config->data_dir);
    res |= sane_log_level(config->log_level, &config->syslog_log_level);
    res |= sane_initial_capacity(config->initial_capacity);
    res |= sane_default_probability(config->default_probability);
    res |= sane_scale_size(config->scale_size);
    res |= sane_probability_reduction(config->probability_reduction);
    res |= sane_flush_interval(config->flush_interval);
    res |= sane_cold_interval(config->cold_interval);
    res |= sane_in_memory(config->in_memory);
    res |= sane_use_mmap(config->use_mmap);
    res |= sane_worker_threads(config->worker_threads);

    return res;
}

/**
 * Callback function to use with INI-H.
 * @arg user Opaque user value. We use the bloom_config pointer
 * @arg section The INI seciton
 * @arg name The config name
 * @arg value The config value
 * @return 1 on success.
 */
static int filter_config_callback(void* user, const char* section, const char* name, const char* value) {
    // Ignore any non-bloomd sections
    if (strcasecmp("bloomd", section) != 0) {
        return 0;
    }

    // Cast the user handle
    bloom_filter_config *config = (bloom_filter_config*)user;

    // Switch on the config
    #define NAME_MATCH(param) (strcasecmp(param, name) == 0)

    // Handle the int cases
    if (NAME_MATCH("scale_size")) {
        return value_to_int(value, &config->scale_size);
    } else if (NAME_MATCH("in_memory")) {
         return value_to_int(value, &config->in_memory);

    // Handle the int64 cases
    } else if (NAME_MATCH("initial_capacity")) {
         return value_to_int64(value, &config->initial_capacity);
    } else if (NAME_MATCH("size")) {
         return value_to_int64(value, &config->size);
    } else if (NAME_MATCH("capacity")) {
         return value_to_int64(value, &config->capacity);
    } else if (NAME_MATCH("bytes")) {
         return value_to_int64(value, &config->bytes);

    // Handle the double cases
    } else if (NAME_MATCH("default_probability")) {
         return value_to_double(value, &config->default_probability);
    } else if (NAME_MATCH("probability_reduction")) {
         return value_to_double(value, &config->probability_reduction);

    // Unknown parameter?
    } else {
        // Log it, but ignore
        syslog(LOG_NOTICE, "Unrecognized filter config parameter: %s", value);
    }

    // Success
    return 1;
}

/**
 * Updates the configuration from a filename.
 * Reads the file as an INI configuration and updates the config.
 * @arg filename The name of the file to read.
 * @arg config Output. The config object to update. Does not initialize!
 * @return 0 on success, negative on error.
 */
int filter_config_from_filename(char *filename, bloom_filter_config *config) {
    // If there is no filename, return now
    if (filename == NULL)
        return 0;

    // Try to open the file
    int res = ini_parse(filename, filter_config_callback, config);
    if (res == -1) {
        return -ENOENT;
    }

    return 0;
}

/**
 * Writes the configuration to a filename.
 * Writes the file as an INI configuration
 * @arg filename The name of the file to write.
 * @arg config The config object to write out.
 * @return 0 on success, negative on error.
 */
int update_filename_from_filter_config(char *filename, bloom_filter_config *config) {
    // Try to open the file
    FILE* f = fopen(filename, "w+");
    if (!f) return -errno;

    // Write out
    fprintf(f, "[bloomd]\n\
initial_capacity = %llu\n\
default_probability = %f\n\
scale_size = %d\n\
probability_reduction = %f\n\
in_memory = %d\n\
size = %llu\n\
capacity = %llu\n\
bytes = %llu\n", (unsigned long long)config->initial_capacity,
                 config->default_probability,
                 config->scale_size,
                 config->probability_reduction,
                 config->in_memory,
                 (unsigned long long)config->size,
                 (unsigned long long)config->capacity,
                 (unsigned long long)config->bytes
    );

    // Close
    fclose(f);
    return 0;
}

