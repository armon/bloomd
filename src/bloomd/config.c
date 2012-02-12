#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <syslog.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "config.h"
#include "ini.h"

/**
 * Attempts to convert a string to an integer,
 * and write the value out.
 * @arg val The string value
 * @arg result The destination for the result
 * @return 1 on success, 0 on error.
 */
int value_to_int(const char *val, int *result) {
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
int value_to_int64(const char *val, uint64_t *result) {
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
int value_to_double(const char *val, double *result) {
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
int config_callback(void* user, const char* section, const char* name, const char* value) {
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
    int len = strlen(path);
    int has_end_slash = path[len-1] == '/';
    int total_len = len + strlen(part2);

    // Add an extra byte for the missing slash
    if (!has_end_slash)
        total_len++;

    // Make new buffer
    char *buf = malloc(total_len * sizeof(char));

    // Copy part 1
    memcpy(buf, path, len);

    // Add the slash
    if (!has_end_slash) {
        buf[len] = '/';
        len++;
    }

    // Copy part 2
    memcpy(buf+len, part2, total_len-len);

    // Return the new buffer
    return buf;
}

int sane_data_dir(char *data_dir) {
    // Check if the path exists, and it is not a dir
    struct stat buf;
    int res = stat(data_dir, &buf);
    if (res == 0) {
        if (buf.st_mode & S_IFDIR == 0) {
            syslog(LOG_ERR,
                   "Provided data directory exists and is not a directory!");
            return 1;
        } 
    } else  {
        // Try to make the directory
        res = mkdir(data_dir, 0775);
        if (res != 0) {
            syslog(LOG_ERR,
                   "Failed to make the data directory!");
            return 1;
        }
    }

    // Try to test we have permissions to write
    char *test_path = join_path(data_dir, "PERMTEST");
    int fh = open(test_path, O_CREAT|O_RDWR);
    
    // Cleanup
    if (fh != -1) close(fh);
    unlink(test_path);
    free(test_path);

    // If we failed to open the file, error
    if (fh == -1) {
        syslog(LOG_ERR,
               "Failed to write to data directory!");
        return 1;
    }
    
    return 0;
}

int sane_log_level(char *log_level, int *syslog_level) {
    #define LOG_MATCH(lvl) (strcasecmp(lvl, log_level) == 0)
    if (LOG_MATCH("DEBUG")) {
        *syslog_level = LOG_DEBUG;
    } else if (LOG_MATCH("INFO")) {
        *syslog_level = LOG_INFO;
    } else if (LOG_MATCH("WARN")) {
        *syslog_level = LOG_WARNING;
    } else if (LOG_MATCH("ERROR")) {
        *syslog_level = LOG_ERR;
    } else if (LOG_MATCH("CRITICAL")) {
        *syslog_level = LOG_CRIT;
    } else {
        syslog(LOG_ERR, "Unknown log level!");
        return 1;
    }
    return 0;
}

int sane_initial_capacity(int64_t initial_capacity) {
    if (initial_capacity <= 10000) {
        syslog(LOG_ERR, 
               "Initial capacity cannot be less than 10K!");
        return 1;
    } else if (initial_capacity > 1000000000) {
        syslog(LOG_WARNING, "Initial capacity set very high!");
    } 
    return 0;
}

int sane_default_probability(double prob) {
    if (prob >= 1) {
        syslog(LOG_ERR, 
               "Probability cannot be equal-to or greater than 1!");
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
    } else if (reduction < 0.1) {
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
    } 
    return 0;
}

int sane_in_memory(int in_mem) {
    if (in_mem != 0) {
        syslog(LOG_WARNING, 
               "Default filters are in-memory only! Filters not persisted by default.");
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
    
    return res;
}

