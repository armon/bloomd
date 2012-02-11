#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <syslog.h>
#include "config.h"
#include "deps/inih/ini.h"

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
    #define MATCH(param) (strcasecmp(param, name) == 0)

    // Handle the int cases
    if (MATCH("port")) {
        return value_to_int(value, &config->tcp_port);
    } else if (MATCH("udp_port")) {
        return value_to_int(value, &config->udp_port);
    } else if (MATCH("scale_size")) {
        return value_to_int(value, &config->scale_size);
    } else if (MATCH("flush_interval")) {
         return value_to_int(value, &config->flush_interval);  
    } else if (MATCH("cold_interval")) {
         return value_to_int(value, &config->cold_interval);  
    } else if (MATCH("in_memory")) {
         return value_to_int(value, &config->in_memory);  

    // Handle the int64 cases
    } else if (MATCH("initial_capacity")) {
         return value_to_int64(value, &config->initial_capacity);  

    // Handle the double cases
    } else if (MATCH("default_probability")) {
         return value_to_double(value, &config->default_probability);  
    } else if (MATCH("probability_reduction")) {
         return value_to_double(value, &config->probability_reduction);  

    // Copy the string values
    } else if (MATCH("data_dir")) {
        config->data_dir = strdup(value); 
    } else if (MATCH("log_level")) {
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

