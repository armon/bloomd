#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "config.h"
#include "filter.h"

/**
 * Initializes a bloom filter wrapper.
 * @arg config The configuration to use
 * @arg filter_name The name of the filter
 * @arg discover Should existing data files be discovered. Otherwise
 * they will be faulted in on-demand.
 * @arg filter Output parameter, the new filter
 * @return 0 on success
int init_bloom_filter(bloom_config *config, char *filter_name, int discover, bloom_filter **filter);
 */

START_TEST(test_filter_init_destroy)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter", 0, &filter);
    fail_unless(res == 0);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
}
END_TEST
