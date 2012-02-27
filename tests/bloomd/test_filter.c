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

START_TEST(test_filter_init_discover_destroy)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter", 1, &filter);
    fail_unless(res == 0);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
}
END_TEST

