#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include "config.h"
#include "filter.h"
#include "filter_manager.h"

START_TEST(test_mgr_init_destroy)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, &mgr);
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

