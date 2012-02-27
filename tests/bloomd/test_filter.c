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

static int filter_out_special(struct dirent *d) {
    char *name = d->d_name;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return 0;
    }
    return 1;
}

static int delete_dir(char *path) {
    // Delete the files
    struct dirent **namelist = NULL;
    int num;

    // Filter only data dirs, in sorted order
    num = scandir(path, &namelist, filter_out_special, NULL);
    if (num == -1) return 0;

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        char *file_path = join_path(path, namelist[i]->d_name);
        if (unlink(file_path)) {
            printf("Failed to delete: %s. %s\n", file_path, strerror(errno));
        }
        free(file_path);
    }

    // Free the memory associated with scandir
    for (int i=0; i < num; i++) {
        free(namelist[i]);
    }
    if (namelist != NULL) free(namelist);

    // Delete the directory
    if (rmdir(path)) {
        printf("Failed to delete dir: %s. %s\n", path, strerror(errno));
    }
    return num;
}

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
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter") == 2);
}
END_TEST

START_TEST(test_filter_init_discover_delete)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter2", 1, &filter);
    fail_unless(res == 0);

    res = bloomf_delete(filter);
    fail_unless(res == 0);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter2") == 0);
}
END_TEST

