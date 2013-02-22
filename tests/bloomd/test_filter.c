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

static int filter_out_special(const struct dirent *d) {
    const char *name = d->d_name;
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
    fail_unless(bloomf_is_proxied(filter) == 0);

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
    fail_unless(bloomf_is_proxied(filter) == 0);

    res = bloomf_delete(filter);
    fail_unless(res == 0);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter2") == 0);
}
END_TEST

START_TEST(test_filter_init_proxied)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter3", 0, &filter);
    fail_unless(res == 0);

    filter_counters *counters = bloomf_counters(filter);
    fail_unless(counters->check_hits == 0);
    fail_unless(counters->check_misses == 0);
    fail_unless(counters->set_hits == 0);
    fail_unless(counters->set_misses == 0);
    fail_unless(counters->page_ins == 0);
    fail_unless(counters->page_outs == 0);

    fail_unless(bloomf_is_proxied(filter) == 1);
    fail_unless(bloomf_capacity(filter) == 100000);
    fail_unless(bloomf_byte_size(filter) == 0);
    fail_unless(bloomf_size(filter) == 0);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter3") == 0);
}
END_TEST

START_TEST(test_filter_add_check)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter4", 0, &filter);
    fail_unless(res == 0);

    filter_counters *counters = bloomf_counters(filter);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(bloomf_size(filter) == 10000);
    fail_unless(bloomf_byte_size(filter) > 32*1024);
    fail_unless(bloomf_capacity(filter) == 100000);
    fail_unless(counters->set_hits == 10000);

    // Check all the keys exist
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_contains(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(counters->check_hits == 10000);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter4") == 2);
}
END_TEST

START_TEST(test_filter_restore)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter5", 0, &filter);
    fail_unless(res == 0);
    filter_counters *counters = bloomf_counters(filter);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    // Destroy the filter
    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);

    // Remake the filter
    res = init_bloom_filter(&config, "test_filter5", 1, &filter);
    fail_unless(res == 0);
    counters = bloomf_counters(filter);

    // Re-check
    fail_unless(bloomf_size(filter) == 10000);
    fail_unless(bloomf_byte_size(filter) > 32*1024);
    fail_unless(bloomf_capacity(filter) == 100000);

    // Check all the keys exist
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_contains(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(counters->set_hits == 0);
    fail_unless(counters->check_hits == 10000);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter5") == 2);
}
END_TEST

START_TEST(test_filter_flush)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter6", 0, &filter);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    // Flush
    fail_unless(bloomf_flush(filter) == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter6/config.ini", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter6/data.000.mmap", 0777) == 0);

    // Remake the filter
    bloom_filter *filter2 = NULL;
    res = init_bloom_filter(&config, "test_filter6", 1, &filter2);
    fail_unless(res == 0);
    filter_counters *counters2 = bloomf_counters(filter2);

    // Re-check
    fail_unless(bloomf_size(filter2) == 10000);
    fail_unless(bloomf_byte_size(filter2) > 32*1024);
    fail_unless(bloomf_capacity(filter2) == 100000);

    // Check all the keys exist
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_contains(filter2, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(counters2->set_hits == 0);
    fail_unless(counters2->check_hits == 10000);

    // Destroy the filter
    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);

    res = destroy_bloom_filter(filter2);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter6") == 2);
}
END_TEST

START_TEST(test_filter_add_check_in_mem)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    config.in_memory = 1;
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter7", 0, &filter);
    fail_unless(res == 0);

    filter_counters *counters = bloomf_counters(filter);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(bloomf_size(filter) == 10000);
    fail_unless(bloomf_byte_size(filter) > 32*1024);
    fail_unless(bloomf_capacity(filter) == 100000);
    fail_unless(counters->set_hits == 10000);

    // Check all the keys exist
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_contains(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(counters->check_hits == 10000);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter7") == 1);
}
END_TEST

START_TEST(test_filter_grow)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.initial_capacity = 10000;

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter8", 1, &filter);
    fail_unless(res == 0);

    filter_counters *counters = bloomf_counters(filter);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<100000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
    }
    fail_unless(bloomf_size(filter) > 99000);
    fail_unless(bloomf_byte_size(filter) > 512*1024);
    fail_unless(bloomf_capacity(filter) == 210000);
    fail_unless(counters->set_hits > 99000);

    // Check all the keys exist
    for (int i=0;i<100000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_contains(filter, (char*)&buf);
    }
    fail_unless(counters->check_hits == 100000);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter8") == 4);
}
END_TEST

START_TEST(test_filter_grow_restore)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.initial_capacity = 10000;

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter9", 1, &filter);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<100000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
    }
    uint64_t size = bloomf_size(filter);
    uint64_t byte_size = bloomf_byte_size(filter);
    uint64_t cap = bloomf_capacity(filter);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter9/config.ini", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter9/data.000.mmap", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter9/data.001.mmap", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter9/data.002.mmap", 0777) == 0);

    res = init_bloom_filter(&config, "test_filter9", 1, &filter);
    fail_unless(res == 0);

    fail_unless(bloomf_size(filter) == size);
    fail_unless(bloomf_byte_size(filter) == byte_size);
    fail_unless(bloomf_capacity(filter) == cap);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);

    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter9") == 4);
}
END_TEST

START_TEST(test_filter_restore_order)
{
    /*
     * This case is from a real world bug we ran into.
     * The problem was that filters should be restored in the wrong
     * internal order.
     *
     * Internally, they are stored as index 0 is largest,
     * index n is smallest. On restore, we'd load the smallest
     * at index 0.
     *
     * This has no impact when there is only 1 filter, but when
     * there are more than one filters, we would load the smallest,
     * exhausted filter first. This means that the first add would
     * force a new filter to be added.
     *
     * It may also have a minor performance impact, as the smaller
     * filters are checked for keys first.
     */
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.initial_capacity = 10000;

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter12", 1, &filter);
    fail_unless(res == 0);

    // Add enough keys so that there are 2 filters.
    char buf[100];
    for (int i=0;i<20000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
    }

    uint64_t size = bloomf_size(filter);
    uint64_t byte_size = bloomf_byte_size(filter);
    uint64_t cap = bloomf_capacity(filter);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter12/config.ini", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter12/data.000.mmap", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter12/data.001.mmap", 0777) == 0);

    res = init_bloom_filter(&config, "test_filter12", 1, &filter);
    fail_unless(res == 0);

    fail_unless(bloomf_size(filter) == size);
    fail_unless(bloomf_byte_size(filter) == byte_size);
    fail_unless(bloomf_capacity(filter) == cap);

    // Adding some more keys should NOT cause
    // a new filter to be added
    for (int i=20000;i<21000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
    }

    fail_unless(bloomf_byte_size(filter) == byte_size);
    fail_unless(bloomf_capacity(filter) == cap);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);

    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter12") == 3);
}
END_TEST

START_TEST(test_filter_page_out)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter10", 0, &filter);
    fail_unless(res == 0);

    filter_counters *counters = bloomf_counters(filter);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_add(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(bloomf_close(filter) == 0);
    fail_unless(bloomf_size(filter) == 10000);
    fail_unless(bloomf_capacity(filter) == 100000);
    fail_unless(counters->page_outs == 1);
    fail_unless(counters->page_ins == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.test_filter10/data.000.mmap", 0777) == 0);

    // Check all the keys exist
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = bloomf_contains(filter, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(counters->check_hits == 10000);
    fail_unless(counters->page_outs == 1);
    fail_unless(counters->page_ins == 1);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    fail_unless(delete_dir("/tmp/bloomd/bloomd.test_filter10") == 2);
}
END_TEST

START_TEST(test_filter_bounded_fp)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.initial_capacity = 10000;
    config.in_memory = 1;
    config.default_probability = 0.001; // 1/1K

    bloom_filter *filter = NULL;
    res = init_bloom_filter(&config, "test_filter11", 1, &filter);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<1000000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        bloomf_add(filter, (char*)&buf);
    }

    filter_counters *counters = bloomf_counters(filter);
    fail_unless(bloomf_size(filter) > 999000);
    fail_unless(bloomf_capacity(filter) > 1000000);
    fail_unless(counters->set_hits > 990000);
    fail_unless(counters->set_misses < 1000);

    // Check all the keys exist
    for (int i=0;i<1000000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        bloomf_contains(filter, (char*)&buf);
    }
    fail_unless(counters->check_hits == 1000000);

    res = destroy_bloom_filter(filter);
    fail_unless(res == 0);
    delete_dir("/tmp/bloomd/bloomd.test_filter11");
}
END_TEST

