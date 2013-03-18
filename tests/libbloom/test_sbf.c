#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "sbf.h"


START_TEST(sbf_initial_size)
{
    bloom_filter_params config_params = {0, 0, 1e4, 1e-4};
    bf_params_for_capacity(&config_params);

    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e4;
    params.fp_probability = 1e-4;
    bloom_sbf sbf;
    int res = sbf_from_filters(&params, NULL, NULL, 0, NULL, &sbf);
    fail_unless(res == 0);
    fail_unless(sbf_size(&sbf) == 0);
    fail_unless(sbf.num_filters == 1);
    fail_unless(sbf_total_capacity(&sbf) == 1e4);
}
END_TEST

START_TEST(sbf_add_filter)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-4;
    bloom_sbf sbf;
    int res = sbf_from_filters(&params, NULL, NULL, 0, NULL, &sbf);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_add(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(sbf_size(&sbf) == 2000);
    fail_unless(sbf.num_filters == 2);
    fail_unless(sbf_total_capacity(&sbf) == 5*1e3);

    // Check all the keys exist
    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_contains(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }
}
END_TEST

START_TEST(sbf_add_filter_2)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-5;
    bloom_sbf sbf;
    int res = sbf_from_filters(&params, NULL, NULL, 0, NULL, &sbf);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_add(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }

    fail_unless(sbf_size(&sbf) == 10000);
    fail_unless(sbf.num_filters == 3);
    fail_unless(sbf_total_capacity(&sbf) == 21*1e3);

    // Check all the keys exist
    for (int i=0;i<10000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_contains(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }

    // Byte size should be greater than a static filter of the same config
    bloom_filter_params config_params = {0, 0, 21e3, 1e-4};
    bf_params_for_capacity(&config_params);
    uint64_t total_size = sbf_total_byte_size(&sbf);
    fail_unless(total_size > config_params.bytes);
    fail_unless(total_size < 2 * config_params.bytes);
}
END_TEST

// Tests the call back, increments the in counter
static int sbf_test_callback(void *in, uint64_t bytes, bloom_bitmap *map) {
    uint64_t *counter = (uint64_t*)in;
    *counter = *counter + 1;
    return bitmap_from_file(-1, bytes, ANONYMOUS, map);
}

START_TEST(sbf_callback)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-4;

    bloom_sbf sbf;
    uint64_t counter = 0;
    int res = sbf_from_filters(&params, sbf_test_callback, &counter, 0, NULL, &sbf);
    fail_unless(res == 0);

    // Check all the keys get added
    char buf[100];
    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_add(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }
    fail_unless(sbf_size(&sbf) == 2000);
    fail_unless(sbf.num_filters == 2);
    fail_unless(counter == 2);
}
END_TEST

START_TEST(test_sbf_double_close)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-4;
    bloom_sbf sbf;
    int res = sbf_from_filters(&params, NULL, NULL, 0, NULL, &sbf);
    fail_unless(res == 0);

    res = sbf_close(&sbf);
    fail_unless(res == 0);

    res = sbf_close(&sbf);
    fail_unless(res == -1);
}
END_TEST

START_TEST(test_sbf_flush_close)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-4;
    bloom_sbf sbf;
    int res = sbf_from_filters(&params, NULL, NULL, 0, NULL, &sbf);
    fail_unless(res == 0);

    res = sbf_flush(&sbf);
    fail_unless(res == 0);

    res = sbf_close(&sbf);
    fail_unless(res == 0);
}
END_TEST

// Makes files
typedef struct {
    uint32_t num;
    char *format;
} nextfile;

static int sbf_make_callback(void *in, uint64_t bytes, bloom_bitmap *map) {
    char buf[1000];
    nextfile *n = in;
    snprintf(buf, 1000, n->format, n->num);
    n->num++;
    int res = bitmap_from_filename((char*)&buf, bytes, 1, SHARED, map);
    fchmod(map->fileno, 0777);
    return res;
}

static uint64_t get_size(char* filename) {
    struct stat buf;
    stat(filename, &buf);
    return buf.st_size;
}

START_TEST(test_sbf_flush)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-4;

    nextfile next;
    next.format = "/tmp/mmap_flush.%d.data";
    next.num = 0;

    bloom_sbf sbf;
    int res = sbf_from_filters(&params, sbf_make_callback, &next, 0, NULL, &sbf);
    fail_unless(res == 0);

    char buf[100];
    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        sbf_add(&sbf, (char*)&buf);
    }

    fail_unless(sbf_flush(&sbf) == 0);

    bloom_bitmap maps[2];
    bitmap_from_filename("/tmp/mmap_flush.0.data", get_size("/tmp/mmap_flush.0.data"), 1, SHARED, (bloom_bitmap*)&maps);
    bitmap_from_filename("/tmp/mmap_flush.1.data", get_size("/tmp/mmap_flush.1.data"), 1, SHARED, ((bloom_bitmap*)&maps)+1);

    bloom_bloomfilter filters[2];
    bf_from_bitmap((bloom_bitmap*)&maps, 1, 0, (bloom_bloomfilter*)&filters);
    bf_from_bitmap(((bloom_bitmap*)&maps)+1, 1, 0, ((bloom_bloomfilter*)&filters)+1);

    bloom_bloomfilter **filter_map = calloc(2, sizeof(bloom_bloomfilter*));
    filter_map[0] = (bloom_bloomfilter*)&filters;
    filter_map[1] = ((bloom_bloomfilter*)&filters)+1;

    bloom_sbf sbf2;
    res = sbf_from_filters(&params, sbf_make_callback, &next, 2, filter_map, &sbf2);
    fail_unless(res == 0);

    fail_unless(sbf_size(&sbf) == sbf_size(&sbf2));
    fail_unless(sbf_total_capacity(&sbf) == sbf_total_capacity(&sbf2));
    fail_unless(sbf_total_byte_size(&sbf) == sbf_total_byte_size(&sbf2));

    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_contains(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }

    unlink("/tmp/mmap_flush.0.data");
    unlink("/tmp/mmap_flush.1.data");
}
END_TEST

START_TEST(test_sbf_close_does_flush)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e3;
    params.fp_probability = 1e-4;

    nextfile next;
    next.format = "/tmp/mmap_close.%d.data";
    next.num = 0;

    bloom_sbf sbf;
    int res = sbf_from_filters(&params, sbf_make_callback, &next, 0, NULL, &sbf);
    fail_unless(res == 0);

    char buf[100];
    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        sbf_add(&sbf, (char*)&buf);
    }
    fail_unless(sbf_close(&sbf) == 0);

    bloom_bitmap maps[2];
    bitmap_from_filename("/tmp/mmap_close.0.data", get_size("/tmp/mmap_close.0.data"), 1, SHARED, (bloom_bitmap*)&maps);
    bitmap_from_filename("/tmp/mmap_close.1.data", get_size("/tmp/mmap_close.1.data"), 1, SHARED, ((bloom_bitmap*)&maps)+1);

    bloom_bloomfilter filters[2];
    bf_from_bitmap((bloom_bitmap*)&maps, 1, 0, (bloom_bloomfilter*)&filters);
    bf_from_bitmap(((bloom_bitmap*)&maps)+1, 1, 0, ((bloom_bloomfilter*)&filters)+1);

    bloom_bloomfilter **filter_map = calloc(2, sizeof(bloom_bloomfilter*));
    filter_map[0] = (bloom_bloomfilter*)&filters;
    filter_map[1] = ((bloom_bloomfilter*)&filters)+1;

    res = sbf_from_filters(&params, sbf_make_callback, &next, 2, filter_map, &sbf);
    fail_unless(res == 0);

    fail_unless(sbf_size(&sbf) == 2000);
    fail_unless(sbf_total_capacity(&sbf) == 5*1e3);

    for (int i=0;i<2000;i++) {
        snprintf((char*)&buf, 100, "foobar%d", i);
        res = sbf_contains(&sbf, (char*)&buf);
        fail_unless(res == 1);
    }

    unlink("/tmp/mmap_close.0.data");
    unlink("/tmp/mmap_close.1.data");
}
END_TEST


START_TEST(sbf_fp_prob)
{
    bloom_sbf_params params = SBF_DEFAULT_PARAMS;
    params.initial_capacity = 1e4;
    params.fp_probability = 0.01;
    bloom_sbf sbf;
    int res = sbf_from_filters(&params, NULL, NULL, 0, NULL, &sbf);
    fail_unless(res == 0);

    char buf[100];
    int num_wrong = 0;
    int wrong_per[5] = {0, 0, 0, 0, 0};
    for (int i=0;i<1e6;i++) {
        snprintf((char*)&buf, 100, "ZibZab__%d", i*i);
        res = sbf_add(&sbf, (char*)&buf);
        if (res == 0) {
            num_wrong++;
            wrong_per[sbf.num_filters-1]++;
        }
    }

    fail_unless(sbf.num_filters == 5);
    fail_unless(num_wrong > 1000);   // Expected error rate
    fail_unless(num_wrong < 10000);  // Expected error rate
}
END_TEST

