#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "bloom.h"

START_TEST(bloom_filter_header_size)
{
    // Check our assumptions
    fail_unless(sizeof(bloom_filter_header) == 512);
}
END_TEST


START_TEST(make_bf_no_map)
{
    // Use -1 for anonymous
    bloom_bloomfilter filter;
    int res = bf_from_bitmap(NULL, 10, 1, &filter);
    fail_unless(res == -EINVAL);
}
END_TEST

START_TEST(make_bf_zero_k)
{
    // Use -1 for anonymous
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    int res = bf_from_bitmap(&map, 0, 1, &filter);
    fail_unless(res == -EINVAL);
}
END_TEST

START_TEST(make_bf_fresh_not_new)
{
    // Use -1 for anonymous
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    int res = bf_from_bitmap(&map, 10, 0, &filter);
    fail_unless(res == -1);
}
END_TEST

START_TEST(make_bf_fresh_then_restore)
{
    // Use -1 for anonymous
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    int res = bf_from_bitmap(&map, 10, 1, &filter); // Make fresh
    fail_unless(res == 0);

    bloom_bloomfilter filter2;
    res = bf_from_bitmap(&map, 10, 0, &filter2); // Restore now
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_bf_value_sanity)
{
    // Use -1 for anonymous
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, 4096, ANONYMOUS, &map);
    int res = bf_from_bitmap(&map, 10, 1, &filter); // Make fresh
    fail_unless(res == 0);

    fail_unless(filter.bitmap_size == 28672);  // Should be the bitmap size
    fail_unless(filter.offset == 2867);        // Should be size / 10
    fail_unless(filter.header->k_num == 10);
    fail_unless(filter.header->count == 0);
}
END_TEST

START_TEST(test_size_for_capacity_prob)
{
    bloom_filter_params params;
    params.capacity = 1e6;
    params.fp_probability = 1e-4;
    int res = bf_size_for_capacity_prob(&params);
    fail_unless(res == 0);
    fail_unless(params.bytes == 2396265);
}
END_TEST

START_TEST(test_fp_prob_for_capacity_size)
{
    bloom_filter_params params;
    params.capacity = 1e6;
    params.bytes = 2396265;
    int res = bf_fp_probability_for_capacity_size(&params);
    fail_unless(res == 0);
    fail_unless(params.fp_probability < 0.00010001);
    fail_unless(params.fp_probability > 0.00009999);
}
END_TEST

START_TEST(test_capacity_for_size_prob)
{
    bloom_filter_params params;
    params.bytes = 2396265;
    params.fp_probability = 1e-4;
    int res = bf_capacity_for_size_prob(&params);
    fail_unless(res == 0);
    fail_unless(params.capacity == 1e6);
}
END_TEST

START_TEST(test_ideal_k_num)
{
    bloom_filter_params params;
    params.bytes = 2396265;
    params.capacity = 1e6;
    int res = bf_ideal_k_num(&params);
    fail_unless(res == 0);
    fail_unless(params.k_num == 13);
}
END_TEST

START_TEST(test_params_for_capacity)
{
    bloom_filter_params params;
    params.capacity = 1e6;
    params.fp_probability = 1e-4;
    int res = bf_params_for_capacity(&params);
    fail_unless(res == 0);
    fail_unless(params.k_num == 13);
    fail_unless(params.bytes == 2396265 + 512);
}
END_TEST

START_TEST(test_hashes_basic)
{
    uint32_t k_num = 1000;
    char *key = "the quick brown fox";
    uint64_t hashes[1000];
    bf_compute_hashes(k_num, key, (uint64_t*)&hashes);

    // Check that all the hashes are unique.
    // This is O(n^2) but fuck it.
    for (int i=0;i<1000;i++) {
        for (int j=i+1;j<1000;j++) {
            fail_unless(hashes[i] != hashes[j]);
        }
    }
}
END_TEST


START_TEST(test_hashes_one_byte)
{
    uint32_t k_num = 1000;
    char *key = "A";
    uint64_t hashes[1000];
    bf_compute_hashes(k_num, key, (uint64_t*)&hashes);

    // Check that all the hashes are unique.
    // This is O(n^2) but fuck it.
    for (int i=0;i<1000;i++) {
        for (int j=i+1;j<1000;j++) {
            fail_unless(hashes[i] != hashes[j]);
        }
    }
}
END_TEST

START_TEST(test_hashes_consistent)
{
    uint32_t k_num = 10;
    char *key = "cat";
    char *key2= "abcdefghijklmnopqrstuvwxyz";
    uint64_t hashes[10];
    uint64_t hashes_cpy[10];

    bf_compute_hashes(k_num, key, (uint64_t*)&hashes);

    // Copy the hashes
    for (int i=0; i< 10; i++) {
        hashes_cpy[i] = hashes[i];
    }

    // Compute something else, then re-hash the first key
    bf_compute_hashes(k_num, key2, (uint64_t*)&hashes);
    bf_compute_hashes(k_num, key, (uint64_t*)&hashes);

    // Check for equality
    for (int i=0; i< 10; i++) {
        fail_unless(hashes_cpy[i] == hashes[i]);
    }
}
END_TEST

START_TEST(test_hashes_key_length)
{
    uint32_t k_num = 10;
    char *key = "cat\0A123456890";
    char *key1 = "cat\0ABCDEFGHI";
    uint64_t hashes[10];
    uint64_t hashes_cpy[10];

    bf_compute_hashes(k_num, key, (uint64_t*)&hashes);

    // Copy the hashes
    for (int i=0; i< 10; i++) {
        hashes_cpy[i] = hashes[i];
    }

    // Compute of second variant
    bf_compute_hashes(k_num, key1, (uint64_t*)&hashes);

    // Check for equality
    for (int i=0; i< 10; i++) {
        fail_unless(hashes_cpy[i] == hashes[i]);
    }
}
END_TEST

START_TEST(test_hashes_same_buffer)
{
    uint32_t k_num = 10;
    uint64_t hashes[10];

    char buf[100];

    uint64_t hash0 = 0;
    snprintf((char*)&buf, 100, "test0");
    bf_compute_hashes(k_num, (char*)&buf, (uint64_t*)&hashes);
    for (int i=0; i< 10; i++) {
        hash0 ^= hashes[i];
    }

    uint64_t hash1 = 0;
    snprintf((char*)&buf, 100, "ABCDEFGHI");
    bf_compute_hashes(k_num, (char*)&buf, (uint64_t*)&hashes);
    for (int i=0; i< 10; i++) {
        hash1 ^= hashes[i];
    }

    uint64_t hash2 = 0;
    snprintf((char*)&buf, 100, "test0");
    bf_compute_hashes(k_num, (char*)&buf, (uint64_t*)&hashes);
    for (int i=0; i< 10; i++) {
        hash2 ^= hashes[i];
    }

    uint64_t hash3 = 0;
    snprintf((char*)&buf, 100, "ABCDEFGHI");
    bf_compute_hashes(k_num, (char*)&buf, (uint64_t*)&hashes);
    for (int i=0; i< 10; i++) {
        hash3 ^= hashes[i];
    }

    fail_unless(hash0 == hash2);
    fail_unless(hash1 == hash3);
}
END_TEST




START_TEST(test_add_with_check)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bitmap_from_file(-1, params.bytes, ANONYMOUS, &map);
    bloom_bloomfilter filter;
    bf_from_bitmap(&map, params.k_num, 1, &filter);

    char buf[100];
    int res;

    // Check all the keys get added
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        fail_unless(res == 1);
    }

    // Check the size
    fail_unless(bf_size(&filter) == 1000);

    // Test all the keys are contained
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_contains(&filter, (char*)&buf);
        fail_unless(res == 1);
    }

    // Check all the keys are not re-added
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        fail_unless(res == 0);
    }
}
END_TEST

START_TEST(test_length)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bitmap_from_file(-1, params.bytes, ANONYMOUS, &map);
    bloom_bloomfilter filter;
    bf_from_bitmap(&map, params.k_num, 1, &filter);

    // Check the size
    fail_unless(bf_size(&filter) == 0);

    // Check all the keys get added
    char buf[100];
    int res;
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        fail_unless(res == 1);
    }

    // Check the size
    fail_unless(bf_size(&filter) == 1000);
}
END_TEST


START_TEST(test_bf_double_close)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, params.bytes, ANONYMOUS, &map);
    bf_from_bitmap(&map, params.k_num, 1, &filter);

    fail_unless(bf_close(&filter) == 0);
    fail_unless(bf_close(&filter) == -1);
}
END_TEST

START_TEST(test_flush_close)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_filename("/tmp/test_flush_close.mmap", params.bytes, 1, SHARED, &map);
    bf_from_bitmap(&map, params.k_num, 1, &filter);

    fail_unless(bf_flush(&filter) == 0);
    fail_unless(bf_close(&filter) == 0);

    unlink("/tmp/test_flush_close.mmap");
}
END_TEST

START_TEST(test_bf_flush)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    fail_unless(bitmap_from_filename("/tmp/test_flush.mmap", params.bytes, 1, SHARED, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);
    fchmod(map.fileno, 0777);

    // Check all the keys get added
    char buf[100];
    int res;
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        fail_unless(res == 1);
    }
    fail_unless(bf_flush(&filter) == 0);

    bloom_bitmap map2;
    bloom_bloomfilter filter2;
    fail_unless(bitmap_from_filename("/tmp/test_flush.mmap", params.bytes, 1, SHARED, &map2) == 0);
    fail_unless(bf_from_bitmap(&map2, params.k_num, 1, &filter2) == 0);

    // Test all the keys are contained
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_contains(&filter2, (char*)&buf);
        fail_unless(res == 1);
    }

    unlink("/tmp/test_flush.mmap");
}
END_TEST

START_TEST(test_bf_close_does_flush)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    fail_unless(bitmap_from_filename("/tmp/test_close_does_flush.mmap", params.bytes, 1, SHARED, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);
    fchmod(map.fileno, 0777);

    // Check all the keys get added
    char buf[100];
    int res;
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        fail_unless(res == 1);
    }
    fail_unless(bf_close(&filter) == 0);

    // Test all the keys are contained
    fail_unless(bitmap_from_filename("/tmp/test_close_does_flush.mmap", params.bytes, 1, SHARED, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_contains(&filter, (char*)&buf);
        fail_unless(res == 1);
    }
    unlink("/tmp/test_close_does_flush.mmap");
}
END_TEST

START_TEST(test_bf_fp_prob)
{
    bloom_filter_params params = {0, 0, 1000, 0.01};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    fail_unless(bitmap_from_file(-1, params.bytes, ANONYMOUS, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);

    // Check all the keys get added
    char buf[100];
    int res;
    int num_wrong = 0;
    for (int i=0;i<1100;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        if (res == 0) num_wrong++;
    }

    // We added 1100 items, with a capacity of 1K and error of 1/100.
    // Technically we should have 11 false positives
    fail_unless(num_wrong <= 10);
}
END_TEST

START_TEST(test_bf_fp_prob_extended)
{
    bloom_filter_params params = {0, 0, 1e6, 0.001};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    fail_unless(bitmap_from_file(-1, params.bytes, ANONYMOUS, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);

    // Check all the keys get added
    char buf[100];
    int res;
    int num_wrong = 0;
    for (int i=0;i<1e6;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        if (res == 0) num_wrong++;
    }

    // We added 1M items, with a capacity of 1M and error of 1/1000.
    // Technically we should have 1K false positives
    fail_unless(num_wrong <= 1000);
}
END_TEST

START_TEST(test_bf_shared_compatible_persist)
{
    bloom_filter_params params = {0, 0, 1e6, 1e-4};
    bf_params_for_capacity(&params);
    bloom_bitmap map;
    bloom_bloomfilter filter;
    fail_unless(bitmap_from_filename("/tmp/shared_compat_persist.mmap", params.bytes, 1, PERSISTENT, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);
    fchmod(map.fileno, 0777);

    // Check all the keys get added
    char buf[100];
    int res;
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_add(&filter, (char*)&buf);
        fail_unless(res == 1);
    }
    fail_unless(bf_close(&filter) == 0);

    // Test all the keys are contained
    fail_unless(bitmap_from_filename("/tmp/shared_compat_persist.mmap", params.bytes, 1, SHARED, &map) == 0);
    fail_unless(bf_from_bitmap(&map, params.k_num, 1, &filter) == 0);
    for (int i=0;i<1000;i++) {
        snprintf((char*)&buf, 100, "test%d", i);
        res = bf_contains(&filter, (char*)&buf);
        fail_unless(res == 1);
    }
    unlink("/tmp/shared_compat_persist.mmap");
}
END_TEST

