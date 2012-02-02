#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "../src/bloom.h"

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
    bitmap_from_file(-1, 4096, &map);
    int res = bf_from_bitmap(&map, 0, 1, &filter);
    fail_unless(res == -EINVAL);
}
END_TEST

START_TEST(make_bf_fresh_not_new)
{
    // Use -1 for anonymous
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, 4096, &map);
    int res = bf_from_bitmap(&map, 10, 0, &filter);
    fail_unless(res == -EFTYPE);
}
END_TEST

START_TEST(make_bf_fresh_then_restore)
{
    // Use -1 for anonymous
    bloom_bitmap map;
    bloom_bloomfilter filter;
    bitmap_from_file(-1, 4096, &map);
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
    bitmap_from_file(-1, 4096, &map);
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

