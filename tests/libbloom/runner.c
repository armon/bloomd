#include <check.h>
#include <stdio.h>
#include "bitmap.h"
#include "test_bitmap.c"
#include "test_bloom.c"
#include "test_sbf.c"

int main(void)
{
    Suite *s1 = suite_create("Blooming");
    TCase *tc1 = tcase_create("Bitmap");
    TCase *tc2 = tcase_create("Bloom");
    TCase *tc3 = tcase_create("SBF");
    SRunner *sr = srunner_create(s1);
    int nf;

    // Add the bitmap tests
    suite_add_tcase(s1, tc1);
    tcase_add_test(tc1, make_anonymous_bitmap);
    tcase_add_test(tc1, make_bitmap_zero_size);
    tcase_add_test(tc1, make_bitmap_bad_fileno);
    tcase_add_test(tc1, make_bitmap_bad_fileno_persistent);
    tcase_add_test(tc1, make_bitmap_nofile);
    tcase_add_test(tc1, make_bitmap_nofile_persistent);
    tcase_add_test(tc1, make_bitmap_nofile_create);
    tcase_add_test(tc1, make_bitmap_nofile_create_persistent);

    tcase_add_test(tc1, flush_bitmap_anonymous);
    tcase_add_test(tc1, flush_bitmap_file);
    tcase_add_test(tc1, flush_bitmap_file_persistent);
    tcase_add_test(tc1, flush_bitmap_null);

    tcase_add_test(tc1, close_bitmap_anonymous);
    tcase_add_test(tc1, close_bitmap_file);
    tcase_add_test(tc1, close_bitmap_file_persistent);
    tcase_add_test(tc1, double_close_bitmap_file);
    tcase_add_test(tc1, double_close_bitmap_file_persist);
    tcase_add_test(tc1, close_bitmap_null);

    tcase_add_test(tc1, getbit_bitmap_anonymous_zero);
    tcase_add_test(tc1, getbit_bitmap_anonymous_one);
    tcase_add_test(tc1, getbit_bitmap_file_zero);
    tcase_add_test(tc1, getbit_bitmap_file_one);
    tcase_add_test(tc1, getbit_bitmap_file_persist_zero);
    tcase_add_test(tc1, getbit_bitmap_file_persist_one);
    tcase_add_test(tc1, getbit_bitmap_anonymous_one_onebyte);

    tcase_add_test(tc1, setbit_bitmap_anonymous_one_byte);
    tcase_add_test(tc1, setbit_bitmap_anonymous_one_byte_aligned);
    tcase_add_test(tc1, setbit_bitmap_anonymous_one);
    tcase_add_test(tc1, setbit_bitmap_file_one);
    tcase_add_test(tc1, setbit_bitmap_file_persist_one);

    tcase_add_test(tc1, flush_does_write);
    tcase_add_test(tc1, close_does_flush);
    tcase_add_test(tc1, flush_does_write_persist);
    tcase_add_test(tc1, close_does_flush_persist);

    // Add the bloom tests
    suite_add_tcase(s1, tc2);
    tcase_add_test(tc2, make_bf_no_map);
    tcase_add_test(tc2, make_bf_fresh_not_new);
    tcase_add_test(tc2, make_bf_zero_k);
    tcase_add_test(tc2, bloom_filter_header_size);
    tcase_add_test(tc2, make_bf_fresh_then_restore);
    tcase_add_test(tc2, test_bf_value_sanity);

    tcase_add_test(tc2, test_size_for_capacity_prob);
    tcase_add_test(tc2, test_fp_prob_for_capacity_size);
    tcase_add_test(tc2, test_capacity_for_size_prob);
    tcase_add_test(tc2, test_ideal_k_num);
    tcase_add_test(tc2, test_params_for_capacity);

    tcase_add_test(tc2, test_hashes_basic);
    tcase_add_test(tc2, test_hashes_one_byte);
    tcase_add_test(tc2, test_hashes_consistent);
    tcase_add_test(tc2, test_hashes_key_length);
    tcase_add_test(tc2, test_hashes_same_buffer);

    tcase_add_test(tc2, test_add_with_check);
    tcase_add_test(tc2, test_length);

    tcase_add_test(tc2, test_bf_double_close);
    tcase_add_test(tc2, test_flush_close);
    tcase_add_test(tc2, test_bf_flush);
    tcase_add_test(tc2, test_bf_close_does_flush);

    tcase_add_test(tc2, test_bf_fp_prob);
    tcase_add_test(tc2, test_bf_fp_prob_extended);

    tcase_add_test(tc2, test_bf_shared_compatible_persist);

    // Add the sbf tests
    suite_add_tcase(s1, tc3);
    tcase_add_test(tc3, sbf_initial_size);
    tcase_add_test(tc3, sbf_add_filter);
    tcase_add_test(tc3, sbf_add_filter_2);
    tcase_add_test(tc3, sbf_callback);
    tcase_add_test(tc3, test_sbf_double_close);
    tcase_add_test(tc3, test_sbf_flush_close);
    tcase_add_test(tc3, test_sbf_flush);
    tcase_add_test(tc3, test_sbf_close_does_flush);
    tcase_add_test(tc3, sbf_fp_prob);

    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

