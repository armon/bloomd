#include <check.h>
#include <stdio.h>
#include "test_bitmap.c"
#include "test_bloom.c"

int main(void)
{
    Suite *s1 = suite_create("Blooming");
    TCase *tc1 = tcase_create("Bitmap");
    TCase *tc2 = tcase_create("Bloom");
    SRunner *sr = srunner_create(s1);
    int nf;

    // Add the bitmap tests
    suite_add_tcase(s1, tc1);
    tcase_add_test(tc1, make_anonymous_bitmap);
    tcase_add_test(tc1, make_bitmap_zero_size);
    tcase_add_test(tc1, make_bitmap_bad_fileno);
    tcase_add_test(tc1, make_bitmap_nofile);
    tcase_add_test(tc1, make_bitmap_nofile_create);
    tcase_add_test(tc1, make_bitmap_resize);

    tcase_add_test(tc1, flush_bitmap_anonymous);
    tcase_add_test(tc1, flush_bitmap_file);
    tcase_add_test(tc1, flush_bitmap_null);

    tcase_add_test(tc1, close_bitmap_anonymous);
    tcase_add_test(tc1, close_bitmap_file);
    tcase_add_test(tc1, close_bitmap_null);

    tcase_add_test(tc1, getbit_bitmap_anonymous_zero);
    tcase_add_test(tc1, getbit_bitmap_anonymous_one);
    tcase_add_test(tc1, getbit_bitmap_file_zero);
    tcase_add_test(tc1, getbit_bitmap_file_one);
    tcase_add_test(tc1, getbit_bitmap_anonymous_one_onebyte);

    tcase_add_test(tc1, setbit_bitmap_anonymous_one_byte);
    tcase_add_test(tc1, setbit_bitmap_anonymous_one_byte_aligned);
    tcase_add_test(tc1, setbit_bitmap_anonymous_one);
    tcase_add_test(tc1, setbit_bitmap_file_one);

    tcase_add_test(tc1, flush_does_write);
    tcase_add_test(tc1, close_does_flush);

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

    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

