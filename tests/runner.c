#include <check.h>
#include <stdio.h>
#include "test_bitmap.c"

int main(void)
{
    Suite *s1 = suite_create("Core");
    TCase *tc1_1 = tcase_create("Core");
    SRunner *sr = srunner_create(s1);
    int nf;

    suite_add_tcase(s1, tc1_1);
    tcase_add_test(tc1_1, make_anonymous_bitmap);
    tcase_add_test(tc1_1, make_bitmap_zero_size);
    tcase_add_test(tc1_1, make_bitmap_bad_fileno);
    tcase_add_test(tc1_1, make_bitmap_nofile);
    tcase_add_test(tc1_1, make_bitmap_nofile_create);
    tcase_add_test(tc1_1, make_bitmap_resize);

    tcase_add_test(tc1_1, bitmap_size_anonymous);
    tcase_add_test(tc1_1, bitmap_size_file);

    tcase_add_test(tc1_1, flush_bitmap_anonymous);
    tcase_add_test(tc1_1, flush_bitmap_file);
    tcase_add_test(tc1_1, flush_bitmap_null);

    tcase_add_test(tc1_1, close_bitmap_anonymous);
    tcase_add_test(tc1_1, close_bitmap_file);
    tcase_add_test(tc1_1, close_bitmap_null);

    tcase_add_test(tc1_1, getbit_bitmap_anonymous_zero);
    tcase_add_test(tc1_1, getbit_bitmap_anonymous_one);
    tcase_add_test(tc1_1, getbit_bitmap_file_zero);
    tcase_add_test(tc1_1, getbit_bitmap_file_one);
    tcase_add_test(tc1_1, getbit_bitmap_anonymous_one_onebyte);

    tcase_add_test(tc1_1, setbit_bitmap_anonymous_one_byte);
    tcase_add_test(tc1_1, setbit_bitmap_anonymous_one_byte_aligned);
    tcase_add_test(tc1_1, setbit_bitmap_anonymous_one);
    tcase_add_test(tc1_1, setbit_bitmap_file_one);

    tcase_add_test(tc1_1, flush_does_write);
    tcase_add_test(tc1_1, close_does_flush);


    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

