#include <check.h>
#include <stdio.h>
#include "test_config.c"
#include "test_hashmap.c"

int main(void)
{
    Suite *s1 = suite_create("Bloomd");
    TCase *tc1 = tcase_create("config");
    TCase *tc2 = tcase_create("hashmap");
    SRunner *sr = srunner_create(s1);
    int nf;

    // Add the bitmap tests
    suite_add_tcase(s1, tc1);
    tcase_add_test(tc1, test_config_get_default);
    tcase_add_test(tc1, test_config_bad_file);
    tcase_add_test(tc1, test_config_empty_file);
    tcase_add_test(tc1, test_config_basic_config);
    tcase_add_test(tc1, test_validate_default_config);
    tcase_add_test(tc1, test_validate_bad_config);
    tcase_add_test(tc1, test_join_path_no_slash);
    tcase_add_test(tc1, test_join_path_with_slash);
    tcase_add_test(tc1, test_sane_log_level);
    tcase_add_test(tc1, test_sane_init_capacity);
    tcase_add_test(tc1, test_sane_default_probability);
    tcase_add_test(tc1, test_sane_scale_size);
    tcase_add_test(tc1, test_sane_probability_reduction);
    tcase_add_test(tc1, test_sane_flush_interval);
    tcase_add_test(tc1, test_sane_cold_interval);
    tcase_add_test(tc1, test_sane_in_memory);
    tcase_add_test(tc1, test_sane_worker_threads);

    // Add the hashmap tests
    suite_add_tcase(s1, tc2);
    tcase_add_test(tc2, test_map_init_and_destroy);
    tcase_add_test(tc2, test_map_get_no_keys);
    tcase_add_test(tc2, test_map_put);
    tcase_add_test(tc2, test_map_put_get);
    tcase_add_test(tc2, test_map_delete_no_keys);
    tcase_add_test(tc2, test_map_put_delete);
    tcase_add_test(tc2, test_map_put_delete_get);
    tcase_add_test(tc2, test_map_iter_no_keys);
    tcase_add_test(tc2, test_map_put_iter_break);
    tcase_add_test(tc2, test_map_put_grow);

    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

