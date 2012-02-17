#include <check.h>
#include <stdio.h>
#include "test_config.c"

int main(void)
{
    Suite *s1 = suite_create("Bloomd");
    TCase *tc1 = tcase_create("config");
    SRunner *sr = srunner_create(s1);
    int nf;

    // Add the bitmap tests
    suite_add_tcase(s1, tc1);
    tcase_add_test(tc1, test_config_get_default);
    tcase_add_test(tc1, test_config_bad_file);

    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

