#include <check.h>
#include <stdio.h>

int main(void)
{
    Suite *s1 = suite_create("Bloomd");
    TCase *tc1 = tcase_create("config");
    SRunner *sr = srunner_create(s1);
    int nf;

    // Add the bitmap tests
    suite_add_tcase(s1, tc1);
    //tcase_add_test(tc1, make_anonymous_bitmap);

    srunner_run_all(sr, CK_ENV);
    nf = srunner_ntests_failed(sr);
    srunner_free(sr);

    return nf == 0 ? 0 : 1;
}

