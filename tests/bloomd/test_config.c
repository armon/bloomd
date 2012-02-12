/* let g:syntastic_c_include_dirs = [ 'src/bloomd/'] */
#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "config.h"

START_TEST(make_anonymous_bitmap)
{
    int res = 0;
    fail_unless(res == 0);
}
END_TEST


