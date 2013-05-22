#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>

#include <check.h>

#include "art.h"

START_TEST(test_art_init_and_destroy)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    fail_unless(art_size(&t) == 0);

    res = destroy_art_tree(&t);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_art_insert)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen("tests/words.txt", "r");

    uintptr_t line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        fail_unless(NULL == art_insert(&t, buf, len, (void*)line));
        fail_unless(art_size(&t) == line);
        line++;
    }

    res = destroy_art_tree(&t);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_art_insert_search)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen("tests/words.txt", "r");

    uintptr_t line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        fail_unless(NULL ==
            art_insert(&t, buf, len, (void*)line));
        line++;
    }

    // Seek back to the start
    fseek(f, 0, SEEK_SET);

    // Search for each line
    line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';

        uintptr_t val = (uintptr_t)art_search(&t, buf, len);
	fail_unless(line == val, "Line: %d Val: %" PRIuPTR " Str: %s\n", line,
	    val, buf);
        line++;
    }

    // Check the minimum
    art_leaf *l = art_minimum(&t);
    fail_unless(l && strcmp((char*)l->key, "A") == 0);

    // Check the maximum
    l = art_maximum(&t);
    fail_unless(l && strcmp((char*)l->key, "zythum") == 0);

    res = destroy_art_tree(&t);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_art_insert_delete)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen("tests/words.txt", "r");

    uintptr_t line = 1, nlines;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        fail_unless(NULL ==
            art_insert(&t, buf, len, (void*)line));
        line++;
    }

    nlines = line - 1;

    // Seek back to the start
    fseek(f, 0, SEEK_SET);

    // Search for each line
    line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';

        // Search first, ensure all entries still
        // visible
        uintptr_t val = (uintptr_t)art_search(&t, buf, len);
	fail_unless(line == val, "Line: %d Val: %" PRIuPTR " Str: %s\n", line,
	    val, buf);

        // Delete, should get lineno back
        val = (uintptr_t)art_delete(&t, buf, len);
	fail_unless(line == val, "Line: %d Val: %" PRIuPTR " Str: %s\n", line,
	    val, buf);

        // Check the size
        fail_unless(art_size(&t) == nlines - line);
        line++;
    }

    // Check the minimum and maximum
    fail_unless(!art_minimum(&t));
    fail_unless(!art_maximum(&t));

    res = destroy_art_tree(&t);
    fail_unless(res == 0);
}
END_TEST

int iter_cb(void *data, const char* key, uint32_t key_len, void *val) {
    uint64_t *out = data;
    uintptr_t line = (uintptr_t)val;
    uint64_t mask = (line * (key[0] + key_len));
    out[0]++;
    out[1] ^= mask;
    return 0;
}

START_TEST(test_art_insert_iter)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen("tests/words.txt", "r");

    uint64_t xor_mask = 0;
    uintptr_t line = 1, nlines;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        fail_unless(NULL ==
            art_insert(&t, buf, len, (void*)line));

        xor_mask ^= (line * (buf[0] + len));
        line++;
    }
    nlines = line - 1;

    uint64_t out[] = {0, 0};
    fail_unless(art_iter(&t, iter_cb, &out) == 0);

    fail_unless(out[0] == nlines);
    fail_unless(out[1] == xor_mask);

    res = destroy_art_tree(&t);
    fail_unless(res == 0);
}
END_TEST

typedef struct {
    int count;
    int max_count;
    char **expected;
} prefix_data;

static int test_prefix_cb(void *data, const char *k, uint32_t k_len, void *val) {
    (void)val;
    prefix_data *p = data;
    fail_unless(p->count < p->max_count);
    fail_unless(memcmp(k, p->expected[p->count], k_len) == 0);
    p->count++;
    return 0;
}

START_TEST(test_art_iter_prefix)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    char *s = "api.foo.bar";
    fail_unless(NULL == art_insert(&t, s, strlen(s)+1, NULL));

    s = "api.foo.baz";
    fail_unless(NULL == art_insert(&t, s, strlen(s)+1, NULL));

    s = "api.foe.fum";
    fail_unless(NULL == art_insert(&t, s, strlen(s)+1, NULL));

    s = "abc.123.456";
    fail_unless(NULL == art_insert(&t, s, strlen(s)+1, NULL));

    s = "api.foo";
    fail_unless(NULL == art_insert(&t, s, strlen(s)+1, NULL));

    s = "api";
    fail_unless(NULL == art_insert(&t, s, strlen(s)+1, NULL));

    // Iterate over api
    char *expected[] = {"api", "api.foe.fum", "api.foo", "api.foo.bar", "api.foo.baz"};
    prefix_data p = { 0, 5, expected };
    fail_unless(!art_iter_prefix(&t, "api", 3, test_prefix_cb, &p));
    fail_unless(p.count == p.max_count, "Count: %d Max: %d", p.count, p.max_count);

    // Iterate over 'a'
    char *expected2[] = {"abc.123.456", "api", "api.foe.fum", "api.foo", "api.foo.bar", "api.foo.baz"};
    prefix_data p2 = { 0, 6, expected2 };
    fail_unless(!art_iter_prefix(&t, "a", 1, test_prefix_cb, &p2));
    fail_unless(p2.count == p2.max_count);

    // Check a failed iteration
    prefix_data p3 = { 0, 0, NULL };
    fail_unless(!art_iter_prefix(&t, "b", 1, test_prefix_cb, &p3));
    fail_unless(p3.count == 0);

    // Iterate over api.
    char *expected4[] = {"api.foe.fum", "api.foo", "api.foo.bar", "api.foo.baz"};
    prefix_data p4 = { 0, 4, expected4 };
    fail_unless(!art_iter_prefix(&t, "api.", 4, test_prefix_cb, &p4));
    fail_unless(p4.count == p4.max_count, "Count: %d Max: %d", p4.count, p4.max_count);

    // Iterate over api.foo.ba
    char *expected5[] = {"api.foo.bar"};
    prefix_data p5 = { 0, 1, expected5 };
    fail_unless(!art_iter_prefix(&t, "api.foo.bar", 11, test_prefix_cb, &p5));
    fail_unless(p5.count == p5.max_count, "Count: %d Max: %d", p5.count, p5.max_count);

    // Check a failed iteration on api.end
    prefix_data p6 = { 0, 0, NULL };
    fail_unless(!art_iter_prefix(&t, "api.end", 7, test_prefix_cb, &p6));
    fail_unless(p6.count == 0);

    // Iterate over empty prefix
    prefix_data p7 = { 0, 6, expected2 };
    fail_unless(!art_iter_prefix(&t, "", 0, test_prefix_cb, &p7));
    fail_unless(p7.count == p7.max_count);

    res = destroy_art_tree(&t);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_art_insert_copy_delete)
{
    art_tree t;
    int res = init_art_tree(&t);
    fail_unless(res == 0);

    int len;
    char buf[512];
    FILE *f = fopen("tests/words.txt", "r");

    uintptr_t line = 1, nlines;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';
        fail_unless(NULL ==
            art_insert(&t, buf, len, (void*)line));
        line++;
    }

    nlines = line - 1;

    // Create a new tree
    art_tree t2;
    fail_unless(art_copy(&t2, &t) == 0);

    // Destroy the original
    res = destroy_art_tree(&t);
    fail_unless(res == 0);

    // Seek back to the start
    fseek(f, 0, SEEK_SET);

    // Search for each line
    line = 1;
    while (fgets(buf, sizeof buf, f)) {
        len = strlen(buf);
        buf[len-1] = '\0';

        // Search first, ensure all entries still
        // visible
        uintptr_t val = (uintptr_t)art_search(&t2, buf, len);
        fail_unless(line == val, "Line: %d Val: %" PRIuPTR " Str: %s\n", line,
            val, buf);

        // Delete, should get lineno back
        val = (uintptr_t)art_delete(&t2, buf, len);
        fail_unless(line == val, "Line: %d Val: %" PRIuPTR " Str: %s\n", line,
            val, buf);

        // Check the size
        fail_unless(art_size(&t2) == nlines - line);
        line++;
    }

    res = destroy_art_tree(&t2);
    fail_unless(res == 0);
}
END_TEST

