#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include "config.h"
#include "filter.h"
#include "filter_manager.h"

START_TEST(test_mgr_init_destroy)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_create_drop)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "foo1", NULL);
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "foo1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_create_double_drop)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "dub1", NULL);
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "dub1");
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "dub1");
    fail_unless(res == -1);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "bar1", NULL);
    fail_unless(res == 0);
    res = filtmgr_create_filter(mgr, "bar2", NULL);
    fail_unless(res == 0);

    bloom_filter_list_head *head;
    res = filtmgr_list_filters(mgr, NULL, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 2);

    int has_bar1 = 0;
    int has_bar2 = 0;

    bloom_filter_list *node = head->head;
    while (node) {
        if (strcmp(node->filter_name, "bar1") == 0)
            has_bar1 = 1;
        else if (strcmp(node->filter_name, "bar2") == 0)
            has_bar2 = 1;
        node = node->next;
    }
    fail_unless(has_bar1);
    fail_unless(has_bar2);

    res = filtmgr_drop_filter(mgr, "bar1");
    fail_unless(res == 0);
    res = filtmgr_drop_filter(mgr, "bar2");
    fail_unless(res == 0);

    filtmgr_cleanup_list(head);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list_prefix)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "bar1", NULL);
    fail_unless(res == 0);
    res = filtmgr_create_filter(mgr, "bar2", NULL);
    fail_unless(res == 0);
    res = filtmgr_create_filter(mgr, "zyx", NULL);
    fail_unless(res == 0);

    bloom_filter_list_head *head;
    res = filtmgr_list_filters(mgr, "bar", &head);
    fail_unless(res == 0);
    fail_unless(head->size == 2);

    int has_bar1 = 0;
    int has_bar2 = 0;

    bloom_filter_list *node = head->head;
    while (node) {
        if (strcmp(node->filter_name, "bar1") == 0)
            has_bar1 = 1;
        else if (strcmp(node->filter_name, "bar2") == 0)
            has_bar2 = 1;
        node = node->next;
    }
    fail_unless(has_bar1);
    fail_unless(has_bar2);

    res = filtmgr_drop_filter(mgr, "bar1");
    fail_unless(res == 0);
    res = filtmgr_drop_filter(mgr, "bar2");
    fail_unless(res == 0);
    res = filtmgr_drop_filter(mgr, "zyx");
    fail_unless(res == 0);

    filtmgr_cleanup_list(head);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list_no_filters)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    bloom_filter_list_head *head;
    res = filtmgr_list_filters(mgr, NULL, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 0);
    filtmgr_cleanup_list(head);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST


START_TEST(test_mgr_add_check_keys)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab1", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "zab1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    for (int i=0;i<3;i++) result[i] = 0;
    res = filtmgr_check_keys(mgr, "zab1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_drop_filter(mgr, "zab1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_check_no_keys)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab2", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    char result[] = {1, 1, 1};
    res = filtmgr_check_keys(mgr, "zab2", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(!result[0]);
    fail_unless(!result[1]);
    fail_unless(!result[2]);

    res = filtmgr_drop_filter(mgr, "zab2");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_add_check_no_filter)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "noop1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == -1);

    for (int i=0;i<3;i++) result[i] = 0;
    res = filtmgr_check_keys(mgr, "noop1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == -1);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Flush */
START_TEST(test_mgr_flush_no_filter)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_flush_filter(mgr, "noop1");
    fail_unless(res == -1);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_flush)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab3", NULL);
    fail_unless(res == 0);

    res = filtmgr_flush_filter(mgr, "zab3");
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "zab3");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Unmap */
START_TEST(test_mgr_unmap_no_filter)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_unmap_filter(mgr, "noop2");
    fail_unless(res == -1);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_unmap)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab4", NULL);
    fail_unless(res == 0);

    res = filtmgr_unmap_filter(mgr, "zab4");
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "zab4");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_unmap_add_keys)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab5", NULL);
    fail_unless(res == 0);

    res = filtmgr_unmap_filter(mgr, "zab5");
    fail_unless(res == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.zab5/config.ini", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.zab5/data.000.mmap", 0777) == 0);

    // Try to add keys now
    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "zab5", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_drop_filter(mgr, "zab5");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Clear command */
START_TEST(test_mgr_clear_no_filter)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_clear_filter(mgr, "noop2");
    fail_unless(res == -1);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_clear_not_proxied)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "dub1", NULL);
    fail_unless(res == 0);

    // Should be not proxied still
    res = filtmgr_clear_filter(mgr, "dub1");
    fail_unless(res == -2);

    res = filtmgr_drop_filter(mgr, "dub1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_clear)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "dub2", NULL);
    fail_unless(res == 0);

    res = filtmgr_unmap_filter(mgr, "dub2");
    fail_unless(res == 0);

    // Should be not proxied still
    res = filtmgr_clear_filter(mgr, "dub2");
    fail_unless(res == 0);

    // Force a vacuum
    filtmgr_vacuum(mgr);

    res = filtmgr_create_filter(mgr, "dub2", NULL);
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "dub2");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_clear_reload)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab9", NULL);
    fail_unless(res == 0);

    // Try to add keys now
    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "zab9", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_unmap_filter(mgr, "zab9");
    fail_unless(res == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.zab9/config.ini", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.zab9/data.000.mmap", 0777) == 0);

    res = filtmgr_clear_filter(mgr, "zab9");
    fail_unless(res == 0);

    // Force a vacuum
    filtmgr_vacuum(mgr);

    // This should rediscover
    res = filtmgr_create_filter(mgr, "zab9", NULL);
    fail_unless(res == 0);

    // Try to check keys now
    res = filtmgr_check_keys(mgr, "zab9", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_drop_filter(mgr, "zab9");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* List Cold */
START_TEST(test_mgr_list_cold_no_filters)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    bloom_filter_list_head *head;
    res = filtmgr_list_cold_filters(mgr, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 0);
    filtmgr_cleanup_list(head);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_mgr_list_cold)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab6", NULL);
    fail_unless(res == 0);
    res = filtmgr_create_filter(mgr, "zab7", NULL);
    fail_unless(res == 0);

    // Force a vacuum so that list_cold_filters sees them
    filtmgr_vacuum(mgr);

    bloom_filter_list_head *head;
    res = filtmgr_list_cold_filters(mgr, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 0);

    // Check the keys in one, so that it stays hot
    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "zab6", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    // Check cold again
    res = filtmgr_list_cold_filters(mgr, &head);
    fail_unless(res == 0);
    fail_unless(head->size == 1);

    int has_zab6 = 0;
    int has_zab7 = 0;

    bloom_filter_list *node = head->head;
    while (node) {
        if (strcmp(node->filter_name, "zab6") == 0)
            has_zab6 = 1;
        else if (strcmp(node->filter_name, "zab7") == 0)
            has_zab7 = 1;
        node = node->next;
    }
    fail_unless(!has_zab6);
    fail_unless(has_zab7);

    res = filtmgr_drop_filter(mgr, "zab6");
    fail_unless(res == 0);
    res = filtmgr_drop_filter(mgr, "zab7");
    fail_unless(res == 0);
    filtmgr_cleanup_list(head);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Unmap in memory */
START_TEST(test_mgr_unmap_in_mem)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.in_memory = 1;

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "mem1", NULL);
    fail_unless(res == 0);

    // Try to add keys now
    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "mem1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_unmap_filter(mgr, "mem1");
    fail_unless(res == 0);

    // Try to add keys now
    for (int i=0;i<3;i++) result[i] = 0;
    res = filtmgr_check_keys(mgr, "mem1", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_drop_filter(mgr, "mem1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Custom config */
START_TEST(test_mgr_create_custom_config)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    // Custom config
    bloom_config *custom = malloc(sizeof(bloom_config));
    memcpy(custom, &config, sizeof(bloom_config));
    custom->in_memory = 1;

    res = filtmgr_create_filter(mgr, "custom1", custom);
    fail_unless(res == 0);

    res = filtmgr_drop_filter(mgr, "custom1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Scale up */
START_TEST(test_mgr_grow)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.in_memory = 1;
    config.initial_capacity = 10000;

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "scale1", NULL);
    fail_unless(res == 0);

    // Try to add keys now
    char *keys[10];
    char result[10];
    for (int iter=0;iter<10000;iter++) {
        // Generate the keys
        for (int i=0;i<10;i++) asprintf(&keys[i], "test_key_%d", i*iter);
        res = filtmgr_set_keys(mgr, "scale1", (char**)&keys, 10, (char*)&result);
        fail_unless(res == 0);
        for (int i=0;i<10;i++) free(keys[i]);
    }

    res = filtmgr_drop_filter(mgr, "scale1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

/* Close & Restore */

START_TEST(test_mgr_restore)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "zab8", NULL);
    fail_unless(res == 0);

    char *keys[] = {"hey","there","person"};
    char result[] = {0, 0, 0};
    res = filtmgr_set_keys(mgr, "zab8", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    // Shutdown
    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);

    // FUCKING annoying umask permissions bullshit
    // Cused by the Check test framework
    fail_unless(chmod("/tmp/bloomd/bloomd.zab8/config.ini", 0777) == 0);
    fail_unless(chmod("/tmp/bloomd/bloomd.zab8/data.000.mmap", 0777) == 0);

    // Restrore
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    for (int i=0;i<3;i++) result[i] = 0;
    res = filtmgr_check_keys(mgr, "zab8", (char**)&keys, 3, (char*)&result);
    fail_unless(res == 0);
    fail_unless(result[0]);
    fail_unless(result[1]);
    fail_unless(result[2]);

    res = filtmgr_drop_filter(mgr, "zab8");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

void test_mgr_cb(void *data, char *filter_name, bloom_filter* filter) {
    (void)filter_name;
    (void)filter;
    int *out = data;
    *out = 1;
}

START_TEST(test_mgr_callback)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    config.in_memory = 1;
    config.initial_capacity = 10000;

    bloom_filtmgr *mgr;
    res = init_filter_manager(&config, 0, &mgr);
    fail_unless(res == 0);

    res = filtmgr_create_filter(mgr, "cb1", NULL);
    fail_unless(res == 0);

    int val = 0;
    res = filtmgr_filter_cb(mgr, "cb1", test_mgr_cb, &val);
    fail_unless(val == 1);

    res = filtmgr_drop_filter(mgr, "cb1");
    fail_unless(res == 0);

    res = destroy_filter_manager(mgr);
    fail_unless(res == 0);
}
END_TEST

