#include <check.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "config.h"

START_TEST(test_config_get_default)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);
    fail_unless(config.tcp_port == 8673);
    fail_unless(config.udp_port == 8674);
    fail_unless(strcmp(config.data_dir, "/tmp/bloomd") == 0);
    fail_unless(strcmp(config.log_level, "DEBUG") == 0);
    fail_unless(config.syslog_log_level == LOG_DEBUG);
    fail_unless(config.initial_capacity == 100000);
    fail_unless(config.default_probability == 1e-4);
    fail_unless(config.scale_size == 4);
    fail_unless(config.probability_reduction == 0.9);
    fail_unless(config.flush_interval == 60);
    fail_unless(config.cold_interval == 3600);
    fail_unless(config.in_memory == 0);
    fail_unless(config.worker_threads == 1);
    fail_unless(config.use_mmap == 0);
}
END_TEST

START_TEST(test_config_bad_file)
{
    bloom_config config;
    int res = config_from_filename("/tmp/does_not_exist", &config);
    fail_unless(res == -ENOENT);

    // Should get the defaults...
    fail_unless(config.tcp_port == 8673);
    fail_unless(config.udp_port == 8674);
    fail_unless(strcmp(config.data_dir, "/tmp/bloomd") == 0);
    fail_unless(strcmp(config.log_level, "DEBUG") == 0);
    fail_unless(config.syslog_log_level == LOG_DEBUG);
    fail_unless(config.initial_capacity == 100000);
    fail_unless(config.default_probability == 1e-4);
    fail_unless(config.scale_size == 4);
    fail_unless(config.probability_reduction == 0.9);
    fail_unless(config.flush_interval == 60);
    fail_unless(config.cold_interval == 3600);
    fail_unless(config.in_memory == 0);
    fail_unless(config.worker_threads == 1);
    fail_unless(config.use_mmap == 0);
}
END_TEST

START_TEST(test_config_empty_file)
{
    int fh = open("/tmp/zero_file", O_CREAT|O_RDWR, 0777);
    fchmod(fh, 777);
    close(fh);

    bloom_config config;
    int res = config_from_filename("/tmp/zero_file", &config);
    fail_unless(res == 0);

    // Should get the defaults...
    fail_unless(config.tcp_port == 8673);
    fail_unless(config.udp_port == 8674);
    fail_unless(strcmp(config.data_dir, "/tmp/bloomd") == 0);
    fail_unless(strcmp(config.log_level, "DEBUG") == 0);
    fail_unless(config.syslog_log_level == LOG_DEBUG);
    fail_unless(config.initial_capacity == 100000);
    fail_unless(config.default_probability == 1e-4);
    fail_unless(config.scale_size == 4);
    fail_unless(config.probability_reduction == 0.9);
    fail_unless(config.flush_interval == 60);
    fail_unless(config.cold_interval == 3600);
    fail_unless(config.in_memory == 0);
    fail_unless(config.worker_threads == 1);
    fail_unless(config.use_mmap == 0);

    unlink("/tmp/zero_file");
}
END_TEST

START_TEST(test_config_basic_config)
{
    int fh = open("/tmp/basic_config", O_CREAT|O_RDWR, 0777);
    char *buf = "[bloomd]\n\
port = 10000\n\
udp_port = 10001\n\
scale_size = 2\n\
flush_interval = 120\n\
cold_interval = 12000\n\
in_memory = 1\n\
initial_capacity = 2000000\n\
default_probability = 0.005\n\
probability_reduction = 0.8\n\
data_dir = /tmp/test\n\
workers = 2\n\
use_mmap = 1\n\
log_level = INFO\n";
    write(fh, buf, strlen(buf));
    fchmod(fh, 777);
    close(fh);

    bloom_config config;
    int res = config_from_filename("/tmp/basic_config", &config);
    fail_unless(res == 0);

    // Should get the config
    fail_unless(config.tcp_port == 10000);
    fail_unless(config.udp_port == 10001);
    fail_unless(strcmp(config.data_dir, "/tmp/test") == 0);
    fail_unless(strcmp(config.log_level, "INFO") == 0);
    fail_unless(config.initial_capacity == 2000000);
    fail_unless(config.default_probability == 0.005);
    fail_unless(config.scale_size == 2);
    fail_unless(config.probability_reduction == 0.8);
    fail_unless(config.flush_interval == 120);
    fail_unless(config.cold_interval == 12000);
    fail_unless(config.in_memory == 1);
    fail_unless(config.worker_threads == 2);
    fail_unless(config.use_mmap == 1);

    unlink("/tmp/basic_config");
}
END_TEST

START_TEST(test_validate_default_config)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    res = validate_config(&config);
    fail_unless(res == 0);
}
END_TEST

START_TEST(test_validate_bad_config)
{
    bloom_config config;
    int res = config_from_filename(NULL, &config);
    fail_unless(res == 0);

    // Set an absurd probability, should fail
    config.default_probability = 1.0;

    res = validate_config(&config);
    fail_unless(res == 1);
}
END_TEST

START_TEST(test_join_path_no_slash)
{
    char *s1 = "/tmp/path";
    char *s2 = "file";
    char *s3 = join_path(s1, s2);
    fail_unless(strcmp(s3, "/tmp/path/file") == 0);
}
END_TEST

START_TEST(test_join_path_with_slash)
{
    char *s1 = "/tmp/path/";
    char *s2 = "file";
    char *s3 = join_path(s1, s2);
    fail_unless(strcmp(s3, "/tmp/path/file") == 0);
}
END_TEST

START_TEST(test_sane_log_level)
{
    int log_lvl;
    fail_unless(sane_log_level("DEBUG", &log_lvl) == 0);
    fail_unless(sane_log_level("debug", &log_lvl) == 0);
    fail_unless(sane_log_level("INFO", &log_lvl) == 0);
    fail_unless(sane_log_level("info", &log_lvl) == 0);
    fail_unless(sane_log_level("WARN", &log_lvl) == 0);
    fail_unless(sane_log_level("warn", &log_lvl) == 0);
    fail_unless(sane_log_level("ERROR", &log_lvl) == 0);
    fail_unless(sane_log_level("error", &log_lvl) == 0);
    fail_unless(sane_log_level("CRITICAL", &log_lvl) == 0);
    fail_unless(sane_log_level("critical", &log_lvl) == 0);
    fail_unless(sane_log_level("foo", &log_lvl) == 1);
    fail_unless(sane_log_level("BAR", &log_lvl) == 1);
}
END_TEST

START_TEST(test_sane_init_capacity)
{
    fail_unless(sane_initial_capacity(10000) == 1);  // 10K
    fail_unless(sane_initial_capacity(100000) == 0);
    fail_unless(sane_initial_capacity(1000000) == 0); // 1MM
    fail_unless(sane_initial_capacity(10000000) == 0);
    fail_unless(sane_initial_capacity(100000000) == 0);
    fail_unless(sane_initial_capacity(1000000000) == 0); // 1B
}
END_TEST

START_TEST(test_sane_default_probability)
{
    fail_unless(sane_default_probability(1.0) == 1);
    fail_unless(sane_default_probability(0.5) == 1);
    fail_unless(sane_default_probability(0.1) == 1);
    fail_unless(sane_default_probability(0.05) == 0);
    fail_unless(sane_default_probability(0.01) == 0);
    fail_unless(sane_default_probability(0.001) == 0);
    fail_unless(sane_default_probability(0.0001) == 0);
    fail_unless(sane_default_probability(0.00001) == 0);
}
END_TEST

START_TEST(test_sane_scale_size)
{
    fail_unless(sane_scale_size(1) == 1);
    fail_unless(sane_scale_size(0) == 1);
    fail_unless(sane_scale_size(5) == 1);
    fail_unless(sane_scale_size(3) == 1);
    fail_unless(sane_scale_size(2) == 0);
    fail_unless(sane_scale_size(4) == 0);
}
END_TEST

START_TEST(test_sane_probability_reduction)
{
    fail_unless(sane_probability_reduction(1.0) == 1);
    fail_unless(sane_probability_reduction(0.9) == 0);
    fail_unless(sane_probability_reduction(0.8) == 0);
    fail_unless(sane_probability_reduction(0.5) == 0);
    fail_unless(sane_probability_reduction(0.1) == 1);
    fail_unless(sane_probability_reduction(0.05) == 1);
    fail_unless(sane_probability_reduction(0.01) == 1);
}
END_TEST

START_TEST(test_sane_flush_interval)
{
    fail_unless(sane_flush_interval(-1) == 1);
    fail_unless(sane_flush_interval(0) == 0);
    fail_unless(sane_flush_interval(60) == 0);
    fail_unless(sane_flush_interval(120) == 0);
    fail_unless(sane_flush_interval(86400) == 0);
}
END_TEST

START_TEST(test_sane_cold_interval)
{
    fail_unless(sane_cold_interval(-1) == 1);
    fail_unless(sane_cold_interval(0) == 0);
    fail_unless(sane_cold_interval(60) == 0);
    fail_unless(sane_cold_interval(120) == 0);
    fail_unless(sane_cold_interval(3600) == 0);
    fail_unless(sane_cold_interval(86400) == 0);
}
END_TEST

START_TEST(test_sane_in_memory)
{
    fail_unless(sane_in_memory(-1) == 1);
    fail_unless(sane_in_memory(0) == 0);
    fail_unless(sane_in_memory(1) == 0);
    fail_unless(sane_in_memory(2) == 1);
}
END_TEST

START_TEST(test_sane_use_mmap)
{
    fail_unless(sane_use_mmap(-1) == 1);
    fail_unless(sane_use_mmap(0) == 0);
    fail_unless(sane_use_mmap(1) == 0);
    fail_unless(sane_use_mmap(2) == 1);
}
END_TEST

START_TEST(test_sane_worker_threads)
{
    fail_unless(sane_worker_threads(-1) == 1);
    fail_unless(sane_worker_threads(0) == 1);
    fail_unless(sane_worker_threads(1) == 0);
    fail_unless(sane_worker_threads(2) == 0);
    fail_unless(sane_worker_threads(16) == 0);
}
END_TEST

START_TEST(test_filter_config_bad_file)
{
    bloom_filter_config config;
    memset(&config, '\0', sizeof(config));
    int res = filter_config_from_filename("/tmp/does_not_exist", &config);
    fail_unless(res == -ENOENT);

    // Should get the defaults...
    fail_unless(config.initial_capacity == 0);
    fail_unless(config.default_probability == 0);
    fail_unless(config.scale_size == 0);
    fail_unless(config.probability_reduction == 0);
    fail_unless(config.size == 0);
    fail_unless(config.capacity == 0);
    fail_unless(config.bytes == 0);
}
END_TEST

START_TEST(test_filter_config_empty_file)
{
    int fh = open("/tmp/zero_file", O_CREAT|O_RDWR, 0777);
    fchmod(fh, 777);
    close(fh);

    bloom_filter_config config;
    memset(&config, '\0', sizeof(config));
    int res = filter_config_from_filename("/tmp/zero_file", &config);
    fail_unless(res == 0);

    // Should get the defaults...
    fail_unless(config.initial_capacity == 0);
    fail_unless(config.default_probability == 0);
    fail_unless(config.scale_size == 0);
    fail_unless(config.probability_reduction == 0);
    fail_unless(config.size == 0);
    fail_unless(config.capacity == 0);
    fail_unless(config.bytes == 0);

    unlink("/tmp/zero_file");
}
END_TEST

START_TEST(test_filter_config_basic_config)
{
    int fh = open("/tmp/filter_basic_config", O_CREAT|O_RDWR, 0777);
    char *buf = "[bloomd]\n\
size = 256\n\
bytes = 999999\n\
capacity = 4000000\n\
scale_size = 2\n\
in_memory = 1\n\
initial_capacity = 2000000\n\
default_probability = 0.005\n\
probability_reduction = 0.8\n";
    write(fh, buf, strlen(buf));
    fchmod(fh, 777);
    close(fh);

    bloom_filter_config config;
    memset(&config, '\0', sizeof(config));
    int res = filter_config_from_filename("/tmp/filter_basic_config", &config);
    fail_unless(res == 0);

    // Should get the config
    fail_unless(config.initial_capacity == 2000000);
    fail_unless(config.default_probability == 0.005);
    fail_unless(config.scale_size == 2);
    fail_unless(config.probability_reduction == 0.8);
    fail_unless(config.size == 256);
    fail_unless(config.capacity == 4000000);
    fail_unless(config.bytes == 999999);

    unlink("/tmp/filter_basic_config");
}
END_TEST

START_TEST(test_update_filename_from_filter_config)
{
    bloom_filter_config config;
    config.initial_capacity = 2000000;
    config.default_probability = 0.005;
    config.scale_size = 2;
    config.probability_reduction = 0.8;
    config.size = 256;
    config.capacity = 4000000;
    config.bytes = 999999;
    config.in_memory = 0;

    int res = update_filename_from_filter_config("/tmp/update_filter", &config);
    chmod("/tmp/update_filter", 777);
    fail_unless(res == 0);

    // Should get the config
    bloom_filter_config config2;
    memset(&config2, '\0', sizeof(config2));
    res = filter_config_from_filename("/tmp/update_filter", &config2);
    fail_unless(res == 0);

    fail_unless(config2.initial_capacity == 2000000);
    fail_unless(config2.default_probability == 0.005);
    fail_unless(config2.scale_size == 2);
    fail_unless(config2.probability_reduction == 0.8);
    fail_unless(config2.size == 256);
    fail_unless(config2.capacity == 4000000);
    fail_unless(config2.bytes == 999999);
    fail_unless(config2.in_memory == 0);

    unlink("/tmp/update_filter");
}
END_TEST

