#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <assert.h>
#include "conn_handler.h"
#include "handler_constants.c"

/**
 * Defines the number of keys we set/check in a single
 * iteration for our multi commands. We do not do all the
 * keys at one time to prevent a client from holding locks
 * for too long. This is especially critical for set
 * operations which serialize access.
 */
#define MULTI_OP_SIZE 32

/**
 * Invoked in any context with a bloom_conn_handler
 * to send out an INTERNAL_ERROR message to the client.
 */
#define INTERNAL_ERROR() (handle_client_resp(handle->conn, (char*)INTERNAL_ERR, INTERNAL_ERR_LEN))

/* Static method declarations */
static void handle_check_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_check_multi_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_set_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_set_multi_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_create_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_drop_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_close_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_clear_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_list_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_info_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_flush_cmd(bloom_conn_handler *handle, char *args, int args_len);

static int handle_multi_response(bloom_conn_handler *handle, int cmd_res, int num_keys, char *res_buf, int end_of_input);
static inline void handle_client_resp(bloom_conn_info *conn, char* resp_mesg, int resp_len);
static void handle_client_err(bloom_conn_info *conn, char* err_msg, int msg_len);
static conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len);
static int buffer_after_terminator(char *buf, int buf_len, char terminator, char **after_term, int *after_len);

/**
 * Invoked to initialize the conn handler layer.
 */
void init_conn_handler() {
    // Compile our regexes
    int res;
    res = regcomp(&VALID_FILTER_NAMES_RE, VALID_FILTER_NAMES_PATTERN, REG_EXTENDED|REG_NOSUB);
    assert(res == 0);

}

/**
 * Invoked by the networking layer when there is new
 * data to be handled. The connection handler should
 * consume all the input possible, and generate responses
 * to all requests.
 * @arg handle The connection related information
 * @return 0 on success.
 */
int handle_client_connect(bloom_conn_handler *handle) {
    // Look for the next command line
    char *buf, *arg_buf;
    int buf_len, arg_buf_len, should_free;
    int status;
    arg_buf_len = 0;
    while (1) {
        status = extract_to_terminator(handle->conn, '\n', &buf, &buf_len, &should_free);
        if (status == -1) break; // Return if no command is available

        // Determine the command type
        conn_cmd_type type = determine_client_command(buf, buf_len, &arg_buf, &arg_buf_len);

        // Handle an error or unknown response
        switch(type) {
            case CHECK:
                handle_check_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CHECK_MULTI:
                handle_check_multi_cmd(handle, arg_buf, arg_buf_len);
                break;
            case SET:
                handle_set_cmd(handle, arg_buf, arg_buf_len);
                break;
            case SET_MULTI:
                handle_set_multi_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CREATE:
                handle_create_cmd(handle, arg_buf, arg_buf_len);
                break;
            case DROP:
                handle_drop_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CLOSE:
                handle_close_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CLEAR:
                handle_clear_cmd(handle, arg_buf, arg_buf_len);
                break;
            case LIST:
                handle_list_cmd(handle, arg_buf, arg_buf_len);
                break;
            case INFO:
                handle_info_cmd(handle, arg_buf, arg_buf_len);
                break;
            case FLUSH:
                handle_flush_cmd(handle, arg_buf, arg_buf_len);
                break;
            default:
                handle_client_err(handle->conn, (char*)&CMD_NOT_SUP, CMD_NOT_SUP_LEN);
                break;
        }

        // Make sure to free the command buffer if we need to
        if (should_free) free(buf);
    }

    return 0;
}

/**
 * Periodic update is used to update our checkpoint with
 * the filter manager, so that vacuum progress can be made.
 */
void periodic_update(bloom_conn_handler *handle) {
    filtmgr_client_checkpoint(handle->mgr);
}


/**
 * Internal method to handle a command that relies
 * on a filter name and a single key, responses are handled using
 * handle_multi_response.
 */
static void handle_filt_key_cmd(bloom_conn_handler *handle, char *args, int args_len,
        int(*filtmgr_func)(bloom_filtmgr *, char*, char **, int, char*)) {
    #define CHECK_ARG_ERR() { \
        handle_client_err(handle->conn, (char*)&FILT_KEY_NEEDED, FILT_KEY_NEEDED_LEN); \
        return; \
    }
    // If we have no args, complain.
    if (!args) CHECK_ARG_ERR();

    // Scan past the filter name
    char *key;
    int key_len;
    int err = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (err || key_len <= 1) CHECK_ARG_ERR();

    // Setup the buffers
    char *key_buf[] = {key};
    char result_buf[1];

    // Call into the filter manager
    int res = filtmgr_func(handle->mgr, args, (char**)&key_buf, 1, (char*)&result_buf);
    handle_multi_response(handle, res, 1, (char*)&result_buf, 1);
}

static void handle_check_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_key_cmd(handle, args, args_len, filtmgr_check_keys);
}

static void handle_set_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_key_cmd(handle, args, args_len, filtmgr_set_keys);
}


/**
 * Internal method to handle a command that relies
 * on a filter name and multiple keys, responses are handled using
 * handle_multi_response.
 */
static void handle_filt_multi_key_cmd(bloom_conn_handler *handle, char *args, int args_len,
        int(*filtmgr_func)(bloom_filtmgr *, char*, char **, int, char*)) {
    #define CHECK_ARG_ERR() { \
        handle_client_err(handle->conn, (char*)&FILT_KEY_NEEDED, FILT_KEY_NEEDED_LEN); \
        return; \
    }
    // If we have no args, complain.
    if (!args) CHECK_ARG_ERR();

    // Setup the buffers
    char *key_buf[MULTI_OP_SIZE];
    char result_buf[MULTI_OP_SIZE];

    // Scan all the keys
    char *key;
    int key_len;
    int err = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (err || key_len <= 1) CHECK_ARG_ERR();

    // Parse any options
    char *curr_key = key;
    int index = 0;
    #define HAS_ANOTHER_KEY() (curr_key && *curr_key != '\0')
    while (HAS_ANOTHER_KEY()) {
        // Adds a zero terminator to the current key, scans forward
        buffer_after_terminator(key, key_len, ' ', &key, &key_len);

        // Set the key
        key_buf[index] = curr_key;

        // Advance to the next key
        curr_key = key;
        index++;

        // If we have filled the buffer, check now
        if (index == MULTI_OP_SIZE) {
            //  Handle the keys now
            int res = filtmgr_func(handle->mgr, args, (char**)&key_buf, index, (char*)&result_buf);
            res = handle_multi_response(handle, res, index, (char*)&result_buf, !HAS_ANOTHER_KEY());
            if (res) return;

            // Reset the index
            index = 0;
        }
    }

    // Handle any remaining keys
    if (index) {
        int res = filtmgr_func(handle->mgr, args, key_buf, index, result_buf);
        handle_multi_response(handle, res, index, (char*)&result_buf, 1);
    }
}

static void handle_check_multi_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_multi_key_cmd(handle, args, args_len, filtmgr_check_keys);
}

static void handle_set_multi_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_multi_key_cmd(handle, args, args_len, filtmgr_set_keys);
}


/**
 * Internal command used to handle filter creation.
 */
static void handle_create_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    // If we have no args, complain.
    if (!args) {
        handle_client_err(handle->conn, (char*)&FILT_NEEDED, FILT_NEEDED_LEN);
        return;
    }

    // Scan for options after the filter name
    char *options;
    int options_len;
    int res = buffer_after_terminator(args, args_len, ' ', &options, &options_len);

    // Verify the filter name is valid
    char *filter_name = args;
    if (regexec(&VALID_FILTER_NAMES_RE, filter_name, 0, NULL, 0) != 0) {
        handle_client_err(handle->conn, (char*)&BAD_FILT_NAME, BAD_FILT_NAME_LEN);
        return;
    }

    // Parse the options
    bloom_config *config = NULL;
    int err = 0;
    if (res == 0) {
        // Make a new config store, copy the current
        config = malloc(sizeof(bloom_config));
        memcpy(config, handle->config, sizeof(bloom_config));

        // Parse any options
        char *param = options;
        while (param) {
            // Adds a zero terminator to the current param, scans forward
            buffer_after_terminator(options, options_len, ' ', &options, &options_len);

            // Check for the custom params
            int match = 0;
            match |= sscanf(param, "capacity=%llu", (unsigned long long*)&config->initial_capacity);
            match |= sscanf(param, "prob=%lf", &config->default_probability);
            match |= sscanf(param, "in_memory=%d", &config->in_memory);

            // Check if there was no match
            if (!match) {
                err = 1;
                handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
                break;
            }

            // Advance to the next param
            param = options;
        }

        // Validate the params
        int invalid_config = 0;
        invalid_config |= sane_initial_capacity(config->initial_capacity);
        invalid_config |= sane_default_probability(config->default_probability);
        invalid_config |= sane_in_memory(config->in_memory);

        // Barf if the configs are bad
        if (invalid_config) {
            err = 1;
            handle_client_err(handle->conn, (char*)&BAD_ARGS, BAD_ARGS_LEN);
        }
    }

    // Clean up an leave on errors
    if (err) {
        if (config) free(config);
        return;
    }

    // Create a new filter
    res = filtmgr_create_filter(handle->mgr, filter_name, config);
    switch (res) {
        case 0:
            handle_client_resp(handle->conn, (char*)DONE_RESP, DONE_RESP_LEN);
            break;
        case -1:
            handle_client_resp(handle->conn, (char*)EXISTS_RESP, EXISTS_RESP_LEN);
            if (config) free(config);
            break;
        case -3:
            handle_client_resp(handle->conn, (char*)DELETE_IN_PROGRESS, DELETE_IN_PROGRESS_LEN);
            if (config) free(config);
            break;
        default:
            INTERNAL_ERROR();
            if (config) free(config);
            break;
    }
}


/**
 * Internal method to handle a command that relies
 * on a filter name and a single key, responses are handled using
 * handle_multi_response.
 */
static void handle_filt_cmd(bloom_conn_handler *handle, char *args, int args_len,
        int(*filtmgr_func)(bloom_filtmgr *, char*)) {
    // If we have no args, complain.
    if (!args) {
        handle_client_err(handle->conn, (char*)&FILT_NEEDED, FILT_NEEDED_LEN);
        return;
    }

    // Scan past the filter name
    char *key;
    int key_len;
    int after = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (after == 0) {
        handle_client_err(handle->conn, (char*)&UNEXPECTED_ARGS, UNEXPECTED_ARGS_LEN);
        return;
    }

    // Call into the filter manager
    int res = filtmgr_func(handle->mgr, args);
    switch (res) {
        case 0:
            handle_client_resp(handle->conn, (char*)DONE_RESP, DONE_RESP_LEN);
            break;
        case -1:
            handle_client_resp(handle->conn, (char*)FILT_NOT_EXIST, FILT_NOT_EXIST_LEN);
            break;
        case -2:
            handle_client_resp(handle->conn, (char*)FILT_NOT_PROXIED, FILT_NOT_PROXIED_LEN);
            break;
        default:
            INTERNAL_ERROR();
            break;
    }
}

static void handle_drop_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_cmd(handle, args, args_len, filtmgr_drop_filter);
}

static void handle_close_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_cmd(handle, args, args_len, filtmgr_unmap_filter);
}

static void handle_clear_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    handle_filt_cmd(handle, args, args_len, filtmgr_clear_filter);
}

// Callback invoked by list command to create an output
// line for each filter. We hold a filter handle which we
// can use to get some info about it
static void list_filter_cb(void *data, char *filter_name, bloom_filter *filter) {
    char **out = data;
    int res;
    res = asprintf(out, "%s %f %llu %llu %llu\n",
            filter_name,
            filter->filter_config.default_probability,
            (unsigned long long)bloomf_byte_size(filter),
            (unsigned long long)bloomf_capacity(filter),
            (unsigned long long)bloomf_size(filter));
    assert(res != -1);
}

static void handle_list_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    (void)args_len;

    // List all the filters
    bloom_filter_list_head *head;
    int res = filtmgr_list_filters(handle->mgr, args, &head);
    if (res != 0) {
        INTERNAL_ERROR();
        return;
    }

    // Allocate buffers for the responses
    int num_out = (head->size+2);
    char** output_bufs = malloc(num_out * sizeof(char*));
    int* output_bufs_len = malloc(num_out * sizeof(int));

    // Setup the START/END lines
    output_bufs[0] = (char*)&START_RESP;
    output_bufs_len[0] = START_RESP_LEN;
    output_bufs[head->size+1] = (char*)&END_RESP;
    output_bufs_len[head->size+1] = END_RESP_LEN;

    // Generate the responses
    char *resp;
    bloom_filter_list *node = head->head;
    for (int i=0; i < head->size; i++) {
        res = filtmgr_filter_cb(handle->mgr, node->filter_name, list_filter_cb, &resp);
        if (res == 0) {
            output_bufs[i+1] = resp;
            output_bufs_len[i+1] = strlen(resp);
        } else { // Skip this output
            output_bufs[i+1] = NULL;
            output_bufs_len[i+1] = 0;
        }
        node = node->next;
    }

    // Write the response
    send_client_response(handle->conn, output_bufs, output_bufs_len, num_out);

    // Cleanup
    for (int i=1; i <= head->size; i++) if(output_bufs[i]) free(output_bufs[i]);
    free(output_bufs);
    free(output_bufs_len);
    filtmgr_cleanup_list(head);
}


// Callback invoked by list command to create an output
// line for each filter. We hold a filter handle which we
// can use to get some info about it
static void info_filter_cb(void *data, char *filter_name, bloom_filter *filter) {
    (void)filter_name;

    // Cast the intput
    char **out = data;

    // Get some metrics
    filter_counters *counters = bloomf_counters(filter);
    uint64_t capacity = bloomf_capacity(filter);
    uint64_t storage = bloomf_byte_size(filter);
    uint64_t size = bloomf_size(filter);
    uint64_t checks = counters->check_hits + counters->check_misses;
    uint64_t sets = counters->set_hits + counters->set_misses;

    // Generate a formatted string output
    int res;
    res = asprintf(out, "capacity %llu\n\
checks %llu\n\
check_hits %llu\n\
check_misses %llu\n\
in_memory %d\n\
page_ins %llu\n\
page_outs %llu\n\
probability %f\n\
sets %llu\n\
set_hits %llu\n\
set_misses %llu\n\
size %llu\n\
storage %llu\n",
    (unsigned long long)capacity, (unsigned long long)checks,
    (unsigned long long)counters->check_hits, (unsigned long long)counters->check_misses,
    ((bloomf_is_proxied(filter)) ? 0 : 1),
    (unsigned long long)counters->page_ins, (unsigned long long)counters->page_outs,
    filter->filter_config.default_probability,
    (unsigned long long)sets, (unsigned long long)counters->set_hits,
    (unsigned long long)counters->set_misses, (unsigned long long)size, (unsigned long long)storage);
    assert(res != -1);
}

static void handle_info_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    // If we have no args, complain.
    if (!args) {
        handle_client_err(handle->conn, (char*)&FILT_NEEDED, FILT_NEEDED_LEN);
        return;
    }

    // Scan past the filter name
    char *key;
    int key_len;
    int after = buffer_after_terminator(args, args_len, ' ', &key, &key_len);
    if (after == 0) {
        handle_client_err(handle->conn, (char*)&UNEXPECTED_ARGS, UNEXPECTED_ARGS_LEN);
        return;
    }

    // Create output buffers
    char *output[] = {(char*)&START_RESP, NULL, (char*)&END_RESP};
    int lens[] = {START_RESP_LEN, 0, END_RESP_LEN};

    // Invoke the callback to get the filter stats
    int res = filtmgr_filter_cb(handle->mgr, args, info_filter_cb, &output[1]);

    // Check for no filter
    if (res != 0) {
        switch (res) {
            case -1:
                handle_client_resp(handle->conn, (char*)FILT_NOT_EXIST, FILT_NOT_EXIST_LEN);
                break;
            default:
                INTERNAL_ERROR();
                break;
        }
        return;
    }

    // Adjust the buffer size
    lens[1] = strlen(output[1]);

    // Write out the bufs
    send_client_response(handle->conn, (char**)&output, (int*)&lens, 3);
    free(output[1]);
}


static void handle_flush_cmd(bloom_conn_handler *handle, char *args, int args_len) {
    // If we have a specfic filter, use filt_cmd
    if (args) {
        handle_filt_cmd(handle, args, args_len, filtmgr_flush_filter);
        return;
    }

    // List all the filters
    bloom_filter_list_head *head;
    int res = filtmgr_list_filters(handle->mgr, NULL, &head);
    if (res != 0) {
        INTERNAL_ERROR();
        return;
    }

    // Flush all, ignore errors since
    // filters might get deleted in the process
    bloom_filter_list *node = head->head;
    while (node) {
        filtmgr_flush_filter(handle->mgr, node->filter_name);
        node = node->next;
    }

    // Respond
    handle_client_resp(handle->conn, (char*)DONE_RESP, DONE_RESP_LEN);

    // Cleanup
    filtmgr_cleanup_list(head);
}


/**
 * Helper to handle sending the response to the multi commands,
 * either multi or bulk.
 * @arg handle The conn handle
 * @arg cmd_res The result of the command
 * @arg num_keys The number of keys in the result buffer. This should NOT be
 * more than MULTI_OP_SIZE.
 * @arg res_buf The result buffer
 * @arg end_of_input Should the last result include a new line
 * @return 0 on success, 1 if we should stop.
 */
static int handle_multi_response(bloom_conn_handler *handle, int cmd_res, int num_keys, char *res_buf, int end_of_input) {
    // Do nothing if we get too many keys
    if (num_keys > MULTI_OP_SIZE || num_keys <= 0) return 1;

    if (cmd_res != 0) {
        switch (cmd_res) {
            case -1:
                handle_client_resp(handle->conn, (char*)FILT_NOT_EXIST, FILT_NOT_EXIST_LEN);
                break;
            default:
                INTERNAL_ERROR();
                break;
        }
        return 1;
    }

    // Allocate buffers for our response, plus a newline
    char *resp_bufs[MULTI_OP_SIZE];
    int resp_buf_lens[MULTI_OP_SIZE];

    // Set the response buffers according to the results
    int last_key = 1;
    for (int i=0; i < num_keys; i++) {
        last_key = end_of_input && (i == (num_keys - 1));
        switch (res_buf[i]) {
            case 0:
                resp_bufs[i] = (char*)((last_key) ? NO_RESP : NO_SPACE);
                resp_buf_lens[i] = (last_key) ? NO_RESP_LEN: NO_SPACE_LEN;
                break;
            case 1:
                resp_bufs[i] = (char*)((last_key) ? YES_RESP : YES_SPACE);
                resp_buf_lens[i] = (last_key) ? YES_RESP_LEN: YES_SPACE_LEN;
                break;
            default:
                INTERNAL_ERROR();
                return 1;
        }
    }

    // Write out!
    send_client_response(handle->conn, (char**)&resp_bufs, (int*)&resp_buf_lens, num_keys);
    return 0;
}


/**
 * Sends a client response message back. Simple convenience wrapper
 * around handle_client_resp.
 */
static inline void handle_client_resp(bloom_conn_info *conn, char* resp_mesg, int resp_len) {
    char *buffers[] = {resp_mesg};
    int sizes[] = {resp_len};
    send_client_response(conn, (char**)&buffers, (int*)&sizes, 1);
}


/**
 * Sends a client error message back. Optimizes to use multiple
 * output buffers so we can collapse this into a single write without
 * needing to move our buffers around.
 */
static void handle_client_err(bloom_conn_info *conn, char* err_msg, int msg_len) {
    char *buffers[] = {(char*)&CLIENT_ERR, err_msg, (char*)&NEW_LINE};
    int sizes[] = {CLIENT_ERR_LEN, msg_len, NEW_LINE_LEN};
    send_client_response(conn, (char**)&buffers, (int*)&sizes, 3);
}


/**
 * Determines the client command.
 * @arg cmd_buf A command buffer
 * @arg buf_len The length of the buffer
 * @arg arg_buf Output. Sets the start address of the command arguments.
 * @arg arg_len Output. Sets the length of arg_buf.
 * @return The conn_cmd_type enum value.
 * UNKNOWN if it doesn't match anything supported, or a proper command.
 */
static conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len) {
    // Check if we are ending with \r, and remove it.
    if (cmd_buf[buf_len-2] == '\r') {
        cmd_buf[buf_len-2] = '\0';
        buf_len -= 1;
    }

    // Scan for a space. This will setup the arg_buf and arg_len
    // if we do find the terminator. It will also insert a null terminator
    // at the space, so we can compare the cmd_buf to the commands.
    buffer_after_terminator(cmd_buf, buf_len, ' ', arg_buf, arg_len);

    // Search for the command
    conn_cmd_type type = UNKNOWN;
    #define CMD_MATCH(name) (strcmp(name, cmd_buf) == 0)
    if (CMD_MATCH("c") || CMD_MATCH("check")) {
        type = CHECK;
    } else if (CMD_MATCH("m") || CMD_MATCH("multi")) {
        type = CHECK_MULTI;
    } else if (CMD_MATCH("s") || CMD_MATCH("set")) {
        type = SET;
    } else if (CMD_MATCH("b") || CMD_MATCH("bulk")) {
        type = SET_MULTI;
    } else if (CMD_MATCH("list")) {
        type = LIST;
    } else if (CMD_MATCH("info")) {
        type = INFO;
    } else if (CMD_MATCH("create")) {
        type = CREATE;
    } else if (CMD_MATCH("drop")) {
        type = DROP;
    } else if (CMD_MATCH("close")) {
        type = CLOSE;
    } else if (CMD_MATCH("clear")) {
        type = CLEAR;
    } else if (CMD_MATCH("flush")) {
        type = FLUSH;
    }

    return type;
}


/**
 * Scans the input buffer of a given length up to a terminator.
 * Then sets the start of the buffer after the terminator including
 * the length of the after buffer.
 * @arg buf The input buffer
 * @arg buf_len The length of the input buffer
 * @arg terminator The terminator to scan to. Replaced with the null terminator.
 * @arg after_term Output. Set to the byte after the terminator.
 * @arg after_len Output. Set to the length of the output buffer.
 * @return 0 if terminator found. -1 otherwise.
 */
static int buffer_after_terminator(char *buf, int buf_len, char terminator, char **after_term, int *after_len) {
    // Scan for a space
    char *term_addr = memchr(buf, terminator, buf_len);
    if (!term_addr) {
        *after_term = NULL;
        return -1;
    }

    // Convert the space to a null-seperator
    *term_addr = '\0';

    // Provide the arg buffer, and arg_len
    *after_term = term_addr+1;
    *after_len = buf_len - (term_addr - buf + 1);
    return 0;
}

