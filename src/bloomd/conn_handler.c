#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <assert.h>
#include "conn_handler.h"
#include "handler_constants.c"

/* Static method declarations */
static void handle_check_cmd(bloom_conn_handler *handle, char *args, int args_len);
static void handle_create_cmd(bloom_conn_handler *handle, char *args, int args_len);
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
    while (1) {
        status = extract_to_terminator(handle->conn, '\n', &buf, &buf_len, &should_free);
        if (status == -1) return 0; // Return if no command is available
        printf("Buffer: %s\n", buf);

        // Determine the command type
        conn_cmd_type type = determine_client_command(buf, buf_len, &arg_buf, &arg_buf_len);

        // Handle an error or unknown response
        switch(type) {
            case CHECK:
                handle_check_cmd(handle, arg_buf, arg_buf_len);
                break;
            case CREATE:
                handle_create_cmd(handle, arg_buf, arg_buf_len);
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


static void handle_check_cmd(bloom_conn_handler *handle, char *args, int args_len) {
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
    int res = filtmgr_check_keys(handle->mgr, args, (char**)&key_buf, 1, (char*)&result_buf);
    switch (res) {
        case 0:
            switch (result_buf[0]) {
                case 0:
                    handle_client_resp(handle->conn, (char*)NO_RESP, NO_RESP_LEN);
                    break;
                case 1:
                    handle_client_resp(handle->conn, (char*)YES_RESP, YES_RESP_LEN);
                    break;
                default:
                    handle_client_resp(handle->conn, (char*)INTERNAL_ERR, INTERNAL_ERR_LEN);
                    break;
            }
            break;
        case -1:
            handle_client_resp(handle->conn, (char*)FILT_NOT_EXIST, FILT_NOT_EXIST_LEN);
            break;
        default:
            handle_client_resp(handle->conn, (char*)INTERNAL_ERR, INTERNAL_ERR_LEN);
            break;
    }
}


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
    if (regexec(&VALID_FILTER_NAMES_RE, filter_name, 0, NULL, 0)) {
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
            match |= sscanf(param, "capacity=%llu", &config->initial_capacity);
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
        default:
            handle_client_resp(handle->conn, (char*)INTERNAL_ERR, INTERNAL_ERR_LEN);
            if (config) free(config);
            break;
    }
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
    } else if (CMD_MATCH("flush")) {
        type = FLUSH;
    } else if (CMD_MATCH("conf")) {
        type = CONF;
    } else if (CMD_MATCH("quit")) {
        type = QUIT;
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

