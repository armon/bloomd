#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "conn_handler.h"

static const char CLIENT_ERR[] = "Client Error: ";
static const int CLIENT_ERR_LEN = sizeof(CLIENT_ERR) - 1;
static const char CMD_NOT_SUP[] = "Command not supported";
static const int CMD_NOT_SUP_LEN = sizeof(CMD_NOT_SUP) - 1;
static const char NEW_LINE[] = "\n";
static const int NEW_LINE_LEN = sizeof(NEW_LINE) - 1;

typedef enum {
    UNKNOWN = 0,    // Unrecognized command
    CHECK,          // Check a single key
    CHECK_MULTI,    // Check multiple space-seperated keys
    SET,            // Set a single key
    SET_MULTI,      // Set multiple space-seperated keys
    LIST,           // List filters
    INFO,           // Info about a fileter
    CREATE,         // Creates a filter
    DROP,           // Drop a filter
    CLOSE,          // Close a filter
    FLUSH,          // Force flush a filter
    CONF            // Configuration dump
} conn_cmd_type;

/* Static method declarations */
static conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len);
static void handle_client_err(bloom_conn_info *conn, char* err_msg, int msg_len);

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
            case UNKNOWN:
                handle_client_err(handle->conn, (char*)&CMD_NOT_SUP, CMD_NOT_SUP_LEN);
                break;
            default:
                printf("Real command: %d\n", type);
                break;
        }

        if (should_free) free(buf);
    }

    return 0;
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
    // Scan for a space
    char *term_addr = memchr(cmd_buf, ' ', buf_len);

    // If there is no space, it could be a command that
    // does not expect args
    if (term_addr) {
         // Convert the space to a null-seperator
        *term_addr = '\0';

        // Provide the arg buffer, and arg_len
        *arg_buf = term_addr+1;
        *arg_len = buf_len - (term_addr - cmd_buf + 1);
    }

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
    }

    return type;
}

