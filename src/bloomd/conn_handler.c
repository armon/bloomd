#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "conn_handler.h"

enum conn_cmd_type {
    ERR = -1,       // Error determining type
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
};

/* Static method declarations */
static enum conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len);

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
    char *buf;
    int buf_len, should_free;
    int status;
    while (1) {
        status = extract_to_terminator(handle->conn, '\n', &buf, &buf_len, &should_free);
        if (status == -1) return 0; // Return if no command is available
        printf("Buffer: %s\n", buf);
        if (should_free) free(buf);
    }

    return 0;
}

/**
 * Determines the client command.
 * @arg cmd_buf A command buffer
 * @arg buf_len The length of the buffer
 * @arg arg_buf Output. Sets the start address of the command arguments.
 * @arg arg_len Output. Sets the length of arg_buf.
 * @return The conn_cmd_type enum value. Could be ERR if a command is not found,
 * UNKNOWN if it doesn't match anything supported, or a proper command.
 */
static enum conn_cmd_type determine_client_command(char *cmd_buf, int buf_len, char **arg_buf, int *arg_len) {
    // Scan for a space
    char *term_addr = memchr(cmd_buf, ' ', buf_len);
    if (!term_addr) return ERR;

    // Convert the space to a null-seperator
    *term_addr = '\0';

    // Provide the arg buffer, and arg_len
    *arg_buf = term_addr+1;
    *arg_len = buf_len - (term_addr - cmd_buf + 1);

    // Search for the command
    enum conn_cmd_type type = UNKNOWN;
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

