#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "conn_handler.h"

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

