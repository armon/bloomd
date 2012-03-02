#include "background.h"

/**
 * Starts a flushing thread which on every
 * configured flush interval, flushes all the filters.
 * @arg config The configuration
 * @arg mgr The filter manager to use
 * @arg should_run Pointer to an integer that is set to 0 to
 * indicate the thread should exit.
 */
void start_flush_thread(bloom_config *config, bloom_filtmgr *mgr, int *should_run) {

}

/**
 * Starts a cold unmap thread which on every
 * cold interval unamps cold filtesr.
 * @arg config The configuration
 * @arg mgr The filter manager to use
 * @arg should_run Pointer to an integer that is set to 0 to
 * indicate the thread should exit.
 */
void start_cold_unmap_thread(bloom_config *config, bloom_filtmgr *mgr, int *should_run) {

}

