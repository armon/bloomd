#ifndef BLOOM_BACKGROUND_H
#define BLOOM_BACKGROUND_H
#include <pthread.h>
#include "config.h"
#include "filter_manager.h"

/**
 * Starts a flushing thread which on every
 * configured flush interval, flushes all the filters.
 * @arg config The configuration
 * @arg mgr The filter manager to use
 * @arg should_run Pointer to an integer that is set to 0 to
 * indicate the thread should exit.
 * @arg t The output thread
 * @return 1 if the thread was started
 */
int start_flush_thread(bloom_config *config, bloom_filtmgr *mgr, int *should_run, pthread_t *t);

/**
 * Starts a cold unmap thread which on every
 * cold interval unamps cold filtesr.
 * @arg config The configuration
 * @arg mgr The filter manager to use
 * @arg should_run Pointer to an integer that is set to 0 to
 * indicate the thread should exit.
 * @arg t The output thread
 * @return 1 if the thread was started
 */
int start_cold_unmap_thread(bloom_config *config, bloom_filtmgr *mgr, int *should_run, pthread_t *);

#endif
