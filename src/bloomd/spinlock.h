#include <unistd.h>
#include <pthread.h>

#ifndef BLOOM_SPINLOCK_H
#define BLOOM_SPINLOCK_H

#ifdef __MACH__
// On OSX use OSAtomic
#include <os/lock.h>
typedef os_unfair_lock bloom_spinlock;
#define INIT_BLOOM_SPIN(spin) { *spin = OS_UNFAIR_LOCK_INIT; }
#define LOCK_BLOOM_SPIN(spin) { os_unfair_lock_lock(spin); }
#define UNLOCK_BLOOM_SPIN(spin) { os_unfair_lock_unlock(spin); }

#else
#ifdef _POSIX_SPIN_LOCKS
// On most POSIX systems, use pthreads spin locks
typedef pthread_spinlock_t bloom_spinlock;
#define INIT_BLOOM_SPIN(spin) { pthread_spin_init(spin, PTHREAD_PROCESS_PRIVATE); }
#define LOCK_BLOOM_SPIN(spin) { pthread_spin_lock(spin); }
#define UNLOCK_BLOOM_SPIN(spin) { pthread_spin_unlock(spin); }

#else
// Fall back onto a mutex
typedef pthread_mutex_t bloom_spinlock;
#define INIT_BLOOM_SPIN(spin) { pthread_mutex_init(spin, NULL); }
#define LOCK_BLOOM_SPIN(spin) { pthread_mutex_lock(spin); }
#define UNLOCK_BLOOM_SPIN(spin) { pthread_mutex_unlock(spin); }

#endif
#endif

#endif
