#include <dirent.h>

#ifdef __MACH__
#define __MAC_10_8 1080
#include <Availability.h>
#if __MAC_OS_X_VERSION_MIN_REQUIRED < __MAC_10_8
#define CONST_DIRENT_T struct dirent
#else
#define CONST_DIRENT_T const struct dirent
#endif
#else
#define CONST_DIRENT_T const struct dirent
#endif

