#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include "bitmap.h"

/**
 * Returns a cbloom_bitmap pointer from a file handle
 * that is already opened with read/write privileges.
 * @arg fileno The fileno. 0 for anonymous.
 * @arg len The length of the bitmap in bytes.
 */
cbloom_bitmap *bitmapFromFile(int fileno, size_t len) {
    // Handle anonymous or file backed
    int flags = 0;
    int newfileno;
    cbloom_flags mode;
    if (fileno == -1) {
        flags |= MAP_ANON;
        flags |= MAP_PRIVATE;
        newfileno = -1;
        mode = ANONYMOUS;
    } else {
        flags |= MAP_FILE;
        flags |= MAP_SHARED;
        newfileno = dup(fileno);
        mode = SHARED;
    }

    // Perform the map in
    unsigned char* addr = mmap(NULL, len, PROT_READ|PROT_WRITE, flags, newfileno, 0);

    // Check for an error, otherwise return
    if (addr == MAP_FAILED) {
        perror("Failed to mmap");
        if (newfileno >= 0) {
            close(newfileno);
        }
        return NULL;
    }

    // Fault the memory in 
    if (mode == SHARED) {
        int res = madvise(addr, len, MADV_WILLNEED);
        if (res != 0) {
            perror("Failed to call madvise()");
        }
    }

    // Allocate space for the map
    cbloom_bitmap *map = (cbloom_bitmap*)calloc(1, sizeof(cbloom_bitmap));
    map->size = len;
    map->flags = mode;
    map->fileno = newfileno;
    map->mmap = addr;

    return map;
}


/**
 * Returns a cbloom_bitmap pointer from a filename.
 * Opens the file with read/write privileges. If create
 * is true, then a file will be created if it does not exist.
 * If the file cannot be opened, NULL will be returned.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg create If 1, then the file will be created if it does not exist.
 * @arg resize If 1, then the file will be expanded to len
 */
cbloom_bitmap *bitmapFromFilename(char* filename, size_t len, int create, int resize) {
    // Get the flags
    int flags = O_RDWR;
    if (create) {
        flags |= O_CREAT;
    }

    // Open the file
    int fileno = open(filename, flags);
    if (fileno == -1) {
        perror("Failed to open the file");
        return NULL;
    }

    // Check if we need to resize
    if (resize) {
        struct stat buf;
        int res = fstat(fileno, &buf);        
        if (res != 0) {
            perror("fstat failed on bitmap!");
        }
        if (buf.st_size < len) {
            res = ftruncate(fileno, len);
            if (res != 0) {
                perror("ftrunctate failed on the bitmap!");
                close(fileno);
                return NULL;
            }
        }
    }

    // Use the filehandler mode
    cbloom_bitmap *map = bitmapFromFile(fileno, len); 

    // Handle is dup'ed, we can close
    close(fileno);
    return map;
}


/**
 * Returns the size of the bitmap in bits.
 * @arg map The bitmap
 */
size_t bitmapBitsize(cbloom_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return 0;

    return (*map).size * 8;
}


/**
 * Flushes the bitmap back to disk. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps.
 * @arg map The bitmap
 * @returns 0 on success, negative failure.
 */
int bitmapFlush(cbloom_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return -EINVAL;

    // Do nothing for anonymous maps
    if (map->flags == ANONYMOUS || map->mmap == NULL)
        return 0;

    int res = msync(map->mmap, map->size, MS_SYNC);
    if (res == -1) return -errno;

    res = fsync(map->fileno);
    if (res == -1) return -errno;
    return 0;
}


/**
 * Closes and flushes the bitmap. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps. The caller should free()
 * the structure after.
 * @arg map The bitmap
 * @returns 0 on success, negative on failure.
 */
int bitmapClose(cbloom_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return -EINVAL;

    // Flush first
    int res = bitmapFlush(map);
    if (res != 0) return res;

    // Unmap the file
    res = munmap(map->mmap, map->size);
    if (res != 0) return -errno;

    // Close the file descriptor if file backed
    if (map->flags != ANONYMOUS) {
       res = close(map->fileno);
       if (res != 0) return -errno;
    }

    // Cleanup
    map->mmap = NULL;
    map->fileno = -1;
    return 0;
}

