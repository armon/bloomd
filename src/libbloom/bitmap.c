#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include "bitmap.h"

/* Static declarations */
static int fill_buffer(int fileno, unsigned char* buf, uint64_t len);
static int flush_dirty_pages(bloom_bitmap *map);
static int flush_page(bloom_bitmap *map, uint64_t page);


/**
 * Returns a bloom_bitmap pointer from a file handle
 * that is already opened with read/write privileges.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg mode The mode to use for the bitmap.
 * @arg map The output map. Will be initialized.
 * @return 0 on success. Negative on error.
 */
int bitmap_from_file(int fileno, uint64_t len, bitmap_mode mode, bloom_bitmap *map) {
    // Handle each mode
    int flags;
    int newfileno;
    if (mode == SHARED) {
        flags = MAP_SHARED;
        newfileno = dup(fileno);

    } else if (mode == PERSISTENT) {
        flags = MAP_ANON | MAP_PRIVATE;
        newfileno = dup(fileno);

    } else {
        flags = MAP_ANON | MAP_PRIVATE;
        newfileno = -1;
    }

    // Perform the map in
    unsigned char* addr = mmap(NULL, len, PROT_READ|PROT_WRITE, flags, newfileno, 0);

    // Check for an error, otherwise return
    if (addr == MAP_FAILED) {
        if (newfileno >= 0) {
            close(newfileno);
        }
        return -errno;
    }

    // Provide some advise on how the memory will be used
    int res = madvise(addr, len, MADV_WILLNEED);
    if (res != 0) {
        perror("Failed to call madvise() [MADV_WILLNEED]");
    }
    res = madvise(addr, len, MADV_RANDOM);
    if (res != 0) {
        perror("Failed to call madvise() [MADV_RANDOM]");
    }

    // For the PERSISTENT case, we manually track
    // dirty pages, and need a bit field for this
    unsigned char* dirty = NULL;
    if (mode == PERSISTENT) {
        // Calculate how big a bit field we need
        uint64_t pages = ceil(len / 4096.0);        // 1 bit per page
        uint64_t field_size = ceil(pages / 8.0);    // 8 bits per byte

        // Allocate the field
        dirty = malloc(field_size);
        if (!dirty) {
            perror("Failed to allocate dirty page bitfield!");
            munmap(addr, len);
            if (newfileno >= 0) close(newfileno);
            return -errno;
        }

        // Zero out the bit field
        bzero(dirty, field_size);

        // Now we need to read in the existing data
        // since we cannot use the kernel to fault it in
        if ((res = fill_buffer(newfileno, addr, len))) {
            free(dirty);
            munmap(addr, len);
            if (newfileno >= 0) close(newfileno);
            return res;
        }
    }

    // Allocate space for the map
    map->mode = mode;
    map->fileno = newfileno;
    map->size = len;
    map->mmap = addr;
    map->dirty_pages = dirty;
    return 0;
}


/*
 * Populates a buffer with the contents of a file
 */
static int fill_buffer(int fileno, unsigned char* buf, uint64_t len) {
    uint64_t total_read = 0, more;
    while (total_read < len) {
        more = pread(fileno, buf+total_read, len-total_read, total_read);
        if (more == 0)
            break;
        else if (more < 0 && errno != EINTR) {
            perror("Failed to fill the bitmap buffer!");
            return -errno;
        } else
            total_read += more;
    }
    return 0;
}


/**
 * Returns a bloom_bitmap pointer from a filename.
 * Opens the file with read/write privileges. If create
 * is true, then a file will be created if it does not exist.
 * If the file cannot be opened, NULL will be returned.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg create If 1, then the file will be created if it does not exist.
 * @arg resize If 1, then the file will be expanded to len
 * @arg mode The mode to use for the bitmap.
 * @arg map The output map. Will be initialized.
 * @return 0 on success. Negative on error.
 */
int bitmap_from_filename(char* filename, uint64_t len, int create, int resize, bitmap_mode mode, bloom_bitmap *map) {
    // Get the flags
    int flags = O_RDWR;
    if (create) {
        flags |= O_CREAT;
    }

    // Open the file
    int fileno = open(filename, flags, 0644);
    if (fileno == -1) {
        return -errno;
    }

    // Check if we need to resize
    if (resize) {
        struct stat buf;
        int res = fstat(fileno, &buf);
        if (res != 0) {
            perror("fstat failed on bitmap!");
            return -errno;
        }
        if (buf.st_size < len) {
            res = ftruncate(fileno, len);
            if (res != 0) {
                perror("ftrunctate failed on the bitmap!");
                close(fileno);
                return -errno;
            }
        }
    }

    // Use the filehandler mode
    int res = bitmap_from_file(fileno, len, mode, map);

    // Handle is dup'ed, we can close
    close(fileno);
    return res;
}


/**
 * Flushes the bitmap back to disk. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps.
 * @arg map The bitmap
 * @returns 0 on success, negative failure.
 */
int bitmap_flush(bloom_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return -EINVAL;

    // Do nothing for anonymous maps
    int res;
    if (map->mode == ANONYMOUS || map->mmap == NULL)
        return 0;

    // For SHARED, we can use an msync and let the kernel deal
    else if (map->mode == SHARED) {
        res = msync(map->mmap, map->size, MS_SYNC);
        if (res == -1) return -errno;

    } else if (map->mode == PERSISTENT) {
        if ((res = flush_dirty_pages(map)))
            return res;
    }

    // SHARED / PERSISTENT both have a file backing
    res = fsync(map->fileno);
    if (res == -1) return -errno;
    return 0;
}


/**
 * Flushes all the dirty pages of the bitmap. We just
 * scan the dirty_pages bitfield and flush every 4K
 * block that is considered dirty. As a bit of a jank hack,
 * we always flush the first block, since it contains headers,
 * and is not reliably marked as dirty.
 */
static int flush_dirty_pages(bloom_bitmap *map) {
    uint64_t pages = ceil(map->size / 4096.0);
    unsigned char byte, *dirty_pages = map->dirty_pages;
    int dirty, res;
    for (uint64_t i=0; i < pages; i++) {
        // Check if the page is dirty
        byte = dirty_pages[i >> 3];
        dirty = ((byte >> (7 - (i % 8))) & 0x1);

        if (dirty || i == 0) {
            // Flush the page
            res = flush_page(map, i);
            if (res) return res;

            // Zero out the bit
            byte &= ~(1 << (7 - (i % 8)));
            dirty_pages[i >> 3] = byte;
        }
    }
    return 0;
}


/**
 * Flushes out a single page that is dirty
 */
static int flush_page(bloom_bitmap *map, uint64_t page) {
    int res, total = 0;
    uint64_t offset = page * 4096;
    while (total < 4096) {
        res = pwrite(map->fileno, map->mmap + offset + total,
                4096 - total, offset + total);
        if (res == -1 && errno != EINTR)
            return -errno;
        else
            total += res;
    }
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
int bitmap_close(bloom_bitmap *map) {
    // Return if there is no map provided
    if (map == NULL) return -EINVAL;

    // Flush first
    int res = bitmap_flush(map);
    if (res != 0) return res;

    // Unmap the file
    res = munmap(map->mmap, map->size);
    if (res != 0) return -errno;

    // Close the file descriptor if file backed
    if (map->mode != ANONYMOUS) {
       res = close(map->fileno);
       if (res != 0) return -errno;
    }

    // Remove the dirty bitfield if any
    if (map->dirty_pages) {
        free(map->dirty_pages);
        map->dirty_pages = NULL;
    }

    // Cleanup
    map->mmap = NULL;
    map->fileno = -1;
    return 0;
}

