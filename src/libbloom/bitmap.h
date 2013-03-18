#ifndef BLOOM_BITMAP_H
#define BLOOM_BITMAP_H
#include <stddef.h>
#include <stdlib.h>
#include <inttypes.h>

typedef enum {
    SHARED      = 1, // MAP_SHARED mmap used, file backed.
    PERSISTENT  = 2, // MAP_ANONYMOUS used, file backed.
    ANONYMOUS   = 4, // MAP_ANONYMOUS mmap used. No file backing.
    NEW_BITMAP  = 8  // File contents not read. Used with PERSISTENT
} bitmap_mode;

typedef struct {
    bitmap_mode mode;
    int fileno;          // Underlying fileno
    uint64_t size;       // Size of bitmap in bytes
    unsigned char* mmap; // Starting address of the bitmap region
    unsigned char* dirty_pages; // Used for the PERSISTENT mode.
} bloom_bitmap;

/**
 * Returns a bloom_bitmap pointer from a file handle
 * that is already opened with read/write privileges.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg mode The mode to use for the bitmap.
 * @arg map The output map. Will be initialized.
 * @return 0 on success. Negative on error.
 */
int bitmap_from_file(int fileno, uint64_t len, bitmap_mode mode, bloom_bitmap *map);

/**
 * Returns a bloom_bitmap pointer from a filename.
 * Opens the file with read/write privileges. If create
 * is true, then a file will be created if it does not exist.
 * If the file cannot be opened, NULL will be returned.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 * @arg create If 1, then the file will be created if it does not exist.
 * @arg mode The mode to use for the bitmap.
 * @arg map The output map. Will be initialized.
 * @return 0 on success. Negative on error.
 */
int bitmap_from_filename(char* filename, uint64_t len, int create, bitmap_mode mode, bloom_bitmap *map);

/**
 * Flushes the bitmap back to disk. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps.
 * @arg map The bitmap
 * @returns 0 on success, negative failure.
 */
int bitmap_flush(bloom_bitmap *map);

/**
 * * Closes and flushes the bitmap. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps. The caller should free()
 * the structure after.
 * @arg map The bitmap
 * @returns 0 on success, negative on failure.
 */
int bitmap_close(bloom_bitmap *map);

/**
 * Returns the value of the bit at index idx for the
 * bloom_bitmap map
 */
inline int bitmap_getbit(bloom_bitmap *map, uint64_t idx) {
    return (map->mmap[idx >> 3] >> (7 - (idx % 8))) & 0x1;
}

/*
 * Used to set a bit in the bitmap, and as a side affect,
 * mark the page as dirty if we are in the PERSISTENT mode
 */
inline void bitmap_setbit(bloom_bitmap *map, uint64_t idx) {
    unsigned char byte = map->mmap[idx >> 3];
    unsigned char byte_off = 7 - idx % 8;
    byte |= 1 << byte_off;
    map->mmap[idx >> 3] = byte;

    // Check if we need to dirty the page
    if (map->mode == PERSISTENT) {
        // >> 12 for 4096 (bytes/page), >> 3 for 8 (bits/byte)
        uint64_t page = idx >> 15;
        byte = map->dirty_pages[page >> 3];
        byte_off = 7 - page % 8;
        byte |= 1 << byte_off;
        map->dirty_pages[page >> 3] = byte;
    }
}

#endif


