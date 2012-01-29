#ifndef CBLOOM_BITMAP_H
#define CBLOOM_BITMAP_H
#include <stddef.h>
#include <stdlib.h>

typedef enum {
    SHARED,         // MAP_SHARED mmap used. File backed.
    ANONYMOUS       // MAP_ANONYMOUS mmap used. No file backing.
} cbloom_flags;

typedef struct {
    size_t size;  // Size of bitmap in bytes
    cbloom_flags flags;    // Bitmap flags.
    int fileno;   // Underlying fileno for the mmap
    unsigned char* mmap;   // Starting address of the mem-map region
} cbloom_bitmap;

/**
 * Returns a cbloom_bitmap pointer from a file handle
 * that is already opened with read/write privileges.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 */
cbloom_bitmap *bitmapFromFile(int fileno, size_t len);

/**
 * Returns a cbloom_bitmap pointer from a filename.
 * Opens the file with read/write privileges. If create
 * is true, then a file will be created if it does not exist.
 * If the file cannot be opened, NULL will be returned.
 * @arg fileno The fileno
 * @arg len The length of the bitmap in bytes.
 */
cbloom_bitmap *bitmapFromFilename(char* filename, size_t len, int create);

/**
 * Returns the size of the bitmap in bits.
 * @arg map The bitmap
 */
size_t bitmapBitsize(cbloom_bitmap *map);

/**
 * Flushes the bitmap back to disk. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps.
 * @arg map The bitmap
 * @returns 1 on success, 0 on failure.
 */
int bitmapFlush(cbloom_bitmap *map);

/**
 * Closes and flushes the bitmap. This is
 * a syncronous operation. It is a no-op for
 * ANONYMOUS bitmaps.
 * @arg map The bitmap
 * @returns 1 on success, 0 on failure.
 */
int bitmapClose(cbloom_bitmap *map);

#define BITMAP_GETBIT(map, idx) {                        \
            unsigned char byte = map->mmap[idx >> 3];    \
            unsigned char byte_off = 7 - idx % 8;        \
            return (byte >> byte_off) & 0x1;             \
        }                                                \

#define BITMAP_SETBIT(map, idx, val) {                   \
            unsigned char byte = map->mmap[idx >> 3];    \
            unsigned char byte_off = 7 - idx % 8;        \
            if (val) {                                   \
                byte |= 1 << byte_off;                   \
            } else {                                     \
                byte &= ~(1 << byte_off);                \
            }                                            \
            map->mmap[idx >> 3] = byte;                  \
            return byte;                                 \
        }                                                \

#endif
