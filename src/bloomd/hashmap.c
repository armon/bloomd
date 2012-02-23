#include "hashmap.h"

// Basic hash entry.
typedef struct hashmap_entry {
    char *key;
    void *value;
    struct hashmap_entry *next; // Support linking.
} hashmap_entry;

struct bloom_hashmap {
    int count;      // Number of entries
    int table_size; // Size of table in nodes
    int max_size;   // Max size before we resize
    hashmap_entry *table[]; // Pointer to an arry of hashmap_entry objects
};

/**
 * Creates a new hashmap and allocates space for it.
 * @arg initial_size The minimim initial size. 0 for default (64).
 * @arg map Output. Set to the address of the map
 * @return 0 on success.
 */
int hashmap_init(int initial_size, bloom_hashmap **map) {
    return 0;
}

/**
 * Destroys a map and cleans up all associated memory
 * @arg map The hashmap to destroy. Frees memory.
 */
int hashmap_destroy(bloom_hashmap *map) {
    return 0;
}

/**
 * Gets a value.
 * @arg key The key to look for
 * @arg key_len The key length
 * @arg value Output. Set to the value of th key.
 * 0 on success. -1 if not found.
 */
int hashmap_get(bloom_hashmap *map, char *key, int key_len, void **value) {
    return 0;
}

/**
 * Puts a key/value pair.
 * @arg key The key to set. This is copied, and a seperate
 * version is owned by the hashmap. The caller the key at will.
 * @notes This method is not thread safe.
 * @arg key_len The key length
 * @arg value The value to set.
 * 0 on success. -1 if not found.
 */
int hashmap_put(bloom_hashmap *map, char *key, int key_len, void *value) {
    return 0;
}

/**
 * Deletes a key/value pair.
 * @notes This method is not thread safe.
 * @arg key The key to delete
 * @arg key_len The key length
 * 0 on success. -1 if not found.
 */
int hashmap_delete(bloom_hashmap *map, char *key, int key_len) {
    return 0;
}

/**
 * Iterates through the key/value pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns 1, then the iteration stops.
 * @arg map The hashmap to iterate over
 * @arg cb The callback function to invoke
 * @return 0 on success
 */
int hashmap_iter(bloom_hashmap *map, hashmap_callback cb) {
    return 0;
}

