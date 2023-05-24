/**
 * https://github.com/armon/libart.git
 */

#include <stdint.h>
#ifndef ART_H
#define ART_H

#ifdef __cplusplus
extern "C" {
#endif

enum node_type : uint8_t {
    NODE4 = 0x01,
    NODE16 = 0x02,
    NODE48 = 0x04,
    NODE256 = 0x08,
};

const uint32_t MAX_PREFIX_LEN = 10;

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

typedef int(*art_callback)(void *data, const uint8_t *key, uint32_t key_len, void *value);
typedef void(*art_prefix_callback)(void *data, void *value);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    node_type type;
    uint8_t num_children;
    uint32_t partial_len;
    uint8_t partial[MAX_PREFIX_LEN];
} art_node;

/**
 * Small node with only 4 children
 */
typedef struct {
    art_node n;
    uint8_t keys[4];
    art_node *children[4];
} art_node4;

/**
 * Node with 16 children
 */
typedef struct {
    art_node n;
    uint8_t keys[16];
    art_node *children[16];
} art_node16;

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct {
    art_node n;
    uint8_t keys[256];
    art_node *children[48];
} art_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    art_node n;
    art_node *children[256];
} art_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;
    uint8_t key[];
} art_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    art_node *root;
    uint64_t size;
} art_tree;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_art_tree(...) art_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_art_tree(...) art_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.
 */
#ifdef BROKEN_GCC_C99_INLINE
# define art_size(t) ((t)->size)
#else
inline uint64_t art_size(art_tree *t) {
    return t->size;
}
#endif

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const uint8_t *key, uint32_t key_len, void *value);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const uint8_t *key, uint32_t key_len);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg depth The current depth of the lookup
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
int art_delete_prefix(art_tree *t, const uint8_t *key, uint32_t key_len, uint32_t depth);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const uint8_t *key, uint32_t key_len);

/**
 * Disable a prefix and all its descendants in the ART tree
 * @arg t The tree
 * @arg prefix The prefix
 * @arg prefix_len The length of the prefix
 * @@return 1 on success, 0 on not found, -1 on already disabled
 */
int art_disable(art_tree *t, const uint8_t *key, uint32_t key_len);

/**
 * Lookup a key and return corresponding pointer
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg ret_ptr pointer used to store return value
 * @return 0 on success, 1 on not found or disabled
 */
int art_lookup(const art_tree* t, const uint8_t* key, uint32_t key_len, void** ret_ptr);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
art_leaf* art_minimum(art_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
art_leaf* art_maximum(art_tree *t);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art_tree *t, art_callback cb, void *data);

/**
 * Iterates through the active entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
void art_iter_active(art_tree *t, art_prefix_callback cb, void *data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art_tree *t, const uint8_t *prefix, int prefix_len, art_callback cb, void *data);

/**
 * Iterates through the active entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
void art_iter_active_prefix(art_tree *t, const uint8_t *key, uint32_t key_len, art_prefix_callback cb, void *data);

#ifdef __cplusplus
}
#endif

#endif
