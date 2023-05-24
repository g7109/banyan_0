/**
 * https://github.com/armon/libart.git
 */

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>
#include <brane/util/art.hh>

#ifdef __i386__
#include <emmintrin.h>
#else
#ifdef __amd64__
#include <emmintrin.h>
#endif
#endif

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((art_leaf*)((void*)((uintptr_t)x & ~1)))

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static art_node* alloc_node(node_type type) {
    art_node* n;
    switch (type) {
        case node_type::NODE4:
            n = (art_node*)calloc(1, sizeof(art_node4));
            break;
        case node_type::NODE16:
            n = (art_node*)calloc(1, sizeof(art_node16));
            break;
        case node_type::NODE48:
            n = (art_node*)calloc(1, sizeof(art_node48));
            break;
        case node_type::NODE256:
            n = (art_node*)calloc(1, sizeof(art_node256));
            break;
        default:
            abort();
    }
    n->type = type;
    return n;
}

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t) {
    t->root = NULL;
    t->size = 0;
    return 0;
}

// Recursively destroys the tree
static int destroy_node(art_node *n) {
    // Break if null
    if (!n) return 0;

    // Special case leafs
    if (IS_LEAF(n)) {
        free(LEAF_RAW(n));
        return 1;
    }

    // Handle each node type
    int i, idx;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;

    int destroyed_children = 0;
    switch (n->type) {
        case node_type::NODE4:
            p.p1 = (art_node4*)n;
            for (i = 0; i < n->num_children; i++) {
                destroyed_children += destroy_node(p.p1->children[i]);
            }
            break;

        case node_type::NODE16:
            p.p2 = (art_node16*)n;
            for (i=0;i<n->num_children;i++) {
                destroyed_children += destroy_node(p.p2->children[i]);
            }
            break;

        case node_type::NODE48:
            p.p3 = (art_node48*)n;
            for (i=0;i<256;i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;
                destroyed_children += destroy_node(p.p3->children[idx-1]);
            }
            break;

        case node_type::NODE256:
            p.p4 = (art_node256*)n;
            for (i=0;i<256;i++) {
                if (p.p4->children[i])
                    destroyed_children += destroy_node(p.p4->children[i]);
            }
            break;

        default:
            abort();
    }

    // Free ourself on the way up
    free(n);
    return destroyed_children;
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t) {
    destroy_node(t->root);
    return 0;
}

/**
 * Returns the size of the ART tree.
 */

#ifndef BROKEN_GCC_C99_INLINE
extern inline uint64_t art_size(art_tree *t);
#endif

static art_node** find_child(art_node *n, uint8_t c) {
    int i, mask, bitfield;
    union {
        art_node4 *p1;
        art_node16 *p2;
        art_node48 *p3;
        art_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (art_node4*)n;
            for (i=0 ; i < n->num_children; i++) {
                /* this cast works around a bug in gcc 5.1 when unrolling loops
                 * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=59124
                 */
                if (((uint8_t*)p.p1->keys)[i] == c)
                    return &p.p1->children[i];
            }
            break;

            {
                case NODE16:
                    p.p2 = (art_node16*)n;

                // support non-86 architectures
#ifdef __i386__
                // Compare the key to all 16 stored keys
            __m128i cmp;
            cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                    _mm_loadu_si128((__m128i*)p.p2->keys));

            // Use a mask to ignore children that don't exist
            mask = (1 << n->num_children) - 1;
            bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
                // Compare the key to all 16 stored keys
                __m128i cmp;
                cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                                     _mm_loadu_si128((__m128i*)p.p2->keys));

                // Use a mask to ignore children that don't exist
                mask = (1 << n->num_children) - 1;
                bitfield = _mm_movemask_epi8(cmp) & mask;
#else
                // Compare the key to all 16 stored keys
            bitfield = 0;
            for (i = 0; i < 16; ++i) {
                if (p.p2->keys[i] == c)
                    bitfield |= (1 << i);
            }

            // Use a mask to ignore children that don't exist
            mask = (1 << n->num_children) - 1;
            bitfield &= mask;
#endif
#endif

                /*
                 * If we have a match (any bit set) then we can
                 * return the pointer match using ctz to get
                 * the index.
                 */
                if (bitfield)
                    return &p.p2->children[__builtin_ctz(bitfield)];
                break;
            }

        case NODE48:
            p.p3 = (art_node48*)n;
            i = p.p3->keys[c];
            if (i)
                return &p.p3->children[i-1];
            break;

        case NODE256:
            p.p4 = (art_node256*)n;
            if (p.p4->children[c])
                return &p.p4->children[c];
            break;

        default:
            abort();
    }
    return NULL;
}

// Simple inlined if
static inline uint32_t min(uint32_t a, uint32_t b) {
    return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static uint32_t check_prefix(const art_node *n, const uint8_t *key, uint32_t key_len, uint32_t depth) {
    auto max_cmp = min(min(n->partial_len, MAX_PREFIX_LEN), static_cast<uint32_t>(key_len - depth));
    uint32_t idx;
    for (idx = 0; idx < max_cmp; ++idx) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }
    return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const art_leaf *n, const uint8_t *key, uint32_t key_len, uint32_t depth) {
    (void)depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t)key_len) return 1;

    // Compare the keys starting at the depth
    return memcmp(n->key, key, key_len);
}

/**
 * Checks if a leaf matches
 * @return -1 on disabled, 0 on not found, 1 on found.
 */
static int active_leaf_prefix_matches(const art_leaf *n, const uint8_t *key, uint32_t key_len, uint32_t depth,
                                      void **ret_ptr) {
    (void)depth;
    if(n->value){ // if regular key
        if(!memcmp(n->key, key, min(key_len, n->key_len))){
            // if match exactly
            if(n->key_len == (uint32_t)(key_len)) {
                *ret_ptr = n->value;
                return 1;
            }
                // if match is a prefix
            else if (n->key_len < (uint32_t)(key_len)) {
                *ret_ptr = n->value;
                return 2;
            }
        }
    }
    else{ // if disabled key
        if(n->key_len < (uint32_t)(key_len) && !memcmp(n->key, key, min(key_len, n->key_len))){
            return -1;
        }
    }
    return 0;
}

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_search(const art_tree *t, const uint8_t *key, uint32_t key_len) {
    art_node **child;
    art_node *n = t->root;
    uint32_t prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((art_leaf*)n, key, key_len , depth)) {
                return ((art_leaf*)n)->value;
            }
            return NULL;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return NULL;
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}

// Find the minimum leaf under a node
static art_leaf* minimum(const art_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case node_type::NODE4:
            return minimum(((const art_node4*)n)->children[0]);
        case node_type::NODE16:
            return minimum(((const art_node16*)n)->children[0]);
        case node_type::NODE48:
            idx=0;
            while (!((const art_node48*)n)->keys[idx]) idx++;
            idx = ((const art_node48*)n)->keys[idx] - 1;
            return minimum(((const art_node48*)n)->children[idx]);
        case node_type::NODE256:
            idx=0;
            while (!((const art_node256*)n)->children[idx]) idx++;
            return minimum(((const art_node256*)n)->children[idx]);
        default:
            abort();
    }
}

// Find the maximum leaf under a node
static art_leaf* maximum(const art_node *n) {
    // Handle base cases
    if (!n) {
        return NULL;
    }
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int idx;
    switch (n->type) {
        case node_type::NODE4:
            return maximum(((const art_node4*)n)->children[n->num_children-1]);
        case node_type::NODE16:
            return maximum(((const art_node16*)n)->children[n->num_children-1]);
        case node_type::NODE48:
            idx=255;
            while (!((const art_node48*)n)->keys[idx]) idx--;
            idx = ((const art_node48*)n)->keys[idx] - 1;
            return maximum(((const art_node48*)n)->children[idx]);
        case node_type::NODE256:
            idx=255;
            while (!((const art_node256*)n)->children[idx]) idx--;
            return maximum(((const art_node256*)n)->children[idx]);
        default:
            abort();
    }
}

/**
 * Returns the minimum valued leaf
 */
art_leaf* art_minimum(art_tree *t) {
    return minimum((art_node*)t->root);
}

/**
 * Returns the maximum valued leaf
 */
art_leaf* art_maximum(art_tree *t) {
    return maximum((art_node*)t->root);
}

static art_leaf* make_leaf(const uint8_t *key, uint32_t key_len, void *value) {
    auto *l = (art_leaf*)calloc(1, sizeof(art_leaf)+key_len);
    l->value = value;
    l->key_len = key_len;
    memcpy(l->key, key, key_len);
    return l;
}

static uint32_t longest_common_prefix(art_leaf *l1, art_leaf *l2, uint32_t depth) {
    uint32_t max_cmp = min(l1->key_len, l2->key_len) - depth;
    uint32_t idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (l1->key[depth+idx] != l2->key[depth+idx])
            return idx;
    }
    return idx;
}

static void copy_header(art_node *dest, art_node *src) {
    dest->num_children = src->num_children;
    dest->partial_len = src->partial_len;
    memcpy(dest->partial, src->partial, min(MAX_PREFIX_LEN, src->partial_len));
}

static void add_child256(art_node256 *n, art_node **ref, uint8_t c, void *child) {
    (void)ref;
    n->n.num_children++;
    n->children[c] = (art_node*)child;
}

static void add_child48(art_node48 *n, art_node **ref, uint8_t c, void *child) {
    if (n->n.num_children < 48) {
        int pos = 0;
        while (n->children[pos]) pos++;
        n->children[pos] = (art_node*)child;
        n->keys[c] = pos + 1;
        n->n.num_children++;
    } else {
        auto *new_node = (art_node256*)alloc_node(node_type::NODE256);
        for (int i=0;i<256;i++) {
            if (n->keys[i]) {
                new_node->children[i] = n->children[n->keys[i] - 1];
            }
        }
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free(n);
        add_child256(new_node, ref, c, child);
    }
}

static void add_child16(art_node16 *n, art_node **ref, uint8_t c, void *child) {
    if (n->n.num_children < 16) {
        unsigned mask = (1 << n->n.num_children) - 1;

        // support non-x86 architectures
#ifdef __i386__
        __m128i cmp;

            // Compare the key to all 16 stored keys
            cmp = _mm_cmplt_epi8(_mm_set1_epi8(c),
                    _mm_loadu_si128((__m128i*)n->keys));

            // Use a mask to ignore children that don't exist
            unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
#ifdef __amd64__
        __m128i cmp;

        // Compare the key to all 16 stored keys
        cmp = _mm_cmplt_epi8(_mm_set1_epi8(c),
                             _mm_loadu_si128((__m128i*)n->keys));

        // Use a mask to ignore children that don't exist
        unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
#else
        // Compare the key to all 16 stored keys
            unsigned bitfield = 0;
            for (short i = 0; i < 16; ++i) {
                if (c < n->keys[i])
                    bitfield |= (1 << i);
            }

            // Use a mask to ignore children that don't exist
            bitfield &= mask;
#endif
#endif

        // Check if less than any
        unsigned idx;
        if (bitfield) {
            idx = __builtin_ctz(bitfield);
            memmove(n->keys+idx+1,n->keys+idx,n->n.num_children-idx);
            memmove(n->children+idx+1,n->children+idx,
                    (n->n.num_children-idx)*sizeof(void*));
        } else
            idx = n->n.num_children;

        // Set the child
        n->keys[idx] = c;
        n->children[idx] = (art_node*)child;
        n->n.num_children++;

    } else {
        art_node48 *new_node = (art_node48*)alloc_node(node_type::NODE48);

        // Copy the child pointers and populate the key map
        memcpy(new_node->children, n->children,
               sizeof(void*)*n->n.num_children);
        for (int i=0;i<n->n.num_children;i++) {
            new_node->keys[n->keys[i]] = i + 1;
        }
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free(n);
        add_child48(new_node, ref, c, child);
    }
}

static void add_child4(art_node4 *n, art_node **ref, uint8_t c, void *child) {
    if (n->n.num_children < 4) {
        int idx;
        for (idx=0; idx < n->n.num_children; idx++) {
            if (c < n->keys[idx]) break;
        }

        // Shift to make room
        memmove(n->keys+idx+1, n->keys+idx, n->n.num_children - idx);
        memmove(n->children+idx+1, n->children+idx,
                (n->n.num_children - idx)*sizeof(void*));

        // Insert element
        n->keys[idx] = c;
        n->children[idx] = (art_node*)child;
        n->n.num_children++;

    } else {
        art_node16 *new_node = (art_node16*)alloc_node(node_type::NODE16);

        // Copy the child pointers and the key map
        memcpy(new_node->children, n->children,
               sizeof(void*)*n->n.num_children);
        memcpy(new_node->keys, n->keys,
               sizeof(uint8_t)*n->n.num_children);
        copy_header((art_node*)new_node, (art_node*)n);
        *ref = (art_node*)new_node;
        free(n);
        add_child16(new_node, ref, c, child);
    }
}

static void add_child(art_node *n, art_node **ref, uint8_t c, void *child) {
    switch (n->type) {
        case node_type::NODE4:
            return add_child4((art_node4*)n, ref, c, child);
        case node_type::NODE16:
            return add_child16((art_node16*)n, ref, c, child);
        case node_type::NODE48:
            return add_child48((art_node48*)n, ref, c, child);
        case node_type::NODE256:
            return add_child256((art_node256*)n, ref, c, child);
        default:
            abort();
    }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static uint32_t prefix_mismatch(const art_node *n, const uint8_t *key, uint32_t key_len, uint32_t depth) {
    auto max_cmp = static_cast<uint32_t>(min(min(MAX_PREFIX_LEN, n->partial_len), key_len - depth));
    uint32_t idx;
    for (idx=0; idx < max_cmp; idx++) {
        if (n->partial[idx] != key[depth+idx])
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->partial_len > MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a leaf
        art_leaf *l = minimum(n);
        max_cmp = min(l->key_len, key_len)- depth;
        for (; idx < max_cmp; idx++) {
            if (l->key[idx+depth] != key[depth+idx])
                return idx;
        }
    }
    return idx;
}

static void* recursive_insert(art_node *n, art_node **ref, const uint8_t *key, uint32_t key_len, void *value, uint32_t depth, int *old, art_tree* tree, int active_index) {
    // If we are at a NULL node, inject a leaf
    if (!n) {
        *ref = (art_node*)SET_LEAF(make_leaf(key, key_len, value));
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);

        // Check if we are updating an existing value
        if (!leaf_matches(l, key, key_len, depth)) {
            *old = 1;
            void *old_val = l->value;
            l->value = value;
            return old_val;
        }

        // New value, we must split the leaf into a node4
        art_node4 *new_node = (art_node4*)alloc_node(node_type::NODE4);

        // Create a new leaf
        art_leaf *l2 = make_leaf(key, key_len, value);

        // Determine longest prefix
        uint32_t longest_prefix = longest_common_prefix(l, l2, depth);
        new_node->n.partial_len = longest_prefix;
        memcpy(new_node->n.partial, key+depth,
               static_cast<size_t>(min(MAX_PREFIX_LEN, longest_prefix)));

        // Add the leafs to the new node4
        *ref = (art_node*)new_node;

        add_child4(new_node, ref, l->key[depth + longest_prefix], SET_LEAF(l));
        add_child4(new_node, ref, l2->key[depth + longest_prefix], SET_LEAF(l2));
        return NULL;
    }

    // Check if given node has a prefix
    if (n->partial_len) {
        // Determine if the prefixes differ, since we need to split
        auto prefix_diff = prefix_mismatch(n, key, key_len, depth);
        if (prefix_diff >= n->partial_len) {
            depth += n->partial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node
        auto *new_node = (art_node4*)alloc_node(node_type::NODE4);
        *ref = (art_node*)new_node;
        new_node->n.partial_len = prefix_diff;
        memcpy(new_node->n.partial, n->partial,
               static_cast<size_t>(min(MAX_PREFIX_LEN, prefix_diff)));

        // Adjust the prefix of the old node
        if (n->partial_len <= MAX_PREFIX_LEN) {
            add_child4(new_node, ref, n->partial[prefix_diff], n);
            n->partial_len -= (prefix_diff+1);
            memmove(n->partial, n->partial+prefix_diff+1,
                    static_cast<size_t>(min(MAX_PREFIX_LEN, n->partial_len)));
        } else {
            n->partial_len -= (prefix_diff+1);
            art_leaf *l = minimum(n);
            add_child4(new_node, ref, l->key[depth+prefix_diff], n);
            memcpy(n->partial, l->key+depth+prefix_diff+1,
                   static_cast<size_t>(min(MAX_PREFIX_LEN, n->partial_len)));
        }

        // Insert the new leaf
        art_leaf *l = make_leaf(key, key_len, value);
        add_child4(new_node, ref, key[depth+prefix_diff], SET_LEAF(l));
        return NULL;
    }

    RECURSE_SEARCH:;

    // Find a child to recurse to
    art_node **child = find_child(n, key[depth]);
    if (child) {
        return recursive_insert(*child, child, key, key_len, value, depth+1, old, tree, active_index);
    }

    // No child, node goes within us
    art_leaf *l = make_leaf(key, key_len, value);
    add_child(n, ref, key[depth], SET_LEAF(l));
    return NULL;
}

/**
 * Checks if a leaf matches
 * @return 1 on success, 0 on not found, -1 on already disabled
 */
static int leaf_disable_prefix(art_tree* tree, art_leaf *n, art_node **ref, const uint8_t *key, uint32_t key_len, uint32_t depth) {
    (void) depth;
    uint32_t max_cmp = min(n->key_len, key_len) - depth;
    uint32_t idx;
    for (idx = 0; idx < max_cmp; idx++) {
        if (n->key[depth + idx] != key[depth + idx])
            break;
    }
    if (idx == key_len - depth) {
        // no need to add new node, disable current leaf
        // key is longer than the disabled portion
        n->key_len = key_len;
        n->value = NULL;
        tree->size--;
    } else {
        // if target key has a prefix that is disabled
        if ((uint32_t) idx == n->key_len - depth && !n->value) return -1;
        art_node4 *new_node = (art_node4 *) alloc_node(node_type::NODE4);
        art_leaf *l2 = make_leaf(key, key_len, NULL);
        new_node->n.partial_len = idx;
        memcpy(new_node->n.partial, key + depth, static_cast<size_t>(min(MAX_PREFIX_LEN, idx)));

        // Add the leafs to the new node4
        *ref = (art_node *) new_node;

        add_child4(new_node, ref, n->key[depth + idx], SET_LEAF(n));
        add_child4(new_node, ref, l2->key[depth + idx], SET_LEAF(l2)); // key_len-depth cant be idx
    }

    return 1;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* art_insert(art_tree *t, const uint8_t *key, uint32_t key_len, void *value) {
    int old_val = 0;
    void *old = recursive_insert(t->root, &t->root, key, key_len, value, 0, &old_val, t, 0);
    if (!old_val) t->size++;
    return old;
}

static void remove_child256(art_node256 *n, art_node **ref, uint8_t c) {
    n->children[c] = NULL;
    n->n.num_children--;

    // Resize to a node48 on underflow, not immediately to prevent
    // trashing if we sit on the 48/49 boundary
    if (n->n.num_children == 37) {
        art_node48 *new_node = (art_node48*)alloc_node(NODE48);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);

        int pos = 0;
        for (int i=0;i<256;i++) {
            if (n->children[i]) {
                new_node->children[pos] = n->children[i];
                new_node->keys[i] = pos + 1;
                pos++;
            }
        }
        free(n);
    }
}

static void remove_child48(art_node48 *n, art_node **ref, uint8_t c) {
    int pos = n->keys[c];

    n->keys[c] = 0;
    n->children[pos-1] = NULL;
    n->n.num_children--;

    if (n->n.num_children == 12) {
        art_node16 *new_node = (art_node16*)alloc_node(NODE16);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);

        int child = 0;
        for (int i=0;i<256;i++) {
            pos = n->keys[i];
            if (pos) {
                new_node->keys[child] = i;
                new_node->children[child] = n->children[pos - 1];
                child++;
            }
        }
        free(n);
    }
}

static void remove_child16(art_node16 *n, art_node **ref, art_node **l) {
    int pos = l - n->children;

    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    if (n->n.num_children == 3) {
        art_node4 *new_node = (art_node4*)alloc_node(node_type::NODE4);
        *ref = (art_node*)new_node;
        copy_header((art_node*)new_node, (art_node*)n);
        memcpy(new_node->keys, n->keys, 4);
        memcpy(new_node->children, n->children, 4*sizeof(void*));
        free(n);
    }
}

static void remove_child4(art_node4 *n, art_node **ref, art_node **l) {
    auto pos = l - n->children;

    memmove(n->keys+pos, n->keys+pos+1, n->n.num_children - 1 - pos);
    memmove(n->children+pos, n->children+pos+1, (n->n.num_children - 1 - pos)*sizeof(void*));
    n->n.num_children--;

    // Remove nodes with only a single child
    if (n->n.num_children == 1) {
        art_node *child = n->children[0];
        if (!IS_LEAF(child)) {
            // Concatenate the prefixes
            auto prefix = n->n.partial_len;
            if (prefix < MAX_PREFIX_LEN) {
                n->n.partial[prefix] = n->keys[0];
                prefix++;
            }
            if (prefix < MAX_PREFIX_LEN) {
                auto sub_prefix = min(child->partial_len,
                                      static_cast<uint32_t>(MAX_PREFIX_LEN - prefix));
                memcpy(n->n.partial+prefix, child->partial, sub_prefix);
                prefix += sub_prefix;
            }

            // Store the prefix in the child
            memcpy(child->partial, n->n.partial, min(prefix, MAX_PREFIX_LEN));
            child->partial_len += n->n.partial_len + 1;

        }

        *ref = child;
        free(n);
    }
}

static void remove_child(art_node *n, art_node **ref, uint8_t c, art_node **l) {
    switch (n->type) {
        case node_type::NODE4:
            return remove_child4((art_node4*)n, ref, l);
        case node_type::NODE16:
            return remove_child16((art_node16*)n, ref, l);
        case node_type::NODE48:
            return remove_child48((art_node48*)n, ref, c);
        case node_type::NODE256:
            return remove_child256((art_node256*)n, ref, c);
        default:
            abort();
    }
}

static art_leaf* recursive_delete(art_node *n, art_node **ref, const uint8_t *key, uint32_t key_len, uint32_t depth) {
    // Search terminated
    if (!n) return NULL;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            return l;
        }
        return NULL;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        auto prefix_len = check_prefix(n, key, key_len, depth);
        if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
            return NULL;
        }
        depth = depth + n->partial_len;
    }

    // Find child node
    art_node **child = find_child(n, key[depth]);
    if (!child) return NULL;

    // If the child is leaf, delete from this node
    if (IS_LEAF(*child)) {
        art_leaf *l = LEAF_RAW(*child);
        if (!leaf_matches(l, key, key_len, depth)) {
            remove_child(n, ref, key[depth], child);
            return l;
        }
        return NULL;

        // Recurse
    } else {
        return recursive_delete(*child, child, key, key_len, depth+1);
    }
}

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* art_delete(art_tree *t, const uint8_t *key, uint32_t key_len) {
    // does not force disabled tag deletion
    art_leaf *l = recursive_delete(t->root, &t->root, key, key_len, 0);
    if (l) {
        t->size--;
        void *old = l->value;
        free(l);
        return old;
    }
    return NULL;
}

/**
 * Disable a prefix and all its descendants in the ART tree
 * @arg t The tree
 * @arg prefix The prefix
 * @arg prefix_len The length of the prefix
 * @return 1 on success, 0 on not found, -1 on already disabled
 */
int art_disable(art_tree *t, const uint8_t *key, uint32_t key_len) {
    art_node *n = t->root;
    art_node **ref = &(t->root);
    uint32_t depth = 0;
    // Search terminated
    if (!n) return 1;

    art_node **child;
    while (n) {
        if (depth == key_len) {
            // if prefix found at byte barrier, child needs to go no matter what
            art_node *to_remove = n;
            // remove child and all subtree with disabled tags and everything
            int ret = destroy_node(to_remove);
            // add the leaf
            art_leaf *l = make_leaf(key, key_len, NULL);
            // overwrite exiting child with the leaf
            *ref = (art_node *) SET_LEAF(l);
            t->size -= (ret-1);
            return 1;
        }

        if (IS_LEAF(n)) {
            // if prefix needs to be resolved at a leaf
            art_node *l = (art_node *) LEAF_RAW(n);
            return leaf_disable_prefix(t, (art_leaf *) l, ref, key, key_len, depth);
        }

        if (n->partial_len) {
            // if prefix needs to be resolved at a non leaf child with partial prefix
            auto prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if (prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }
            auto remain_key_len = static_cast<uint32_t>(key_len - depth);

            if (remain_key_len == prefix_len) {
                // if prefix found at byte barrier, child needs to go no matter what
                art_node *to_remove = n;
                // remove child and all subtree with disabled tags and everything
                int ret = destroy_node(to_remove);
                // add the leaf
                art_leaf *l = make_leaf(key, key_len, NULL);
                // overwrite exiting child with the leaf
                *ref = (art_node *) SET_LEAF(l);
                t->size -= ret;
                return 1;
            }

            // If there is no match, spawn new node
            if (prefix_len < remain_key_len && (prefix_len < n->partial_len)) {
                // Create a new node
                art_node4 *new_node = (art_node4 *) alloc_node(node_type::NODE4);
                // *ref = (art_node*)new_node;
                *ref = (art_node *) new_node;
                new_node->n.partial_len = prefix_len;
                memcpy(new_node->n.partial, n->partial, min(MAX_PREFIX_LEN, prefix_len));

                // Adjust the prefix of the old node
                if (n->partial_len <= MAX_PREFIX_LEN) {
                    add_child4(new_node, ref, n->partial[prefix_len], n);
                    n->partial_len -= (prefix_len + 1);
                    memmove(n->partial, n->partial + prefix_len + 1,
                            min(MAX_PREFIX_LEN, n->partial_len));
                } else {
                    n->partial_len -= (prefix_len + 1);
                    art_leaf *l = minimum(n);
                    add_child4(new_node, ref, l->key[depth + prefix_len], n);
                    memcpy((n)->partial, l->key + depth + prefix_len + 1,
                           min(MAX_PREFIX_LEN, (n)->partial_len));
                }

                // Insert the new leaf
                art_leaf *l = make_leaf(key, key_len, NULL);
                add_child4(new_node, ref, key[depth + prefix_len], SET_LEAF(l));

                return 1;
                // If we've matched the prefix, iterate on this node
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Find child node
        child = find_child(n, key[depth]);
        if (!child) break;

        n = *child;
        ref = child;
        depth++;
    }

    if (depth <= key_len) {
        // add the leaf
        art_leaf *l = make_leaf(key, key_len, NULL);
        // overwrite exiting child with the leaf
        add_child(n, ref, key[depth], SET_LEAF(l));
        return 1;
    }
    return 0;
}

/**
 * Lookup a key and return corresponding pointer
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg ret_ptr pointer used to store return value
 * @return -1 on disabled, 0 on not found, 1 on found
 */
int art_lookup(const art_tree* t, const uint8_t* key, uint32_t key_len, void** ret_ptr){
    art_node **child;
    art_node *n = t->root;
    uint32_t prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            return active_leaf_prefix_matches((art_leaf *) n, key, key_len, depth, ret_ptr);
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = check_prefix(n, key, key_len, depth);
            if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len))
                return 0;
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        if (child) {
            n=*child;
        }
        else {
            return 0;
        }
        depth++;
    }
    return 0;
}

// Recursively iterates over the tree
static int recursive_iter(art_node *n, art_callback cb, void *data) {
    // Handle base cases
    if (!n) return 0;
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        return cb(data, (const uint8_t*)l->key, l->key_len, l->value);
    }

    int idx, res;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node4*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                res = recursive_iter(((art_node16*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;

                res = recursive_iter(((art_node48*)n)->children[idx-1], cb, data);
                if (res) return res;
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                res = recursive_iter(((art_node256*)n)->children[i], cb, data);
                if (res) return res;
            }
            break;

        default:
            abort();
    }
    return 0;
}

// Recursively iterates over the tree
static void recursive_iter_active(art_node *n, art_prefix_callback cb, void *data) {
    // Handle base cases
    if (!n) return;
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if(l->value){
            cb(data, l->value);
        }
        return;
    }

    int idx;
    switch (n->type) {
        case NODE4:
            for (int i=0; i < n->num_children; i++) {
                recursive_iter_active(((art_node4*)n)->children[i], cb, data);
            }
            break;

        case NODE16:
            for (int i=0; i < n->num_children; i++) {
                recursive_iter_active(((art_node16*)n)->children[i], cb, data);
            }
            break;

        case NODE48:
            for (int i=0; i < 256; i++) {
                idx = ((art_node48*)n)->keys[i];
                if (!idx) continue;

                recursive_iter_active(((art_node48*)n)->children[idx-1], cb, data);
            }
            break;

        case NODE256:
            for (int i=0; i < 256; i++) {
                if (!((art_node256*)n)->children[i]) continue;
                recursive_iter_active(((art_node256*)n)->children[i], cb, data);
            }
            break;

        default:
            abort();
    }
}

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
int art_iter(art_tree *t, art_callback cb, void *data) {
    return recursive_iter(t->root, cb, data);
}

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
void art_iter_active(art_tree *t, art_prefix_callback cb, void *data) {
    recursive_iter_active(t->root, cb, data);
}


/**
 * Checks if a leaf prefix matches
 * @return 0 on success.
 */
static int leaf_prefix_matches(const art_leaf *n, const uint8_t *prefix, uint32_t prefix_len) {
    // Fail if the key length is too short
    if (n->key_len < prefix_len) return 1;

    // Compare the keys
    return memcmp(n->key, prefix, prefix_len);
}

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
int art_iter_prefix(art_tree *t, const uint8_t *key, uint32_t key_len, art_callback cb, void *data) {
    art_node **child;
    art_node *n = t->root;
    uint32_t prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
                art_leaf *l = (art_leaf*)n;
                return cb(data, (const uint8_t*)l->key, l->key_len, l->value);
            }
            return 0;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n);
            if (!leaf_prefix_matches(l, key, key_len))
                return recursive_iter(n, cb, data);
            return 0;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if ((uint32_t)prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return 0;

                // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                return recursive_iter(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
    return 0;
}

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
void art_iter_active_prefix(art_tree *t, const uint8_t *key, uint32_t key_len, art_prefix_callback cb, void *data) {
    art_node **child;
    art_node *n = t->root;
    uint32_t prefix_len, depth = 0;
    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (art_node*)LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_prefix_matches((art_leaf*)n, key, key_len)) {
                art_leaf *l = (art_leaf*)n;
                if(l->value){
                    cb(data, l->value);
                }
            }
            return;
        }

        // If the depth matches the prefix, we need to handle this node
        if (depth == key_len) {
            art_leaf *l = minimum(n);
            if (!leaf_prefix_matches(l, key, key_len))
                recursive_iter_active(n, cb, data);
            return;
        }

        // Bail if the prefix does not match
        if (n->partial_len) {
            prefix_len = prefix_mismatch(n, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if ((uint32_t)prefix_len > n->partial_len) {
                prefix_len = n->partial_len;
            }

            // If there is no match, search is terminated
            if (!prefix_len) {
                return;

                // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                return recursive_iter_active(n, cb, data);
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        // Recursively search
        child = find_child(n, key[depth]);
        n = (child) ? *child : NULL;
        depth++;
    }
}

/**
 * Deletes a prefix and all its sub-tree from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */

int art_delete_prefix(art_tree *t, const uint8_t *key, uint32_t key_len, uint32_t depth) {
    art_node* n = t->root;
    art_node **ref = &(t->root);
    // Search terminated
    if (!n) return 0;

    // Handle hitting a leaf node
    if (IS_LEAF(n)) {
        art_leaf *l = LEAF_RAW(n);
        if (!leaf_matches(l, key, key_len, depth)) {
            *ref = NULL;
            t->size--;
            return 1;
        }
        return 0;
    }

    // Bail if the prefix does not match
    if (n->partial_len) {
        auto prefix_len = check_prefix(n, key, key_len, depth);
        if (prefix_len != min(MAX_PREFIX_LEN, n->partial_len)) {
            return 0;
        }
        depth = depth + n->partial_len;
    }

    while(n){
        // Find child node
        art_node **child = find_child(n, key[depth]);
        if (!child) return 0;

        if(depth + 1 == key_len) {
            // if prefix found at byte barrier, child needs to go no matter what
            art_node* to_remove = *child;
            // remove child and all subtree with disabled tags and everything
            remove_child(n, ref, key[depth], child);
            int ret = destroy_node(to_remove);
            t->size -= ret;
            return ret;
        }

        if (IS_LEAF(*child)) {
            // if prefix needs to be resolved at a leaf
            art_node* l = (art_node*)LEAF_RAW(*child);
            if (!leaf_prefix_matches((art_leaf *) l, key, key_len)) {
                art_node* to_remove = *child;
                // remove child and all subtree with disabled tags and everything
                remove_child(n, ref, key[depth], child);
                int ret = destroy_node(to_remove);
                t->size -= ret;
                return ret;
            }
        }

        if ((*child)->partial_len) {
            // if prefix needs to be resolved at a non leaf child with partial prefix
            auto prefix_len = prefix_mismatch(*child, key, key_len, depth);

            // Guard if the mis-match is longer than the MAX_PREFIX_LEN
            if ((uint32_t)prefix_len > (*child)->partial_len) {
                prefix_len = (*child)->partial_len;
            }

            // If there is no match, search is terminated
            if ( prefix_len < static_cast<uint32_t>(key_len - depth)) {
                return 0;
                // If we've matched the prefix, iterate on this node
            } else if (depth + prefix_len == key_len) {
                // return recursive_iter(n, cb, data);
                art_node* to_remove = *child;
                // remove child but keep disabled tag
                remove_child(n, ref, key[depth], child);
                int ret = destroy_node(to_remove);
                t->size -= ret;
                return ret;
            }

            // if there is a full match, go deeper
            depth = depth + n->partial_len;
        }

        depth++;
        n = *child;
        ref = child;
    }
    return 0;
}
