//
// Created by le.xl on 2019/6/26.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <brane/util/art.hh>

namespace brane {

template<class valueType>
class radix_tree_index {
    art_tree *_tree;
public:
    radix_tree_index() : _tree(new art_tree()) {
        if (art_tree_init(_tree) != 0) {
            throw ("Art tree initiliaztion failed!");
        }
    }

    ~radix_tree_index() {
        art_tree_destroy(_tree);
        delete _tree;
    }

    void remove_key(const uint8_t *key, const uint32_t key_length) {
        art_delete(_tree, key, key_length);
    }

    int remove_prefix(const uint8_t *key, const uint32_t key_length) {
        return art_delete_prefix(_tree, key, key_length, 0);
    }

    valueType get(const uint8_t *key, const uint32_t key_length) {
        return (valueType) art_search(_tree, key, key_length);
    }

    void insert(const uint8_t *key, const valueType value, const uint32_t key_length) {
        art_insert(_tree, key, key_length, (void *) value);
    }

    bool contains(const uint8_t *key, const uint32_t key_length) {
        return art_search(_tree, key, key_length) != nullptr;
    }

    int disable(const uint8_t *key, const uint32_t key_length) {
        return art_disable(_tree, key, key_length);
    }

    int lookup(const uint8_t *key, void **ret_ptr, const uint32_t key_length) {
        return art_lookup(_tree, key, key_length, ret_ptr);
    }

    void iter_prefix_apply(const uint8_t* key, const uint32_t key_length, void (*apply)(void*, void*), void* data) {
        return art_iter_active_prefix(_tree, key, key_length, apply, data);
    }

    void iter_prefix_apply(void (*apply)(void* data, void* target), void* data) {
        return art_iter_active(_tree, apply, data);
    }

    uint64_t get_size() {
        return _tree->size;
    }
};

} // namespace brane
