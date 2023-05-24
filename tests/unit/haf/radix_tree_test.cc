//
// Created by le.xl on 2019/6/27.
//

#define BOOST_TEST_MAIN

#include <boost/test/included/unit_test.hpp>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <algorithm>
#include <array>
#include <random>
#include <assert.h>
#include <iostream>
#include <seastar/haf/util/radix_tree_index.hh>

using namespace seastar;

BOOST_AUTO_TEST_SUITE(test_radix_specialized)

void get_randoms(int size, uint64_t *random_list) {
	std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
	std::uniform_int_distribution<uint64_t> dis(2, UINT64_MAX);

	for (int i = 0; i < size; i++) {
		random_list[i] = dis(gen);
	}
}

struct uint64_key {
	uint8_t *ptr;

	uint64_key(uint64_t key) : ptr(new uint8_t[8]) {
		reinterpret_cast<uint64_t *>(ptr)[0] = __builtin_bswap64(key);
	}

	~uint64_key() { delete[] ptr; }
};


BOOST_AUTO_TEST_CASE(test_radix_tree_specialized) {
	radix_tree_index<uint64_t *> *index = new radix_tree_index<uint64_t *>();

	int num_key_base = 10;
	int num_keys_per_base = 10;
	uint64_t key_arr[num_key_base][num_keys_per_base];
	uint64_t keys[num_keys_per_base];
	uint64_t key_bases[num_keys_per_base];
	get_randoms(num_keys_per_base, keys);
	get_randoms(num_keys_per_base, key_bases);

	printf("key base \n");
	for (int i = 0; i < num_key_base; i++) {
		printf("0x%lx,\n", key_bases[i]);
	}

	printf("random keys \n");
	for (int i = 0; i < num_keys_per_base; i++) {
		printf("0x%lx,\n", keys[i]);
	}

	// insert random keys
	std::unordered_set<uint64_t> to_delete;

	for (int j = 0; j <= 7; j++) {
		for (int i = num_keys_per_base - 1; i >= 0; i--) {
			key_arr[j][i] = (key_bases[j] & (~0ULL << 32)) | ((uint64_t) keys[i] >> 32);
			int key_len = 8;
			uint64_key key64 = uint64_key{key_arr[j][i]};
			index->insert(key64.ptr, &key_arr[j][i], key_len);
			if (rand() % 2 == 0) to_delete.insert(key_arr[j][i]);
		}
	}

	unsigned long size_original = index->get_size();

	printf("delete\n");
	// delete keys
	for (auto tup: to_delete) {
		uint64_key key64 = uint64_key{tup};
		index->remove_key(key64.ptr, 8);
		printf("0x%lx,\n", tup);
	}

	BOOST_ASSERT(size_original - index->get_size() == to_delete.size());

	// disable prefixed key sets
	for (int j = 0; j <= 7; j++) {
		int key_len = j + 1;
		uint64_key key64 = uint64_key{key_bases[j]};
		uint8_t *key_base = key64.ptr;
		index->disable(key_base, key_len);
//        printf("Disabled key base %lx with length %u, ret %i\n", key_bases[j], key_len, disabled);
	}

	// lookup
	for (int j = 0; j <= 7; j++) {
		for (int i = num_keys_per_base - 1; i >= 0; i--) {
			int key_len = 8; //(71-i)/8;
			void *ptr = nullptr;
			uint64_key key64 = uint64_key{key_arr[j][i]};
			uint8_t *key_ptr = key64.ptr;
			int ret = index->lookup(key_ptr, &ptr, key_len);
			uint64_key key64_cmp = uint64_key{key_arr[j][i]};
			uint8_t *key = key64_cmp.ptr;
			uint64_key key64_base = uint64_key{key_bases[j]};
			uint8_t *key_base = key64_base.ptr;
			if (std::memcmp(key, key_base, j + 1) != 0) {
				if (to_delete.find(key_arr[j][i]) == to_delete.end()) {
					uint64_key key64_found = uint64_key{key_arr[j][i]};
					void *found = index->get(key64_found.ptr, key_len);
					BOOST_ASSERT(ret == 1);
					BOOST_ASSERT((uintptr_t) ptr == (uintptr_t) found);
				} else {
					BOOST_ASSERT(ret == 0);
					BOOST_ASSERT(ptr == nullptr);
				}
			} else {
				BOOST_ASSERT(ret == -1);
			}
		}
	}


	for (int j = 0; j <= 7; j++) {
		// disable keys down from disable barrier
		// dissable key up from disable barrier
		int key_len = 8 - j;
		uint64_key key64 = uint64_key{key_bases[j]};
		index->disable(key64.ptr, key_len);
	}

	for (int j = 0; j <= 7; j++) {
		for (int i = num_keys_per_base - 1; i >= 0; i--) {
			int key_len = 8; //(71-i)/8;
			void *ptr = nullptr;
			uint64_key key64 = uint64_key{key_arr[j][i]};
			int ret = index->lookup(key64.ptr, &ptr, key_len);
			if (j >= 8 && to_delete.find(key_arr[j][i]) == to_delete.end()) {
				BOOST_ASSERT(ret == 0);
				if (to_delete.find(key_arr[j][i]) == to_delete.end()) {
					BOOST_ASSERT(ret == 1);
					BOOST_ASSERT((uintptr_t) ptr == (uintptr_t) index->get(uint64_key(key_arr[j][i]).ptr, key_len));
				} else {
					BOOST_ASSERT(ret == 0);
					BOOST_ASSERT(ptr == nullptr);
				}
			} else {
				BOOST_ASSERT(ret == -1);
				BOOST_ASSERT(ptr == nullptr);
			}
		}
	}

	uint64_t disabled_key_bases[num_keys_per_base];
	get_randoms(num_keys_per_base, &disabled_key_bases[0]);
	int disabled_flag[num_key_base];
	printf("disabled key bases \n");
	for (int i = 0; i < num_key_base; i++) {
		printf("0x%lx,\n", disabled_key_bases[i]);
	}

	for (int i = 0; i < num_key_base; i++) {
		int key_len = 4;
		uint64_key key64 = uint64_key{disabled_key_bases[i]};
		uint8_t *key = key64.ptr;
		disabled_flag[i] = index->disable(key, key_len);
	}

	for (int i = 0; i < num_key_base; i++) {
		void *ptr = nullptr;
		uint64_key key64 = uint64_key{disabled_key_bases[i]};
		int ret = index->lookup(key64.ptr, &ptr, 8);
		if (i / 2 >= 1) {
			BOOST_ASSERT(ret == -1);
			BOOST_ASSERT(ptr == nullptr);
		}
	}

	// delete key by prefixes
	for (int i = 0; i < num_key_base; i++) {
		int key_len = i / 2;
		uint64_key key64 = uint64_key{disabled_key_bases[i]};
		uint8_t *key = key64.ptr;
		void *ptr = nullptr;
		if (key_len > 0) {
			index->lookup(key, &ptr, 8);
			index->remove_prefix(key, key_len);
		}
	}

	for (int i = 0; i < num_key_base; i++) {
		void *ptr = nullptr;
		uint64_key key64 = uint64_key{disabled_key_bases[i]};
		uint8_t *key = key64.ptr;
		int ret = index->lookup(key, &ptr, 8);
		if (i / 2 >= 1 && disabled_flag[i] == 1) {
			BOOST_ASSERT(ret == 0);
			BOOST_ASSERT(ptr == nullptr);
		}
	}

	delete index;
}

BOOST_AUTO_TEST_CASE(test_radix_tree_specialized_buffer) {
	radix_tree_index<uint64_t *> idx_tree;
	int num_key_base = 10;
	int num_keys_per_base = 10;
	uint64_t key_arr[num_key_base][num_keys_per_base];
	uint64_t keys[num_keys_per_base];
	uint64_t key_bases[num_keys_per_base];
	get_randoms(num_keys_per_base, keys);
	get_randoms(num_keys_per_base, key_bases);

	printf("key base \n");
	for (int i = 0; i < num_key_base; i++) {
		printf("0x%lx,\n", key_bases[i]);
	}

	printf("random keys \n");
	for (int i = 0; i < num_keys_per_base; i++) {
		printf("0x%lx,\n", keys[i]);
	}

	// insert random keys
	std::unordered_set<uint64_t> to_delete_set{};
	std::unordered_map<uint64_t, uint8_t *> to_delete_key_to_ptr{};

	uint64_t key_buffer[num_keys_per_base * 8];
	int idx = 0;
	for (int j = 0; j <= 7; j++) {
		for (int i = num_keys_per_base - 1; i >= 0; i--) {
			key_arr[j][i] = __builtin_bswap64((key_bases[j] & (~0ULL << 32)) | ((uint64_t) keys[i] >> 32));
			uint32_t key_len = 8;
			memcpy(&key_buffer[idx], &key_arr[j][i], sizeof(uint64_t));
			idx_tree.insert((uint8_t *) (&key_buffer[idx]), &key_arr[j][i], key_len);
			if (rand() % 2 == 0) {
				to_delete_set.insert(key_arr[j][i]);
				to_delete_key_to_ptr.insert(std::make_pair(key_arr[j][i], (uint8_t *) (&key_buffer[idx])));
			}
			idx += 1;
		}
	}

	uint64_t size_original = idx_tree.get_size();

	printf("delete\n");
	// delete keys
	for (auto tup: to_delete_set) {
		idx_tree.remove_key(to_delete_key_to_ptr.at(tup), 8);
		printf("0x%lx,\n", tup);
	}

	std::cout << "size original: " << size_original << " index size "
			  << idx_tree.get_size() << " to_delete size" << to_delete_set.size() << std::endl;
	BOOST_ASSERT(size_original - idx_tree.get_size() == to_delete_set.size());

	// disable prefixed key sets
	uint8_t key_base_buffer[num_keys_per_base * 8];
	for (int j = 0; j <= 7; j++) {
		uint32_t key_len = j + 1;
		uint64_t key_base_reversed = __builtin_bswap64(key_bases[j]);
		memcpy(&key_base_buffer[j * 8], &key_base_reversed, sizeof(uint64_t));
		idx_tree.disable(&key_base_buffer[j * 8], key_len);
//        printf("Disabled key base %lx with length %u, ret %i\n", key_base_reversed, key_len, disabled);
	}

	// lookup
	idx = 0;
	for (int j = 0; j <= 7; j++) {
		for (int i = num_keys_per_base - 1; i >= 0; i--) {
			uint32_t key_len = 8; //(71-i)/8;
			void *ptr = nullptr;
			int ret = idx_tree.lookup((uint8_t *) (&key_buffer[idx]), &ptr, key_len);
			uint64_t *key = &key_buffer[idx];

			int cmp = 0;
			bool disabled_flag = false;
			while (cmp <= j) {
				uint64_t key_base_reversed = __builtin_bswap64(key_bases[cmp]);
				if (std::memcmp(key, &key_base_reversed, cmp + 1) == 0) {
					disabled_flag = true;
					break;
				}
				cmp++;
			}

			if (!disabled_flag) {
				if (to_delete_set.find(key_arr[j][i]) == to_delete_set.end()) {
					void *found = idx_tree.get((uint8_t *) (&key_buffer[idx]), key_len);
					BOOST_ASSERT(ret >= 1);
					BOOST_ASSERT((uintptr_t) ptr == (uintptr_t) found);
				} else {
					BOOST_ASSERT(ret == 0);
					BOOST_ASSERT(ptr == nullptr);
				}
			} else {
				BOOST_ASSERT(ret == -1);
			}
			idx++;
		}
	}

	for (int j = 0; j <= 7; j++) {
		// disable keys down from disable barrier
		uint32_t key_len = 8 - j;
		idx_tree.disable(&key_base_buffer[j * 8], key_len);
	}

	idx = 0;
	for (int j = 0; j <= 7; j++) {
		for (int i = num_keys_per_base - 1; i >= 0; i--) {
			uint32_t key_len = 8; //(71-i)/8;
			void *ptr = nullptr;

			int ret = idx_tree.lookup((uint8_t *) (&key_buffer[idx]), &ptr, key_len);
			if (j >= 8 && to_delete_set.find(key_arr[j][i]) == to_delete_set.end()) {
				BOOST_ASSERT(ret == 0);
				if (to_delete_set.find(key_arr[j][i]) == to_delete_set.end()) {
					BOOST_ASSERT(ret >= 1);
					BOOST_ASSERT((uintptr_t) ptr == (uintptr_t) idx_tree.get(uint64_key{key_arr[j][i]}.ptr, key_len));
				} else {
					BOOST_ASSERT(ret == 0);
					BOOST_ASSERT(ptr == nullptr);
				}
			} else {
				BOOST_ASSERT(ret == -1);
				BOOST_ASSERT(ptr == nullptr);
			}
			idx++;
		}
	}
	uint64_t disabled_key_bases[num_keys_per_base];
	int disabled_flag[num_key_base];
	get_randoms(num_keys_per_base, disabled_key_bases);
	uint8_t disabled_key_base_buffer[num_keys_per_base * 8];
	printf("disabled key bases \n");
	for (int i = 0; i < num_key_base; i++) {
		printf("0x%lx,\n", disabled_key_bases[i]);
		uint64_t key_base_reversed = __builtin_bswap64(disabled_key_bases[i]);
		memcpy(&disabled_key_base_buffer[i * 8], &key_base_reversed, sizeof(uint64_t));
	}

	for (int i = 0; i < num_key_base; i++) {
		uint32_t key_len = 4;
		disabled_flag[i] = idx_tree.disable(&disabled_key_base_buffer[i * 8], key_len);
	}

	for (int i = 0; i < num_key_base; i++) {
		void *ptr = nullptr;
		int ret = idx_tree.lookup(&disabled_key_base_buffer[i * 8], &ptr, 8);
		if (i / 2 >= 1) {
			BOOST_ASSERT(ret == -1);
			BOOST_ASSERT(ptr == nullptr);
		}
	}

	// delete key by prefixes
	for (int i = 0; i < num_key_base; i++) {
		uint32_t key_len = i / 2;
		void *ptr = nullptr;
		if (key_len > 0) {
			idx_tree.lookup(&disabled_key_base_buffer[i * 8], &ptr, 8);
			idx_tree.remove_prefix(&disabled_key_base_buffer[i * 8], key_len);
		}
	}

	for (int i = 0; i < num_key_base; i++) {
		void *ptr = nullptr;
		int ret = idx_tree.lookup(&disabled_key_base_buffer[i * 8], &ptr, 8);
		if (i / 2 >= 1 && disabled_flag[i] == 1) {
			BOOST_ASSERT(ret == 0);
			BOOST_ASSERT(ptr == nullptr);
		}
	}
}

BOOST_AUTO_TEST_SUITE_END()
