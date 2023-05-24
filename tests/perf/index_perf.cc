//
// Created by le.xl on 2019/6/27.
//

#include <random>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <algorithm>
#include <array>
#include <random>
#include <assert.h>
#include <iostream>
#include <google/dense_hash_map>
#include <brane/util/radix_tree_index.hh>

using namespace brane;

// For basic tests
constexpr int num_keys = 1000000;
constexpr int read_size = 2000;
constexpr int window_size = 5000;
constexpr int slide = 1000;

//For simulation tests
constexpr uint64_t num_users = 30;
constexpr uint64_t num_dtflws = 30;

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

template<class k, class v>
class index_instance {
public:
	virtual void insert(k, v) = 0;

	virtual v read(k) = 0;

	virtual void remove(k) = 0;

	virtual bool contains(k) = 0;

	virtual ~index_instance() = 0;
};

template<class k, class v>
inline index_instance<k, v>::~index_instance<k, v>() = default;

class dense_map_instance : public index_instance<uint64_t, uint64_t *> {
private:
	google::dense_hash_map<uint64_t, uint64_t *> *map;

public:
	void insert(uint64_t key, uint64_t *value) override {
		map->insert(std::make_pair(key, value));
	}

	uint64_t *read(uint64_t key) override {
		return map->find(key)->second;
	}

	void remove(uint64_t key) override {
		map->erase(key);
	}

	bool contains(uint64_t key) override {
		return !(map->find(key) == map->end());
	}

	dense_map_instance() {
		map = new google::dense_hash_map<uint64_t, uint64_t *>();
		map->set_empty_key(0);
		map->set_deleted_key(1);
	}

	~dense_map_instance() override {
		delete map;
	}
};

class unordered_map_instance : public index_instance<uint64_t, uint64_t *> {
private:
	std::unordered_map<uint64_t, uint64_t *> *map;

public:
	void insert(uint64_t key, uint64_t *value) override {
		map->insert({key, value});
	}

	uint64_t *read(uint64_t key) override {
		auto ret = map->find(key);
		if (map->find(key) != map->end()) {
			return ret->second;
		} else {
			return nullptr;
		}
	}

	void remove(uint64_t key) override {
		map->erase(key);
	}

	bool contains(uint64_t key) override {
		return !(map->find(key) == map->end());
	}

	unordered_map_instance() {
		map = new std::unordered_map<uint64_t, uint64_t *>();
	}

	~unordered_map_instance() override {
		delete map;
	}
};

class art_lib_instance : public index_instance<uint64_t, uint64_t *> {

private:
	radix_tree_index<uint64_t *> *index;
	const int key_length = sizeof(uint64_t) / sizeof(char);

public:
	void insert(uint64_t key, uint64_t *value) override {
		uint64_key key64 = uint64_key{key};
		index->insert(key64.ptr, value, key_length);
	}

	uint64_t *read(uint64_t key) override {
		uint64_key key64 = uint64_key{key};
		return index->get(key64.ptr, key_length);
	}

	void remove(uint64_t key) override {
		uint64_key key64 = uint64_key{key};
		index->remove_key(key64.ptr, key_length);
	}

	bool contains(uint64_t key) override {
		uint64_key key64 = uint64_key{key};
		return index->contains(key64.ptr, key_length);
	}

	art_lib_instance() {
		index = new radix_tree_index<uint64_t *>();
	}

	~art_lib_instance() override {
		delete index;
	}
};

void test_index_correctness() {
	index_instance<uint64_t, uint64_t *> *correctness_instances[] = {new art_lib_instance(),
																	 new dense_map_instance(),
																	 new unordered_map_instance()};
	for (auto index : correctness_instances) {
		// regular operations
		int num_actors = 10000;
		std::unordered_set<int> key_idx_remove;
		// generate random id
		uint64_t keys[num_actors];
		uint64_t values[num_actors];
		get_randoms(num_actors, &keys[0]);
		get_randoms(num_actors, &values[0]);

		for (int i = 0; i < num_actors; i++) {
			index->insert(keys[i], &values[i]);
			if (rand() % 2 == 0) {
				key_idx_remove.insert(i);
			}
		}

		for (int idx_remove : key_idx_remove) {
			index->remove(keys[idx_remove]);
		}
		delete index;
	}
}

void generate_operations(std::vector<std::pair<int, uint64_t>> *operations) {
	operations->reserve(num_keys / slide * (slide * 2 + read_size));

	// generate random uint64_t keys
	uint64_t keys[num_keys];
	get_randoms(num_keys, &keys[0]);

	// generate operations
	int window_head = 0;

	while (window_head + slide < num_keys) {
		int window_tail = window_head - window_size;
		int first_available = window_tail + slide > 0 ? window_tail + slide : 0;
		int last_available = window_head - 1 > 0 ? window_head - 1 : 0;
		int operation_head_idx = static_cast<int>(operations->size());

		// insert
		for (int i = window_head, end = window_head + slide; i < end; i++) {
			operations->push_back(std::make_pair(1, keys[i]));
		}

		// delete
		if (window_tail >= 0) {
			for (int i = window_tail, end = window_tail + slide; i < end; i++) {
				operations->push_back(std::make_pair(2, keys[i]));
			}
		}

		// read
		if (last_available > first_available) {
			int rand_idx = rand() % (last_available - first_available);
			int read_count = 0;
			while (read_count < read_size) {
				operations->push_back(std::make_pair(3, keys[rand_idx + first_available]));
				read_count++;
			}
		}

		// shuffle operations
		// obtain a time-based seed:
		int64_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		shuffle(operations->begin() + operation_head_idx, operations->end(), std::default_random_engine(seed));
		window_head += slide;
	}
}

void generate_simulation_operations(std::vector<std::vector<std::pair<int, uint64_t>>> *operations) {
	uint64_t base_operator_ids[] = {1, 2, 3, 4, 5, 6};
	uint64_t sid_count = 6;
	uint64_t l_count = 10;

	// 1 -> 2 -> 3 -> 4/1 -> 4/2 -> 4/3 -> 4/4 -> 4/6 -> 5 -> 6
	//                  |             |     |       |
	//                  |            4/5 - ->       |
	//                 <-    -   -   -   -   -   -  |
	//
	uint64_t read_skew = 10;
	for (uint64_t user = 0; user < num_users; user++) {
		for (uint64_t dtflw = 0; dtflw < num_dtflws; dtflw++) {
			std::vector<uint64_t> per_dtflw_operator_ids;
			std::vector<std::pair<int, uint64_t>> per_dtflw_operations;
			// Per dataflow
			for (uint64_t op : base_operator_ids) {
				if (op == 4) {
					for (uint64_t j = 0; j < l_count; j++) {
						{
							for (uint64_t i = 0; i < sid_count; i++)
								per_dtflw_operator_ids.push_back(op << 16 | i << 8 | j);
						}
					}
				} else {
					per_dtflw_operator_ids.push_back(op << 16);
				}
			}

			for (uint64_t i = 0; i < per_dtflw_operator_ids.size(); i++) {
				uint64_t current_op_id = per_dtflw_operator_ids[i];
				// insert
				per_dtflw_operations.push_back(std::make_pair(1, user << 48 | dtflw << 32 | current_op_id));
				// read once
				per_dtflw_operations.push_back(std::make_pair(3, user << 48 | dtflw << 32 | current_op_id));
				// read multiple
				uint32_t read_count = 0;
				while (read_count < read_skew) {
					auto current_id = per_dtflw_operator_ids[read_count % (i + 1)];
					if (((current_id & 0xff00) >> 8) == 4) {
						if (rand() % 2 == 1) {
							per_dtflw_operations.push_back(std::make_pair(3, user << 48 | dtflw << 32 |
																			 per_dtflw_operator_ids[(read_count++) %
																									(i + 1)]));
						} else {
							per_dtflw_operations.push_back(std::make_pair(3, user << 48 | dtflw << 32 |
																			 per_dtflw_operator_ids[(++read_count) %
																									(i + 1)]));
						}
					} else {
						per_dtflw_operations.push_back(std::make_pair(3, user << 48 | dtflw << 32 |
																		 per_dtflw_operator_ids[read_count %
																								(i + 1)]));
					}
					read_count++;
				}
			}
			// delete all
			for (auto id : per_dtflw_operator_ids) {
				per_dtflw_operations.push_back(std::make_pair(2, user << 48 | dtflw << 32 | id));
			}

			operations->push_back(per_dtflw_operations);
		}
	}
}


void test_index_performance() {
//void run_index_performance_test() {

	auto operations = std::vector<std::pair<int, uint64_t>>();
	generate_operations(&operations);
	//std::vector<std::pair<int, uint64_t>> *operations = generate_operations();
	index_instance<uint64_t, uint64_t *> *perf_instances[] = {new art_lib_instance(),
															  new dense_map_instance(),
															  new unordered_map_instance()};
	for (auto index : perf_instances) {
		auto start = std::chrono::high_resolution_clock::now();

		for (auto p : operations) {
			uint64_t key = p.second;
			switch (p.first) {
				case 1:  // insert
					index->insert(key, &key);
					break;
				case 3:  // read
					index->read(key);
					break;
				case 2:  // delete
					index->remove(key);
					break;
				default:
					index->read(key);
			}
		}
		auto finish = std::chrono::high_resolution_clock::now();
		auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(finish - start);
		std::cout << typeid(*index).name() << " " << microseconds.count() << "µs" << std::endl;
		delete index;
	}

}

void test_index_simulation() {
//void run_index_simulation_test() {

	index_instance<uint64_t, uint64_t *> *perf_simulation_instances[] = {
		new art_lib_instance(),
		new dense_map_instance(),
		new unordered_map_instance()
	};

	for (auto index : perf_simulation_instances) {
		std::vector<std::vector<std::pair<int, uint64_t>>> operations;
		generate_simulation_operations(&operations);

		auto start = std::chrono::high_resolution_clock::now();
		while (!operations.empty()) {
			uint64_t rand_index = rand() % operations.size();
			auto pair = operations[rand_index].begin();
			switch (pair->first) {
				case 1:
					index->insert(pair->second, &pair->second);
					break;
				case 3:
					index->read(pair->second);
					break;
				case 2:
					index->remove(pair->second);
					break;
				default:
					index->read(pair->second);
			}
			operations[rand_index].erase(pair);
			if (operations[rand_index].empty()) {
				operations.erase(operations.begin() + rand_index);
			}
		}

		auto finish = std::chrono::high_resolution_clock::now();
		auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(finish - start);
		std::cout << typeid(*index).name() << " " << microseconds.count() << "µs" << std::endl;

		delete index;
	}

}

int main() {
	test_index_performance();
	std::cout << std::endl;
	test_index_simulation();
}
