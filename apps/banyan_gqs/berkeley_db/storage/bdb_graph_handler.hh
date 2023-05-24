#pragma once

#include "berkeley_db/storage/bdb_graph_store.hh"
#include <memory>
#include <brane/core/shard-config.hh>
#include "common/configs.hh"

class locate_helper {
public:
    static unsigned locate(int64_t vertex_id) {
        int64_t s_num = brane::global_shard_count();
        return static_cast<unsigned>((int)(vertex_id % s_num));
    }
};

class load_balancer {
    static bool _balanced;
public:
    static unsigned locate(int64_t vertex_id) {
        auto partition_id = static_cast<unsigned>(
                (int)(vertex_id % static_cast<int64_t>(graph_partition_num)));
        return dst_shard_of_partition(partition_id);
    }

    static inline unsigned dst_shard_of_partition(unsigned partition_id) {
        if (_balanced) {
            return partition_id % brane::global_shard_count();
        }
        unsigned slice_len = brane::global_shard_count() / 2;
        unsigned base_shard_id = partition_id % slice_len;
        unsigned shard_offset = (((partition_id + slice_len) % (4 * slice_len)) == base_shard_id)
            ? slice_len : 0;
        return base_shard_id + shard_offset;
    }

    static inline void set_skewed() { _balanced = false; }
    static void re_balance();
};

namespace storage {

struct local_graph_store_handler {
    std::vector<berkeleydb_store*> _graphs;

    explicit local_graph_store_handler() {
        _graphs.reserve(graph_partition_num);
        for (unsigned i = 0; i < graph_partition_num; i++) {
            if (load_balancer::dst_shard_of_partition(i) == brane::global_shard_id()) {
                _graphs.push_back(berkeleydb_store::open(path_of_partition(i)));
            } else {
                _graphs.push_back(nullptr);
            }
        }
    }
    ~local_graph_store_handler() {
        for (auto& _graph : _graphs) {
            delete _graph;
        }
    }

    void warmup_cache() {
        for(auto& _graph : _graphs) {
            if (_graph) { _graph->warmup_cache(); }
        }
    }

    seastar::future<> set_to_balance() {
        for(unsigned i = 0; i < graph_partition_num; i++) {
            if (i % brane::global_shard_count() == brane::global_shard_id()) {
                if (_graphs[i] == nullptr) {
                    _graphs[i] = berkeleydb_store::open(path_of_partition(i));
                }
            }
        }
        return seastar::make_ready_future<>();
    }

    std::string path_of_partition(unsigned partition_id) {
        std::string path = std::string(banyan_gqs_dir) + "/berkeley_db/db_data/" + dataset + "/" + dataset
            + "_p" + std::to_string(partition_id) + "_s" + std::to_string(graph_partition_num) + ".db";
        return path;
    }

    std::vector<unsigned> get_partition_ids(unsigned shard_id) {
        std::vector<unsigned> ids{};
        ids.reserve(graph_partition_num / brane::global_shard_count());
        for (unsigned i = shard_id; i < graph_partition_num; i += brane::global_shard_count()) {
            ids.push_back(i);
        }
        return ids;
    }

    template <typename Pred = base_predicate>
    bool get_vertex(int64_t id, Pred &predicate, std::string &vertex_label) {
        return _graphs[static_cast<uint64_t>(id) % graph_partition_num]->get_vertex<Pred>(id, predicate, vertex_label);
    }

    long get_vertex_label_id(int64_t id) {
        return _graphs[static_cast<uint64_t>(id) % graph_partition_num]->get_vertex_label_id(id);
    }

    template <typename Pred = base_predicate>
    scan_vertex_iterator<Pred> scan_vertex(unsigned partition_id, const std::string &label) {
        return _graphs[partition_id]->scan_vertex<Pred>(label);
    }

    template <typename Pred = base_predicate>
    scan_edge_iterator<Pred> scan_edge(unsigned partition_id, const std::string &label) {
        return _graphs[partition_id]->scan_edge<Pred>(label);
    }

    template <typename Pred = base_predicate>
    vertex_iterator<Pred> get_out_v(int64_t src_id, const std::string &label) {
        return _graphs[static_cast<uint64_t>(src_id) % graph_partition_num]->get_out_v<Pred>(src_id, label);
    }

    template <typename Pred = base_predicate>
    vertex_iterator<Pred> get_in_v(int64_t src_id, const std::string &label) {
        return _graphs[static_cast<uint64_t>(src_id) % graph_partition_num]->get_in_v<Pred>(src_id, label);
    }

    vertex_property_iterator get_vertex_properties(int64_t src_id) {
        return _graphs[static_cast<uint64_t>(src_id) % graph_partition_num]->get_vertex_properties(src_id);
    }

    edge_property_iterator get_in_edge_properties(int64_t other_vid, int64_t local_vid, const std::string &label) {
        return _graphs[static_cast<uint64_t>(local_vid) % graph_partition_num]->get_in_edge_properties(other_vid, local_vid, label);
    }

    edge_property_iterator get_out_edge_properties(int64_t local_vid, int64_t other_vid, const std::string &label) {
        return _graphs[static_cast<uint64_t>(local_vid) % graph_partition_num]->get_out_edge_properties(local_vid, other_vid, label);
    }

    template <typename Pred = base_predicate>
    vertex_iterator_with_property<Pred> get_in_v_with_property(int64_t src_id, const std::string &label) {
        return _graphs[static_cast<uint64_t>(src_id) % graph_partition_num]->get_in_v_with_property<Pred>(src_id, label);
    }

    template <typename Pred = base_predicate>
    vertex_iterator_with_property<Pred> get_out_v_with_property(int64_t src_id, const std::string &label) {
        return _graphs[static_cast<uint64_t>(src_id) % graph_partition_num]->get_out_v_with_property<Pred>(src_id, label);
    }
};

struct bdb_graph_handler {
    static thread_local std::unique_ptr<local_graph_store_handler> _local_graph;
    static void init_graph();
    static void close();
};

} // namespace storage