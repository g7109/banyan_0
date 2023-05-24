#include "berkeley_db/storage/bdb_graph_handler.hh"
#include <seastar/core/print.hh>
#include <utility>

bool load_balancer::_balanced = true;

void load_balancer::re_balance() {
    std::vector<seastar::future<>> futs;
    futs.reserve(brane::global_shard_count());
    for (unsigned i = 0; i < brane::global_shard_count(); i++) {
        futs.push_back(seastar::smp::submit_to(i, [] {
            return storage::bdb_graph_handler::_local_graph->set_to_balance();
        }));
    }
    seastar::when_all_succeed(futs.begin(), futs.end()).then([] {
        _balanced = true;
        // TODO: set all machines "_balanced = true"
    }, nullptr);
}

namespace storage {

thread_local std::unique_ptr<storage::local_graph_store_handler> bdb_graph_handler::_local_graph;

void bdb_graph_handler::init_graph() {
    auto* p_graph = new local_graph_store_handler();
    _local_graph = std::unique_ptr<local_graph_store_handler>{p_graph};
}

void bdb_graph_handler::close() {
    _local_graph.release()->~local_graph_store_handler();
}

} // namespace storage