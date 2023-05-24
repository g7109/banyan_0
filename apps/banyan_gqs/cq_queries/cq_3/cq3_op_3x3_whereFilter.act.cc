//
// Created by everlighting on 2021/4/13.
//

#include "cq3_op_3x3_whereFilter.act.h"
#include "cq3_op_3x4_whereBranchSync.act.h"

void cq3_op_3x3_whereFilter::process(VertexBatch &&input) {
    if (_found) { return; }
    std::tuple<char*, int> prop_loc_info;
    std::string name;
    for (unsigned i = 0; i < input.size(); i++) {
        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
        prop_loc_info = prop_iter.next(_name_propId);
        name = storage::berkeleydb_helper::readString(
                std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
        if (name == _expect_type_name) {
            _sync_ref->set_successful();
            _found = true;
            return;
        }
    }
}

void cq3_op_3x3_whereFilter::receive_eos(PathEos &&eos) {
    _sync_ref->receive_eos(std::move(eos));
}

void cq3_op_3x3_whereFilter::initialize() {
    auto builder = get_scope();
    auto branch_id = builder.get_current_scope_id();
    builder.reset_shard(branch_id % brane::global_shard_count());
    _sync_ref = builder.new_ref<sync_t>(_sync_op_id);
}

void cq3_op_3x3_whereFilter::finalize() {
    delete _sync_ref;
}
