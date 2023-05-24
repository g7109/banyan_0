#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "q1_op_2_loopLeave_dedup_filter.act.h"
#include "q1_op_3_globalSync_order_limit.act.h"

void q1_op_2_loopLeave_dedup_filter::process(vertex_pathLen_Batch&& input) {
    std::string label = "";
    vertex_pathLen_StrProp_Batch output{input.size()};
    for (unsigned i = 0; i < input.size(); i++) {
        if (_vertices_have_seen.find(input[i].v_id) == _vertices_have_seen.end()) {
            _vertices_have_seen.insert(input[i].v_id);
            if (storage::bdb_graph_handler::_local_graph->get_vertex(input[i].v_id, _prop_checker, label)) {
                auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i].v_id);
                auto prop_loc_info = prop_iter.next(_lastName_type_id);
                std::string prop_str = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
                output.emplace_back(input[i].v_id, input[i].path_len, prop_str);
            }
        }
    }
    if (output.size() > 0) {
        _ds_sync_hdl->process(std::move(output));
    }
}

void q1_op_2_loopLeave_dedup_filter::receive_eos(Eos &&eos) {
    // Upstream Num = 1. No sync operator.
    // As a special case, the downstream of this operator happens to be the global sync operator.
    _ds_sync_hdl->receive_eos();
}

void q1_op_2_loopLeave_dedup_filter::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    _prop_checker.property_check.checker = q1_helper.get_first_name(query_id);

    unsigned sync_shard = at_ic ? 0 : query_id % brane::global_shard_count();
    auto sync_builder = brane::scope_builder(sync_shard, brane::scope<brane::actor_group<> >(query_id));
    _ds_sync_hdl = new ds_and_sync_t(sync_builder, 3);
}

void q1_op_2_loopLeave_dedup_filter::finalize() {
    delete _ds_sync_hdl;
}






