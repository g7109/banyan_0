//
// Created by everlighting on 2020/10/9.
//

#include "q8_op_3_filter_propGet.act.h"
#include "q8_op_4_globalSync_order_limit.act.h"

void q8_op_3_filter_propGet::process(VertexBatch &&input) {
    create_relation_Batch relations{input.size()};
    int64_t creator;
    int64_t creation_date;
    for (unsigned i = 0; i < input.size(); i++) {
        if (storage::bdb_graph_handler::_local_graph->get_vertex(input[i], _prop_checker, _filter_label)) {
            auto creator_iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _prop_out_label);
            if (!creator_iter.next(creator)) {
                std::cout << "Error in dataset!\n";
            }
            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
            auto prop_loc_info = prop_iter.next(_prop_type_id);
            creation_date = storage::berkeleydb_helper::readLong(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
            relations.emplace_back(input[i], creator, creation_date);
        }
    }
    if (relations.size() > 0) {
        _ds_sync_hdl->process(std::move(relations));
    }
}

void q8_op_3_filter_propGet::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_sync_hdl->receive_eos();
    }
}

void q8_op_3_filter_propGet::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _ds_sync_hdl = new ds_and_sync_t(sync_builder, 4);
}

void q8_op_3_filter_propGet::finalize() {
    delete _ds_sync_hdl;
}

