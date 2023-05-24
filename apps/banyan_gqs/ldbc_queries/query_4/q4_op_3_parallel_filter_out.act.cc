//
// Created by everlighting on 2020/11/13.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "q4_op_3_parallel_filter_out.act.h"
#include "q4_op_4_filter_propGet.act.h"
#include "q4_op_5_globalSync_order.act.h"

void q4_op_3_parallel_filter_out::process(VertexBatch &&vertices) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < vertices.size(); i++) {
        if (storage::bdb_graph_handler::_local_graph->get_vertex(
                vertices[i], _side_prop_checker, _filter_label)) {
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(vertices[i], _out_label);
            while (iter.next(out_neighbour)) {
                _side_ds_group_hdl->emplace_data<&i_q4_op_4_filter_propGet::record_excluded_tags>(
                        locate_helper::locate(out_neighbour), out_neighbour);
            }
        }
        if (storage::bdb_graph_handler::_local_graph->get_vertex(
                vertices[i], _prop_checker, _filter_label)) {
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(vertices[i], _out_label);
            while (iter.next(out_neighbour)) {
                _ds_group_hdl->emplace_data<&i_q4_op_4_filter_propGet::record_candidate_tags>(
                        locate_helper::locate(out_neighbour), out_neighbour);
            }
        }
    }
    _side_ds_group_hdl->flush<&i_q4_op_4_filter_propGet::record_excluded_tags>();
    _ds_group_hdl->flush<&i_q4_op_4_filter_propGet::record_candidate_tags>();
}

void q4_op_3_parallel_filter_out::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void q4_op_3_parallel_filter_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _side_ds_group_hdl = new side_ds_group_t(builder, ds_id);
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 5);

    _side_prop_checker.property_check.checker = q4_helper.get_start_date(query_id);
    _prop_checker.property_check.lower = q4_helper.get_start_date(query_id);
    _prop_checker.property_check.upper = q4_helper.get_end_date(query_id);
}

void q4_op_3_parallel_filter_out::finalize() {
    delete _side_ds_group_hdl;
    delete _ds_group_hdl;
    delete _sync_hdl;
}



