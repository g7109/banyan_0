//
// Created by everlighting on 2021/4/9.
//
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq5x_op_2_loopLeave_dedup_in.act.h"

#include "cq5x_op_1x1_loopUntil.act.h"

void cq5x_op_2_loopLeave_dedup_in::process(VertexBatch &&input) {
    int64_t in_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_friends_have_seen.find(input[i]) == _friends_have_seen.end()) {
            _friends_have_seen.insert(input[i]);

            auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(input[i], _in_label);
            while (iter.next(in_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour, input[i]);
            }
        }
    }
    _ds_group_hdl->flush();
}

void cq5x_op_2_loopLeave_dedup_in::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void cq5x_op_2_loopLeave_dedup_in::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void cq5x_op_2_loopLeave_dedup_in::finalize() {
    cq5x_op_1x1_loopUntil::_successful_vertices.clear();
    cq5x_op_1x1_loopUntil::_failed_vertices.clear();
    delete _ds_group_hdl;
}
