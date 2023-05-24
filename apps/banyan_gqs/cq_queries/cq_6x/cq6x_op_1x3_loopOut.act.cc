//
// Created by everlighting on 2021/4/19.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq6x_op_1x3_loopOut.act.h"
#include "cq6x_op_1x4_dedup_loopFilter.act.h"

void cq6x_op_1x3_loopOut::process(vertex_friendId_Batch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].v_id, _out_label);
        iter.next(out_neighbour);
        _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour, input[i].friend_id);
    }
    _ds_group_hdl->flush();
}

void cq6x_op_1x3_loopOut::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void cq6x_op_1x3_loopOut::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void cq6x_op_1x3_loopOut::finalize() {
    delete _ds_group_hdl;
}

