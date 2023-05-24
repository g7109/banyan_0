//
// Created by everlighting on 2021/4/9.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq5x_op_1x0_loopOut.act.h"
#include "cq5x_op_1x1_loopUntil.act.h"

void cq5x_op_1x0_loopOut::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void cq5x_op_1x0_loopOut::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void cq5x_op_1x0_loopOut::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void cq5x_op_1x0_loopOut::finalize() {
    delete _ds_group_hdl;
}
