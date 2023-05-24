//
// Created by everlighting on 2020/8/10.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq6_op_1x0_loopOut.act.h"
#include "cq6_op_1x2_loop_dedup_whereEnter.act.h"

void cq6_op_1x0_loopOut::process(VertexBatch&& input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void cq6_op_1x0_loopOut::receive_eos(Eos&& eos) {
    _ds_group_hdl->receive_eos();
}

void cq6_op_1x0_loopOut::initialize() {
    auto builder = get_scope();
    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);
}

void cq6_op_1x0_loopOut::finalize() {
    delete _ds_group_hdl;
}
