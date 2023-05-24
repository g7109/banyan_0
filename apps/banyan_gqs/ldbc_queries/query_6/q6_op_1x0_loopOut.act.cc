//
// Created by everlighting on 2020/9/29.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q6_op_1x0_loopOut.act.h"
#include "q6_op_1x1_loopUntil.act.h"

void q6_op_1x0_loopOut::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            unsigned loc;
            if (at_e5) {
                loc = load_balancer::locate(out_neighbour);
            } else {
                loc = locate_helper::locate(out_neighbour);
            }
            _ds_group_hdl->emplace_data(loc, out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void q6_op_1x0_loopOut::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q6_op_1x0_loopOut::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q6_op_1x0_loopOut::finalize() {
    delete _ds_group_hdl;
}
