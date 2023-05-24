//
// Created by everlighting on 2020/10/12.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q12_op_1_out.act.h"
#include "q12_op_2_in.act.h"

void q12_op_1_out::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for(unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void q12_op_1_out::receive_eos(Eos &&eos) {
    // Upstream Num = 1. No sync operator.
    _ds_group_hdl->receive_eos();
}

void q12_op_1_out::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q12_op_1_out::finalize() {
    delete _ds_group_hdl;
}

