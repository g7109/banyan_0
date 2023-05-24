//
// Created by everlighting on 2020/10/9.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q11_op_1_unionOut.act.h"
#include "q11_op_2_dedup_out.act.h"

void q11_op_1_unionOut::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        _ds_group_hdl->emplace_data(locate_helper::locate(input[i]), input[i]);
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void q11_op_1_unionOut::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q11_op_1_unionOut::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q11_op_1_unionOut::finalize() {
    delete _ds_group_hdl;
}
