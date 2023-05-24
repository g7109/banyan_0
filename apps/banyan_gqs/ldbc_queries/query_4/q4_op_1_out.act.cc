//
// Created by everlighting on 2020/9/17.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q4_op_1_out.act.h"
#include "q4_op_2_in.act.h"

void q4_op_1_out::process(VertexBatch&& vertices) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < vertices.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(vertices[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void q4_op_1_out::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q4_op_1_out::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q4_op_1_out::finalize() {
    delete _ds_group_hdl;
}
