#include "q2_op_2_in.act.h"

void q2_op_2_in::process(VertexBatch &&vertices) {
    int64_t in_neighbour;
    for(unsigned i = 0; i < vertices.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(vertices[i], _in_label);
        while(iter.next(in_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void q2_op_2_in::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q2_op_2_in::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q2_op_2_in::finalize() {
    delete _ds_group_hdl;
}




