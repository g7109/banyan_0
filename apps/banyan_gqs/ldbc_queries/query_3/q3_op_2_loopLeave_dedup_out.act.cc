#include "q3_op_2_loopLeave_dedup_out.act.h"
#include "q3_op_8_group_and_order.act.h"

void q3_op_2_loopLeave_dedup_out::process(VertexBatch &&vertices) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < vertices.size(); i++) {
        if (_vertices_have_seen.find(vertices[i]) == _vertices_have_seen.end()) {
            _vertices_have_seen.insert(vertices[i]);

            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(vertices[i], _out_label);
            iter.next(out_neighbour);
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour, vertices[i]);
        }
    }
    _ds_group_hdl->flush();
}

void q3_op_2_loopLeave_dedup_out::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q3_op_2_loopLeave_dedup_out::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q3_op_2_loopLeave_dedup_out::finalize() {
    delete _ds_group_hdl;
}


