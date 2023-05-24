#include "cq3x_op_2_dedup_in.act.h"
#include "berkeley_db/storage/bdb_graph_handler.hh"

void cq3x_op_2_dedup_in::process(VertexBatch&& vertices) {
    int64_t in_neighbour;
    for (unsigned i = 0; i < vertices.size(); i++) {
        if (_friends_have_seen.find(vertices[i]) == _friends_have_seen.end()) {
            _friends_have_seen.insert(vertices[i]);

            auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(vertices[i], _in_label);
            while (iter.next(in_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour, vertices[i]);
            }
        }
    }
    _ds_group_hdl->flush();
}

void cq3x_op_2_dedup_in::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void cq3x_op_2_dedup_in::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void cq3x_op_2_dedup_in::finalize() {
    delete _ds_group_hdl;
}
