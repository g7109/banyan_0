//
// Created by everlighting on 2020/11/13.
//

#include "q3_op_5_in.act.h"
#include "q3_op_8_group_and_order.act.h"

void q3_op_5_in::process(VertexBatch &&vertices) {
    int64_t in_neighbour;
    for (unsigned i = 0; i < vertices.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(vertices[i], _in_label);
        while (iter.next(in_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour, vertices[i]);
        }
    }
    _ds_group_hdl->flush();
}

void q3_op_5_in::receive_eos(Eos &&eos) {
    // Upstream Num = #shard. Data flow may be interrupted
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void q3_op_5_in::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto query_id = builder.get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 8);
}

void q3_op_5_in::finalize() {
    delete _sync_hdl;
    delete _ds_group_hdl;
}
