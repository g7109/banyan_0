//
// Created by everlighting on 2021/4/13.
//


#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq3_op_2_unionOut.act.h"
#include "cq3_op_4_dedup_whereEnter.act.h"
#include "cq3_op_6_globalSync_order_limit.act.h"

void cq3_op_2_unionOut::process(VertexBatch &&vertices) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < vertices.size(); i++) {
        _ds_group_hdl->emplace_data(locate_helper::locate(vertices[i]), vertices[i]);
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(vertices[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_group_hdl->flush();
}

void cq3_op_2_unionOut::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void cq3_op_2_unionOut::initialize() {
    auto builder = get_scope();
    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);
    builder.reset_shard(0);
    _sync_hdl = new sync_t(builder, 6);
}

void cq3_op_2_unionOut::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
