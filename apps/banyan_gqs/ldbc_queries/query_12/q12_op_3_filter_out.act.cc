//
// Created by everlighting on 2020/10/12.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q12_op_3_filter_out.act.h"
#include "q12_op_4_filter_out.act.h"
#include "q12_op_9_globalSync_group_order.act.h"

void q12_op_3_filter_out::process(vertex_friendId_Batch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto label_id = storage::bdb_graph_handler::_local_graph->get_vertex_label_id(input[i].v_id);
        if (label_id == _comment_labelId) {
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].v_id, _out_label);
            while (iter.next(out_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour),
                                            input[i].friend_id, input[i].v_id, out_neighbour);
            }
        }
    }
    _ds_group_hdl->flush();
}

void q12_op_3_filter_out::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos(2);
    }
}

void q12_op_3_filter_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 9);
}

void q12_op_3_filter_out::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
