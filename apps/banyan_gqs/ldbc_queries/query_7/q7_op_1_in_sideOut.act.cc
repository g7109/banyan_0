//
// Created by everlighting on 2020/10/13.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q7_op_1_in_sideOut.act.h"
#include "q7_op_2_friendStore.act.h"
#include "q7_op_2_in_localOrder.act.h"
#include "q7_op_3_globalSync_order_limit.act.h"

void q7_op_1_in_sideOut::process(StartVertex &&input) {
    int64_t msg_id;
    int64_t friend_id;

    auto friend_iter = storage::bdb_graph_handler::_local_graph->get_out_v(input.val, _side_out_label);
    while (friend_iter.next(friend_id)) {
        _side_ds_group_hdl->emplace_data(locate_helper::locate(friend_id), friend_id);
    }
    auto msg_iter = storage::bdb_graph_handler::_local_graph->get_in_v(input.val, _in_label);
    while (msg_iter.next(msg_id)) {
        _ds_group_hdl->emplace_data(locate_helper::locate(msg_id), msg_id);
    }

    _side_ds_group_hdl->flush();
    _ds_group_hdl->flush();
}

void q7_op_1_in_sideOut::receive_eos(Eos &&eos) {
    if (_ds_group_hdl->have_send_data()) {
        _side_ds_group_hdl->receive_eos();
        _ds_group_hdl->receive_eos();
    } else {
        _sync_hdl->Get()->finish_query();
    }
}

void q7_op_1_in_sideOut::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _side_ds_group_hdl = new side_ds_group_t(builder, ds_id);
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 3);
}

void q7_op_1_in_sideOut::finalize() {
    delete _side_ds_group_hdl;
    delete _ds_group_hdl;
    delete _sync_hdl;
}
