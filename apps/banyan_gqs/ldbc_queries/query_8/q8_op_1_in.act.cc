//
// Created by everlighting on 2020/10/9.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q8_op_1_in.act.h"
#include "q8_op_2_in.act.h"
#include "q8_op_4_globalSync_order_limit.act.h"

void q8_op_1_in::process(StartVertex &&input) {
    int64_t in_neighbour;
    auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(input.val, _in_label);
    while (iter.next(in_neighbour)) {
        _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour);
    }
    _ds_group_hdl->flush();
}

void q8_op_1_in::receive_eos(Eos &&eos) {
    if (_ds_group_hdl->have_send_data()) {
        _ds_group_hdl->receive_eos();
    } else {
        _sync_hdl->Get()->finish_query();
    }
}

void q8_op_1_in::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 4);
}

void q8_op_1_in::finalize() {
    delete _sync_hdl;
    delete _ds_group_hdl;
}
