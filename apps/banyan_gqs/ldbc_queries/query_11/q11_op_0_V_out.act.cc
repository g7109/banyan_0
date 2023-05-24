//
// Created by everlighting on 2020/10/9.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q11_op_0_V_out.act.h"
#include "q11_op_1_unionOut.act.h"
#include "q11_op_6_globalSync_order_limit.act.h"

void q11_op_0_V_out::trigger(StartVertex&& src) {
    int64_t out_neighbour;
    auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(src.val, _out_label);
    while (iter.next(out_neighbour)) {
        _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
    }
    _ds_group_hdl->flush();

    if (_ds_group_hdl->have_send_data()) {
        _ds_group_hdl->receive_eos();
    } else {
        _sync_hdl->Get()->finish_query();
    }
}

void q11_op_0_V_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_group_hdl = new ds_group_t(builder, 1);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 6);
}

void q11_op_0_V_out::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
