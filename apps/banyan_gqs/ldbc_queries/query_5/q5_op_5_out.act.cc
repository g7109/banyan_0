//
// Created by everlighting on 2020/9/28.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q5_op_3_dedup_friendStore_in.act.h"
#include "q5_op_5_out.act.h"
#include "q5_op_7_globalSync_order.act.h"

void q5_op_5_out::process(vertex_forumId_Batch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].v_id, _out_label);
        while (iter.next(out_neighbour)) {
            _ds_group_hdl->emplace_data<&i_q5_op_3_dedup_friendStore_in::record_msgCreators_in_forum>(
                    locate_helper::locate(out_neighbour), out_neighbour, input[i].forum_id);
        }
    }
    _ds_group_hdl->flush<&i_q5_op_3_dedup_friendStore_in::record_msgCreators_in_forum>();
}

void q5_op_5_out::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void q5_op_5_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 7);
}

void q5_op_5_out::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
