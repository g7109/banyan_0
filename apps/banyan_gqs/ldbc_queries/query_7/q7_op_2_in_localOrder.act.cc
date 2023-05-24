//
// Created by everlighting on 2020/10/13.
//

#include "q7_op_2_in_localOrder.act.h"
#include "q7_op_2_friendStore.act.h"
#include "q7_op_3_globalSync_order_limit.act.h"

void q7_op_2_in_localOrder::process(VertexBatch &&input) {
    int64_t liker;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_in_v_with_property(input[i], _in_label);
        while (iter.next(liker)) {
            auto edge_creationDate_loc_info = iter.next_property(_creationDate_propId);
            auto like_date = storage::berkeleydb_helper::readLong(
                    std::get<0>(edge_creationDate_loc_info), std::get<1>(edge_creationDate_loc_info));
            _order_heap.emplace(input[i], liker, like_date);
            if (_order_heap.size() > _expect_num) {
                _order_heap.pop();
            }
        }
    }
}

void q7_op_2_in_localOrder::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        if (_notify_ds) {
            forward_results();
            _ds_group_hdl->receive_eos();
            _sync_hdl->receive_eos();
        } else {
            _sync_hdl->receive_eos(2);
        }
    }
}

void q7_op_2_in_localOrder::forward_results() {
    if (_order_heap.empty()) return;

    like_relation_Batch relations{static_cast<uint32_t>(_order_heap.size())};
    while (!_order_heap.empty()) {
        _ds_group_hdl->emplace_data<&i_q7_op_2_friendStore::filter_liker_within_friends>(
                locate_helper::locate(_order_heap.top().liker), _order_heap.top().liker);
        relations.emplace_back(_order_heap.top());
        _order_heap.pop();
    }
    _ds_group_hdl->flush<&i_q7_op_2_friendStore::filter_liker_within_friends>();
    _sync_hdl->Get()->record_like_relations(std::move(relations));
}

void q7_op_2_in_localOrder::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 3);
}

void q7_op_2_in_localOrder::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
