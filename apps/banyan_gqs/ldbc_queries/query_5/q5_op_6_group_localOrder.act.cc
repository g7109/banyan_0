//
// Created by everlighting on 2021/4/2.
//

#include "q5_op_6_group_localOrder.act.h"

void q5_op_6_group_localOrder::process(vertex_count_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_group_map.find(input[i].v_id) == _group_map.end()) {
            _group_map[input[i].v_id] = input[i].count;
        } else {
            _group_map[input[i].v_id] += input[i].count;
        }
    }
}

void q5_op_6_group_localOrder::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        order_forward();
        _ds_sync_hdl->receive_eos();
    }
}

void q5_op_6_group_localOrder::order_forward() {
    for (auto& iter: _group_map) {
        _order_heap.emplace(iter.first, iter.second);
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
    vertex_count_Batch output{static_cast<uint32_t>(_order_heap.size())};
    while (!_order_heap.empty()) {
        output.emplace_back(_order_heap.top());
        _order_heap.pop();
    }
    _ds_sync_hdl->Get()->process(std::move(output));
}

void q5_op_6_group_localOrder::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _ds_sync_hdl = new ds_sync_t(sync_builder, 7);
}

void q5_op_6_group_localOrder::finalize() {
    delete _ds_sync_hdl;
}
