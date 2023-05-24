//
// Created by everlighting on 2021/4/13.
//

#include "cq3_op_4_dedup_whereEnter.act.h"
#include "cq3_op_4_whereLeave_localOrder.act.h"
#include "cq3_op_6_globalSync_order_limit.act.h"

void cq3_op_4_whereLeave_localOrder::process(TrivialStruct<int64_t> &&input) {
    _order_heap.push(input.val);
    if (_order_heap.size() > _expect_num) {
        _order_heap.pop();
    }
}

void cq3_op_4_whereLeave_localOrder::set_branch_num(TrivialStruct<unsigned int> &&branch_num) {
    _expect_eos_num = branch_num.val;
    _eos_num_set = true;
    if (_eos_rcv_num == _expect_eos_num) {
        forward_results();
        _ds_sync_hdl->receive_eos();
    }
}

void cq3_op_4_whereLeave_localOrder::receive_eos(Eos &&eos) {
    ++_eos_rcv_num;
    _where_enter_hdl->Get()->signal();
    if (_eos_num_set && _eos_rcv_num == _expect_eos_num) {
        forward_results();
        _ds_sync_hdl->receive_eos();
    }
}

void cq3_op_4_whereLeave_localOrder::forward_results() {
    if (_order_heap.empty()) { return; }

    VertexBatch output{static_cast<uint32_t>(_order_heap.size())};
    while (!_order_heap.empty()) {
        output.push_back(_order_heap.top());
        _order_heap.pop();
    }
    _ds_sync_hdl->process(std::move(output));
}

void cq3_op_4_whereLeave_localOrder::initialize() {
    auto builder = get_scope();

    auto where_enter_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);
    _where_enter_hdl = new where_enter_t(builder, where_enter_op_id);

    builder.reset_shard(0);
    _ds_sync_hdl = new ds_and_sync_t(builder, 6);
}

void cq3_op_4_whereLeave_localOrder::finalize() {
    delete _where_enter_hdl;
    delete _ds_sync_hdl;
}
