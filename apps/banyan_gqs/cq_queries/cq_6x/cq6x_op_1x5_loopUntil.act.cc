//
// Created by everlighting on 2021/4/19.
//

#include "common/schedule_helpers.hh"
#include "cq6x_op_1x5_loopUntil.act.h"
#include "cq6x_op_1x0_loopOut.act.h"
#include "cq6x_op_2_loopLeave_dedup.act.h"

void cq6x_op_1x5_loopUntil::process(VertexBatch&& input) {
    if (_cur_loop_times < _max_loop_times) {
        _next_loop_hdl->process(std::move(input));
    } else {
        _loop_sync_hdl->process(std::move(input));
    }
}

void cq6x_op_1x5_loopUntil::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _next_loop_hdl->receive_eos() : _loop_sync_hdl->receive_eos();
    }
}

void cq6x_op_1x5_loopUntil::initialize() {
    auto builder = get_scope();

    _cur_loop_times = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    enter_loop_instance_scope(&builder, _cur_loop_times);
    _next_loop_hdl = new next_loop_t(builder, 0);

    builder.back_to_parent_scope();
    auto sync_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    _loop_sync_hdl = new loop_sync_t(builder, sync_op_id);
}

void cq6x_op_1x5_loopUntil::finalize() {
    delete _next_loop_hdl;
    delete _loop_sync_hdl;
}
