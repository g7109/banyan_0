#include "common/schedule_helpers.hh"
#include "cq6_op_0_V_loopEnter.act.h"
#include "cq6_op_1x0_loopOut.act.h"

void cq6_op_0_V_loopEnter::trigger(StartVertex&& src) {
    _ds_group_hdl->emplace_data(brane::global_shard_id(), src.val);
    _ds_group_hdl->flush();
    _ds_group_hdl->receive_eos();
}

void cq6_op_0_V_loopEnter::initialize() {
    auto builder = get_scope();
    enter_loop_scope(&builder, 1);
    enter_loop_instance_scope(&builder, 0);
    _ds_group_hdl = new ds_group_t(builder, 0, 1);
}

void cq6_op_0_V_loopEnter::finalize() {
    delete _ds_group_hdl;
}
