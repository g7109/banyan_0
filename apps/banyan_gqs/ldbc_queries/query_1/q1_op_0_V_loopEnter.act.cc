#include "q1_op_0_V_loopEnter.act.h"
#include "q1_op_1x0_loopOut.act.h"

void q1_op_0_V_loopEnter::trigger(StartVertex&& src) {
    _ds_group_hdl->emplace_data(brane::global_shard_id(), src.val, 0);
    _ds_group_hdl->flush();
    _ds_group_hdl->receive_eos();
}

void q1_op_0_V_loopEnter::initialize() {
    auto builder = get_scope();
    builder.enter_sub_scope(brane::scope<brane::actor_group<> >(1));
    builder.enter_sub_scope(brane::scope<brane::actor_group<> >(0));
    _ds_group_hdl = new ds_group_t(builder, 0, 1);
}

void q1_op_0_V_loopEnter::finalize() {
    delete _ds_group_hdl;
}



