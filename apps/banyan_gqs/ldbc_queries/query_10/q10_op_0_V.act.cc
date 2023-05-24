#include "q10_op_0_V.act.h"
#include "q10_op_1_out_sideOut.act.h"

void q10_op_0_V::trigger(StartVertex &&src) {
    _ds_group_hdl->emplace_data(brane::global_shard_id(), src.val);
    _ds_group_hdl->flush();
    _ds_group_hdl->receive_eos();
}

void q10_op_0_V::initialize() {
    auto builder = get_scope();
    _ds_group_hdl = new ds_group_t(builder, 1, 1);
}

void q10_op_0_V::finalize() {
    delete _ds_group_hdl;
}



