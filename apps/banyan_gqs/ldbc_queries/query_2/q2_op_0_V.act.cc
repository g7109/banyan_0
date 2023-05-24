#include "q2_op_0_V.act.h"
#include "q2_op_1_out.act.h"

void q2_op_0_V::trigger(StartVertex&& src) {
    _ds_hdl->process(std::move(src));
    _ds_hdl->receive_eos();
}

void q2_op_0_V::initialize() {
    auto builder = get_scope();
    _ds_hdl = new ds_t(builder, 1);
}

void q2_op_0_V::finalize() {
    delete _ds_hdl;
}



