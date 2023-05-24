//
// Created by everlighting on 2020/10/13.
//

#include "q7_op_0_V.act.h"
#include "q7_op_1_in_sideOut.act.h"

void q7_op_0_V::trigger(StartVertex &&src) {
    _ds_hdl->process(std::move(src));
    _ds_hdl->receive_eos();
}

void q7_op_0_V::initialize() {
    auto builder = get_scope();
    _ds_hdl = new ds_t(builder, 1);
}

void q7_op_0_V::finalize() {
    delete _ds_hdl;
}

