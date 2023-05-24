//
// Created by everlighting on 2020/10/9.
//

#include "q8_op_0_V.act.h"
#include "q8_op_1_in.act.h"

void q8_op_0_V::trigger(StartVertex &&src) {
    _ds_hdl->process(std::move(src));
    _ds_hdl->receive_eos();
}

void q8_op_0_V::initialize() {
    auto builder = get_scope();
    _ds_hdl = new ds_t(builder, 1);
}

void q8_op_0_V::finalize() {
    delete _ds_hdl;
}
