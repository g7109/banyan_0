//
// Created by everlighting on 2020/9/16.
//

#include "q4_op_0_V.act.h"
#include "q4_op_1_out.act.h"
#include "berkeley_db/storage/bdb_graph_handler.hh"

void q4_op_0_V::trigger(StartVertex&& src) {
    _ds_group_hdl->emplace_data(locate_helper::locate(src.val), src.val);
    _ds_group_hdl->flush();
    _ds_group_hdl->receive_eos();
}

void q4_op_0_V::initialize() {
    auto builder = get_scope();
    _ds_group_hdl = new ds_group_t(builder, 1);
}

void q4_op_0_V::finalize() {
    delete _ds_group_hdl;
}