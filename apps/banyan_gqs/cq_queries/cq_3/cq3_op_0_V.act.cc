//
// Created by everlighting on 2021/4/13.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq3_op_0_V.act.h"
#include "cq3_op_1_out.act.h"

void cq3_op_0_V::trigger(StartVertex&& src) {
    _ds_group_hdl->emplace_data(locate_helper::locate(src.val), src.val);
    _ds_group_hdl->flush();
    _ds_group_hdl->receive_eos();
}

void cq3_op_0_V::initialize() {
    auto builder = get_scope();
    _ds_group_hdl = new ds_group_t(builder, 1);
}

void cq3_op_0_V::finalize() {
    delete _ds_group_hdl;
}



