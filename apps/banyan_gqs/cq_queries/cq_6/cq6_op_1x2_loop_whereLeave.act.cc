//
// Created by everlighting on 2021/4/13.
//
#include "cq6_op_1x2_loop_dedup_whereEnter.act.h"
#include "cq6_op_1x2_loop_whereLeave.act.h"
#include "cq6_op_1x3_loopUntil.act.h"

void cq6_op_1x2_whereLeave_localOrder::process(TrivialStruct<int64_t> &&input) {
    _ds_group_hdl->emplace_data(locate_helper::locate(input.val), input.val);
    _ds_group_hdl->flush();
}

void cq6_op_1x2_whereLeave_localOrder::set_branch_num(TrivialStruct<unsigned int> &&branch_num) {
    _expect_eos_num = branch_num.val;
    _eos_num_set = true;
    if (_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void cq6_op_1x2_whereLeave_localOrder::receive_eos(Eos &&eos) {
    ++_eos_rcv_num;
    _where_enter_hdl->Get()->signal();
    if (_eos_num_set && _eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void cq6_op_1x2_whereLeave_localOrder::initialize() {
    auto builder = get_scope();

    auto where_enter_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);
    _where_enter_hdl = new where_enter_t(builder, where_enter_op_id);

    auto ds_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_op_id);
}

void cq6_op_1x2_whereLeave_localOrder::finalize() {
    delete _where_enter_hdl;
    delete _ds_group_hdl;
}
