//
// Created by everlighting on 2021/4/15.
//

#include "cq4_op_1x0x1_where_loopUntil.act.h"
#include "cq4_op_2_whereLeave.act.h"
#include "cq4_op_2_whereEnter.act.h"
#include "cq4_op_3_globalSync.act.h"

void cq4_op_2_whereLeave::process(TrivialStruct<int64_t> &&input) {
    _ds_sync_hdl->process(std::move(input));
}

void cq4_op_2_whereLeave::set_branch_num(TrivialStruct<unsigned int> &&branch_num) {
    _expect_eos_num = branch_num.val;
    _eos_num_set = true;
    if (_eos_rcv_num == _expect_eos_num) {
        _ds_sync_hdl->receive_eos();
    }
}

void cq4_op_2_whereLeave::receive_eos(Eos &&eos) {
    ++_eos_rcv_num;
    _where_enter_hdl->Get()->signal();
    if (_eos_num_set && _eos_rcv_num == _expect_eos_num) {
        _ds_sync_hdl->receive_eos();
    }
}

void cq4_op_2_whereLeave::initialize() {
    auto builder = get_scope();

    auto where_enter_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);
    _where_enter_hdl = new where_enter_t(builder, where_enter_op_id);

    builder.reset_shard(0);
    _ds_sync_hdl = new ds_and_sync_t(builder, 3);
}

void cq4_op_2_whereLeave::finalize() {
    cq4_op_1x0x1_where_loopUntil::_successful_vertices.clear();
    cq4_op_1x0x1_where_loopUntil::_failed_vertices.clear();
    delete _where_enter_hdl;
    delete _ds_sync_hdl;
}

