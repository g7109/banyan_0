//
// Created by everlighting on 2021/4/15.
//

#include "common/schedule_helpers.hh"
#include "cq4_op_2_whereEnter.act.h"
#include "cq4_op_1x0x0_where_loopOut.act.h"
#include "cq4_op_1x1_whereBranchSync.act.h"
#include "cq4_op_2_whereLeave.act.h"

void cq4_op_2_whereEnter::process(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        enter_where_branch_scope(&_branch_scope_builder, _cur_branch_id);
        auto branch_sync_ref = _branch_scope_builder.build_ref<branch_sync_t>(_branch_sync_op_id);
        enter_loop_scope(&_branch_scope_builder, _branch_ds_id);
        enter_loop_instance_scope(&_branch_scope_builder, 0);
        auto branch_ds_ref = _branch_scope_builder.build_ref<branch_ds_t>(0);
        _cur_branch_id += brane::global_shard_count();
        _branch_num++;
        _branch_scope_builder.back_to_parent_scope();
        _branch_scope_builder.back_to_parent_scope();
        _branch_scope_builder.back_to_parent_scope();

        auto sem_f = _sem.wait(1);
        if (sem_f.available()) {
            trigger_branch(&branch_ds_ref, &branch_sync_ref, input[i]);
        } else {
            auto func = [this, br_ds_ref = std::move(branch_ds_ref), friend_id = input[i],
                         br_sync_ref = std::move(branch_sync_ref)]
                    (const seastar::future_state<>&& state) mutable {
                trigger_branch(&br_ds_ref, &br_sync_ref, friend_id);
            };
            using continuationized_func =
            seastar::continuation<std::function<void(const seastar::future_state<>&&)> >;
            seastar::internal::set_callback(
                    sem_f,std::make_unique<continuationized_func>(std::move(func), this));
        }
    }
}

void cq4_op_2_whereEnter::receive_eos(Eos &&eos) {
    _where_leave_hdl->Get()->set_branch_num(TrivialStruct<unsigned>{_branch_num});
}

void cq4_op_2_whereEnter::signal() {
    _sem.signal(1);
}

void cq4_op_2_whereEnter::trigger_branch(cq4_op_2_whereEnter::branch_ds_t *br_ds_ref,
                                         cq4_op_2_whereEnter::branch_sync_t *br_sync_ref, int64_t v) {
    br_sync_ref->set_owner(TrivialStruct<int64_t>{v});

    VertexBatch ds_output{1};
    ds_output.push_back(v);
    br_ds_ref->process(std::move(ds_output));

    PathEos branch_init_eos;
    branch_init_eos.append_step(1, 0);
    br_ds_ref->receive_eos(std::move(branch_init_eos));
}

void cq4_op_2_whereEnter::initialize() {
    auto builder = get_scope();

    auto where_scope_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) - 1;
    auto where_leave_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);

    _where_leave_hdl = new where_leave_t(builder, where_leave_op_id);

    _branch_scope_builder = builder;
    enter_where_scope(&_branch_scope_builder, where_scope_id);

    _sem.signal(cq_max_si);
}

void cq4_op_2_whereEnter::finalize() {
    delete _where_leave_hdl;
}
