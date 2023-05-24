//
// Created by everlighting on 2021/4/13.
//

#include "common/schedule_helpers.hh"
#include "cq3_op_4_dedup_whereEnter.act.h"
#include "cq3_op_3x0_whereIn.act.h"
#include "cq3_op_3x4_whereBranchSync.act.h"
#include "cq3_op_4_whereLeave_localOrder.act.h"
#include "cq3_op_6_globalSync_order_limit.act.h"

void cq3_op_4_dedup_whereEnter::process(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_friends_have_seen.find(input[i]) == _friends_have_seen.end()) {
            _friends_have_seen.insert(input[i]);

            enter_where_branch_scope(&_branch_scope_builder, _cur_branch_id);
            auto branch_ds_ref = _branch_scope_builder.build_ref<branch_ds_t>(branch_ds_op_id);
            auto branch_sync_ref = _branch_scope_builder.build_ref<branch_sync_t>(branch_sync_op_id);
            _cur_branch_id += brane::global_shard_count();
            _branch_num++;
            _branch_scope_builder.back_to_parent_scope();

            auto sem_f = _sem.wait(1);
            if (sem_f.available()) {
                branch_sync_ref.set_owner(TrivialStruct<int64_t>{input[i]});
                branch_ds_ref.process(TrivialStruct<int64_t>{input[i]});
            } else {
                auto func = [br_ds_ref = std::move(branch_ds_ref), friend_id = input[i],
                        br_sync_ref = std::move(branch_sync_ref)]
                        (const seastar::future_state<>&& state) mutable {
                    br_sync_ref.set_owner(TrivialStruct<int64_t>{friend_id});
                    br_ds_ref.process(TrivialStruct<int64_t>{friend_id});
                };
                using continuationized_func =
                seastar::continuation<std::function<void(const seastar::future_state<>&&)> >;
                seastar::internal::set_callback(
                        sem_f,std::make_unique<continuationized_func>(std::move(func), this));
            }
        }
    }
}

void cq3_op_4_dedup_whereEnter::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ?
            _where_leave_hdl->Get()->set_branch_num(TrivialStruct<unsigned>{_branch_num}) :
            _sync_hdl->receive_eos();
    }
}

void cq3_op_4_dedup_whereEnter::signal() {
    _sem.signal(1);
}

void cq3_op_4_dedup_whereEnter::initialize() {
    auto builder = get_scope();

    auto where_scope_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) - 1;
    auto where_leave_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);

    _where_leave_hdl = new where_leave_t(builder, where_leave_op_id);

    _branch_scope_builder = builder;
    enter_where_scope(&_branch_scope_builder, where_scope_id);

    builder.reset_shard(0);
    _sync_hdl = new sync_t(builder, 6);

    _sem.signal(cq_max_si);
}

void cq3_op_4_dedup_whereEnter::finalize() {
    delete _where_leave_hdl;
    delete _sync_hdl;
}

