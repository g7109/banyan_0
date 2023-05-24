//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq6_op_1x1x0_whereIn;
class i_cq6_op_1x1x4_whereBranchSync;
class i_cq6_op_1x2_whereLeave_localOrder;
class i_cq6_op_1x3_loopUntil;

class ANNOTATION(actor:reference) i_cq6_op_1x0_loop_dedup_whereEnter : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void signal();

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6_op_1x0_loop_dedup_whereEnter);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6_op_1x0_loop_dedup_whereEnter);
};

class ANNOTATION(actor:implement) cq6_op_1x0_loop_dedup_whereEnter : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void signal();

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq6_op_1x0_loop_dedup_whereEnter);
    // Destructor
    ACTOR_IMPL_DTOR(cq6_op_1x0_loop_dedup_whereEnter);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    brane::scope_builder _branch_scope_builder{brane::global_shard_id()};
    unsigned _cur_branch_id = brane::global_shard_id();
    unsigned _branch_num = 0;
    seastar::semaphore _sem{0};

    std::unordered_set<int64_t> _vertices_have_seen{};

    using branch_ds_t = i_cq6_op_1x1x0_whereIn;
    const unsigned branch_ds_op_id = 0;
    using branch_sync_t = i_cq6_op_1x1x4_whereBranchSync;
    const unsigned branch_sync_op_id = 4;
    using where_leave_t = downstream_handler<i_cq6_op_1x2_whereLeave_localOrder, true>;
    where_leave_t* _where_leave_hdl = nullptr;
};
