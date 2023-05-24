//
// Created by everlighting on 2021/4/15.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq4_op_1x0x0_where_loopOut;
class i_cq4_op_1x1_whereBranchSync;
class i_cq4_op_2_whereLeave;

class ANNOTATION(actor:reference) i_cq4_op_2_whereEnter : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void signal();

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq4_op_2_whereEnter);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq4_op_2_whereEnter);
};

class ANNOTATION(actor:implement) cq4_op_2_whereEnter : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void signal();

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq4_op_2_whereEnter);
    // Destructor
    ACTOR_IMPL_DTOR(cq4_op_2_whereEnter);
    // Do work
    ACTOR_DO_WORK();

private:
    brane::scope_builder _branch_scope_builder{brane::global_shard_id()};
    unsigned _cur_branch_id = brane::global_shard_id();
    unsigned _branch_num = 0;
    seastar::semaphore _sem{0};

    using branch_ds_t = i_cq4_op_1x0x0_where_loopOut;
    const unsigned _branch_ds_id = 0;
    using branch_sync_t = i_cq4_op_1x1_whereBranchSync;
    const unsigned _branch_sync_op_id = 1;
    using where_leave_t = downstream_handler<i_cq4_op_2_whereLeave, true>;
    where_leave_t* _where_leave_hdl = nullptr;

private:
    void trigger_branch(branch_ds_t* br_ds_ref, branch_sync_t* br_sync_ref, int64_t v);
};

