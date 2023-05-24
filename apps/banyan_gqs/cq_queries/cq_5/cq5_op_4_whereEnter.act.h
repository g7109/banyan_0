//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq5_op_3x0_whereIn;
class i_cq5_op_3x4_whereBranchSync;
class i_cq5_op_4_whereLeave;

class ANNOTATION(actor:reference) i_cq5_op_4_whereEnter : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void signal();

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq5_op_4_whereEnter);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq5_op_4_whereEnter);
};

class ANNOTATION(actor:implement) cq5_op_4_whereEnter : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void signal();

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq5_op_4_whereEnter);
    // Destructor
    ACTOR_IMPL_DTOR(cq5_op_4_whereEnter);
    // Do work
    ACTOR_DO_WORK();

private:
    brane::scope_builder _branch_scope_builder{brane::global_shard_id()};
    unsigned _cur_branch_id = brane::global_shard_id();
    unsigned _branch_num = 0;
    seastar::semaphore _sem{0};

    using branch_ds_t = i_cq5_op_3x0_whereIn;
    const unsigned branch_ds_op_id = 0;
    using branch_sync_t = i_cq5_op_3x4_whereBranchSync;
    const unsigned branch_sync_op_id = 4;
    using where_leave_t = downstream_handler<i_cq5_op_4_whereLeave, true>;
    where_leave_t* _where_leave_hdl = nullptr;
};
