//
// Created by everlighting on 2021/4/16.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq6x_op_1x2_loopOut;

class ANNOTATION(actor:reference) i_cq6x_op_1x1_dedup_loopIn : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6x_op_1x1_dedup_loopIn);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6x_op_1x1_dedup_loopIn);
};

class ANNOTATION(actor:implement) cq6x_op_1x1_dedup_loopIn : public brane::stateful_actor {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq6x_op_1x1_dedup_loopIn);
    // Destructor
    ACTOR_IMPL_DTOR(cq6x_op_1x1_dedup_loopIn);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    const std::string _in_label = "hasCreator";

    std::unordered_set<int64_t> _friends_have_seen{};

    using ds_group_t = downstream_group_handler<i_cq6x_op_1x2_loopOut, vertex_friendId_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;
};