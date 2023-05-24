//
// Created by everlighting on 2020/8/10.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq6_op_1x0_loopOut;
class i_cq6_op_2_loopLeave_dedup;

class ANNOTATION(actor:reference) i_cq6_op_1x3_loopUntil : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6_op_1x3_loopUntil);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6_op_1x3_loopUntil);
};

class ANNOTATION(actor:implement) cq6_op_1x3_loopUntil : public brane::stateless_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq6_op_1x3_loopUntil);
    // Destructor
    ACTOR_IMPL_DTOR(cq6_op_1x3_loopUntil);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    const unsigned _max_loop_times = 5;
    unsigned _cur_loop_times = 0;

    using next_loop_t = downstream_handler<i_cq6_op_1x0_loopOut, false, VertexBatch>;
    using loop_sync_t = downstream_handler<i_cq6_op_2_loopLeave_dedup, true, VertexBatch>;
    next_loop_t* _next_loop_hdl = nullptr;
    loop_sync_t* _loop_sync_hdl = nullptr;
};
