//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/downstream_handlers.hh"

class ANNOTATION(actor:reference) i_cq3_op_6_globalSync_order_limit : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3_op_6_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3_op_6_globalSync_order_limit);
};

class ANNOTATION(actor:implement) cq3_op_6_globalSync_order_limit : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(cq3_op_6_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(cq3_op_6_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void cq_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    std::priority_queue<int64_t, std::vector<int64_t>, std::less<int64_t> > _order_heap;
    const unsigned _expect_num = 10;
};
