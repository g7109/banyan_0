//
// Created by everlighting on 2020/9/28.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "q5_op_7_globalSync_order.act.h"

class ANNOTATION(actor:reference) i_q5_op_6_group_localOrder : public brane::reference_base {
public:
    void process(vertex_count_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q5_op_6_group_localOrder);
    // Destructor
    ACTOR_ITFC_DTOR(i_q5_op_6_group_localOrder);
};

class ANNOTATION(actor:implement) q5_op_6_group_localOrder : public brane::stateful_actor {
public:
    void process(vertex_count_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q5_op_6_group_localOrder);
    // Destructor
    ACTOR_IMPL_DTOR(q5_op_6_group_localOrder);
    // Do work
    ACTOR_DO_WORK();

private:
    void order_forward();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_sync_t = downstream_handler<i_q5_op_7_globalSync_order, true>;
    ds_sync_t* _ds_sync_hdl = nullptr;

    const unsigned _expect_num = 20;
    std::unordered_map<int64_t, unsigned> _group_map;
    std::priority_queue<vertex_count, std::vector<vertex_count>, cmp> _order_heap;
};
