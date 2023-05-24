//
// Created by everlighting on 2020/9/28.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include <unordered_map>

struct cmp{
    bool operator() (const vertex_count& a, const vertex_count& b){
        if(a.count != b.count) {
            return a.count > b.count;
        } else {
            return a.v_id < b.v_id;
        }
    }
};

class ANNOTATION(actor:reference) i_q5_op_7_globalSync_order : public brane::reference_base {
public:
    void process(vertex_count_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q5_op_7_globalSync_order);
    // Destructor
    ACTOR_ITFC_DTOR(i_q5_op_7_globalSync_order);
};

class ANNOTATION(actor:implement) q5_op_7_globalSync_order : public brane::stateful_actor {
public:
    void process(vertex_count_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(q5_op_7_globalSync_order);
    // Destructor
    ACTOR_IMPL_DTOR(q5_op_7_globalSync_order);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    const unsigned _expect_num = 20;
    std::priority_queue<vertex_count, std::vector<vertex_count>, cmp> _order_heap;
};
