//
// Created by everlighting on 2020/9/27.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include <unordered_map>

class ANNOTATION(actor:reference) i_q3_op_8_group_and_order : public brane::reference_base {
public:
    void process(q3_result_node_Batch&& results);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q3_op_8_group_and_order);
    // Destructor
    ACTOR_ITFC_DTOR(i_q3_op_8_group_and_order);
};

class ANNOTATION(actor:implement) q3_op_8_group_and_order : public brane::stateful_actor {
public:
    void process(q3_result_node_Batch&& results);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(q3_op_8_group_and_order);
    // Destructor
    ACTOR_IMPL_DTOR(q3_op_8_group_and_order);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    std::unordered_map<int64_t, q3_result_node> _group_map;
    struct cmp{
        bool operator() (const q3_result_node& a, const q3_result_node& b){
            if(a.x_count != b.x_count) {
                return a.x_count > b.x_count;
            } else {
                return a.v_id < b.v_id;
            }
        }
    };
    const unsigned _expect_num = 20;
    std::priority_queue<q3_result_node, std::vector<q3_result_node>, cmp> _order_heap;
};
