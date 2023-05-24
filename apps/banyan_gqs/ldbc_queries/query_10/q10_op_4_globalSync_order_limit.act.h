//
// Created by everlighting on 2020/8/7.
//
#pragma once

#include <unordered_map>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"

class ANNOTATION(actor:reference) i_q10_op_4_globalSync_order_limit : public brane::reference_base {
public:
    void record_post_count(vertex_intProp_Batch&& input);
    void count_common(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q10_op_4_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_q10_op_4_globalSync_order_limit);
};

class ANNOTATION(actor:implement) q10_op_4_globalSync_order_limit : public brane::stateful_actor {
public:
    void record_post_count(vertex_intProp_Batch&& input);
    void count_common(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(q10_op_4_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(q10_op_4_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();
    void e4_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count() * 2;

    std::unordered_map<int64_t, int64_t> _count_map{};
    std::unordered_map<int64_t, int64_t> _common_map{};

    struct heap_node {
        int64_t id;
        int64_t score;

        heap_node(int64_t m1_id, int64_t m2_score)
                : id(m1_id), score(m2_score) {}
        heap_node(const heap_node& hn) = default;
        heap_node(heap_node&& hn) noexcept
                : id(hn.id), score(hn.score) {}

        heap_node& operator=(const heap_node& hn) = delete;
        heap_node& operator=(heap_node&& hn) noexcept {
            if(this != &hn) {
                id = hn.id;
                score = hn.score;
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const heap_node& a, const heap_node& b){
            if (a.score != b.score) {
                return a.score > b.score;
            } else {
                return a.id < b.id;
            }
        }
    };
    const unsigned _expect_num = 10;
    std::priority_queue<heap_node, std::vector<heap_node>, cmp> _order_heap;
};
