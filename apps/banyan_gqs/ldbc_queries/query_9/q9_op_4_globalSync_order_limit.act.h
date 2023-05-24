//
// Created by everlighting on 2020/8/7.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q9_op_4_propGet;

struct heap_node {
    int64_t msg_id;
    int64_t date;
    int64_t creator_id;

    heap_node(int64_t msg_id, int64_t date, int64_t creator_id)
            : msg_id(msg_id), date(date), creator_id(creator_id) {}
    heap_node(const heap_node& hn) = default;
    heap_node(heap_node&& hn) noexcept = default;

    heap_node& operator=(const heap_node& hn) = default;
    heap_node& operator=(heap_node&& hn) noexcept = default;
};
struct cmp{
    bool operator() (const heap_node& a, const heap_node& b){
        if (a.date != b.date) {
            return a.date > b.date;
        } else {
            return a.creator_id < b.creator_id;
        }
    }
};

class ANNOTATION(actor:reference) i_q9_op_4_globalSync_order_limit : public brane::reference_base {
public:
    void record_create_relations(create_relation_Batch&& input);
    void record_msg_contents(vertex_string_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q9_op_4_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_q9_op_4_globalSync_order_limit);
};

class ANNOTATION(actor:implement) q9_op_4_globalSync_order_limit : public brane::stateful_actor {
public:
    void record_create_relations(create_relation_Batch&& input);
    void record_msg_contents(vertex_string_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q9_op_4_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(q9_op_4_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void get_props();
    void log_results();
    void benchmark_end_action();
    void e3_end_action();
    void e4_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _at_stage2 = false;

    using prop_getter_group_t = downstream_group_handler<i_q9_op_4_propGet, VertexBatch>;
    prop_getter_group_t* _prop_getter_hdl = nullptr;

    std::unordered_map<int64_t, std::string> _msg_contents{};

    std::priority_queue<heap_node, std::vector<heap_node>, cmp> _order_heap;
    const unsigned _expect_num = 20;
    std::vector<heap_node> _results;
};
