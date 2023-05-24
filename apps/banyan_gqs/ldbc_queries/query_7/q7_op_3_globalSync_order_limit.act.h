//
// Created by everlighting on 2020/10/13.
//
#pragma once

#include <unordered_map>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q7_op_3_propGet;

struct cmp{
    bool operator() (const like_relation& a, const like_relation& b){
        if(a.like_date != b.like_date) {
            return a.like_date > b.like_date;
        } else {
            return a.liker < b.liker;
        }
    }
};

class ANNOTATION(actor:reference) i_q7_op_3_globalSync_order_limit : public brane::reference_base {
public:
    void record_msg_props(vertex_IntProp_StrProp_Batch&& input);
    void record_like_relations(like_relation_Batch&& input);
    void record_likers_within_friends(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_ITFC_CTOR(i_q7_op_3_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_q7_op_3_globalSync_order_limit);
};

class ANNOTATION(actor:implement) q7_op_3_globalSync_order_limit : public brane::stateful_actor {
public:
    void record_msg_props(vertex_IntProp_StrProp_Batch&& input);
    void record_like_relations(like_relation_Batch&& input);
    void record_likers_within_friends(VertexBatch&& input);
    void receive_eos(Eos&& eos);
    void finish_query();

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q7_op_3_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(q7_op_3_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void get_props();
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    unsigned _expect_eos_num = brane::global_shard_count() * 2;
    bool _at_stage2 = false;

    using prop_getter_group_t = downstream_group_handler<i_q7_op_3_propGet, VertexBatch>;
    prop_getter_group_t* _prop_getter_hdl = nullptr;

    std::unordered_map<int64_t, int64_t> _msg_creationDates;
    std::unordered_map<int64_t, std::string> _msg_contents;
    std::unordered_set<int64_t> _likers_within_friends;

    const unsigned _expect_num = 20;
    std::priority_queue<like_relation, std::vector<like_relation>, cmp> _order_heap;
    std::vector<like_relation> _results;
};
