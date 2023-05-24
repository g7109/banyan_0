//
// Created by everlighting on 2020/10/9.
//
#pragma once

#include <unordered_map>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"

class ANNOTATION(actor:reference) i_q8_op_4_globalSync_order_limit : public brane::reference_base {
public:
    void process(create_relation_Batch&& input);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_ITFC_CTOR(i_q8_op_4_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_q8_op_4_globalSync_order_limit);
};

class ANNOTATION(actor:implement) q8_op_4_globalSync_order_limit : public brane::stateful_actor {
public:
    void process(create_relation_Batch&& input);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_IMPL_CTOR(q8_op_4_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(q8_op_4_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    struct node{
        int64_t comment;
        int64_t commenter;
        int64_t creation_date;
        node(int64_t comment, int64_t commenter, int64_t creation_date)
                : comment(comment), commenter(commenter), creation_date(creation_date) {}
        node(const node&) = default;
        node(node&& n) noexcept
                : comment(n.comment), commenter(n.commenter), creation_date(n.creation_date) {}

        node& operator=(const node& n) = delete;
        node& operator=(node&& n) noexcept {
            if(this != &n) {
                comment = n.comment;
                commenter = n.commenter;
                creation_date = n.creation_date;
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const node& a, const node& b){
            if(a.creation_date != b.creation_date) {
                return a.creation_date > b.creation_date;
            } else {
                return a.comment < b.comment;
            }
        }
    };
    const unsigned _expect_num = 20;
    std::priority_queue<node, std::vector<node>, cmp> _order_heap;
};

