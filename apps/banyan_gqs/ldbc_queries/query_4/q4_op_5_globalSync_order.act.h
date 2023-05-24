//
// Created by everlighting on 2020/9/17.
//
#pragma once

#include <unordered_map>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"

class ANNOTATION(actor:reference) i_q4_op_5_globalSync_order : public brane::reference_base {
public:
    void process(count_name_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q4_op_5_globalSync_order);
    // Destructor
    ACTOR_ITFC_DTOR(i_q4_op_5_globalSync_order);
};

class ANNOTATION(actor:implement) q4_op_5_globalSync_order : public brane::stateful_actor {
public:
    void process(count_name_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(q4_op_5_globalSync_order);
    // Destructor
    ACTOR_IMPL_DTOR(q4_op_5_globalSync_order);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    struct node{
        unsigned count;
        std::string tag_name;
        node(unsigned count, char* str_ptr, uint32_t str_len)
            : count(count), tag_name(str_ptr, str_len) {}
        node(const node&) = default;
        node(node&& n) noexcept
            : count(n.count), tag_name(std::move(n.tag_name)) {}

        node& operator=(const node& n) = delete;
        node& operator=(node&& n) noexcept {
            if(this != &n) {
                count = n.count;
                tag_name = std::move(n.tag_name);
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const node& a, const node& b){
            if(a.count != b.count) {
                return a.count > b.count;
            } else {
                return a.tag_name < b.tag_name;
            }
        }
    };
    std::priority_queue<node, std::vector<node>, cmp> _order_heap;
    const unsigned _expect_num = 10;
};
