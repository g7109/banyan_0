#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"

class ANNOTATION(actor:reference) i_q2_op_4_order_and_limit_display : public brane::reference_base {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_ITFC_CTOR(i_q2_op_4_order_and_limit_display);
    // Destructor
    ACTOR_ITFC_DTOR(i_q2_op_4_order_and_limit_display);
};

class ANNOTATION(actor:implement) q2_op_4_order_and_limit_display : public brane::stateful_actor {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_IMPL_CTOR(q2_op_4_order_and_limit_display);
    // Destructor
    ACTOR_IMPL_DTOR(q2_op_4_order_and_limit_display);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    struct heap_node {
        uint64_t creation_date;
        uint64_t id;

        heap_node(uint64_t m1_date, uint64_t m2_id) : creation_date(m1_date), id(m2_id) {}
        heap_node(const heap_node& hn) = default;
        heap_node(heap_node&& hn) noexcept : creation_date(hn.creation_date), id(hn.id) {}

        heap_node& operator=(const heap_node& hn) = delete;
        heap_node& operator=(heap_node&& hn) noexcept {
            if(this != &hn) {
                creation_date = hn.creation_date;
                id = hn.id;
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const heap_node& a, const heap_node& b){
            if (a.creation_date != b.creation_date) {
                return a.creation_date > b.creation_date;
            } else {
                return a.id < b.id;
            }
        }
    };
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    const unsigned _expect_num = 20;
    std::priority_queue<heap_node, std::vector<heap_node>, cmp> _order_heap;
};
