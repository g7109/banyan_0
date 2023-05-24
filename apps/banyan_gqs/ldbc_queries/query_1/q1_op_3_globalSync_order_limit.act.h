#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/downstream_handlers.hh"

class ANNOTATION(actor:reference) i_q1_op_3_globalSync_order_limit : public brane::reference_base {
public:
    void process(vertex_pathLen_StrProp_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q1_op_3_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_q1_op_3_globalSync_order_limit);
};

class ANNOTATION(actor:implement) q1_op_3_globalSync_order_limit : public brane::stateful_actor {
public:
    void process(vertex_pathLen_StrProp_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(q1_op_3_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(q1_op_3_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();
    void e3_end_action();
    void e4_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    struct heap_node {
        uint8_t path_step_num;
        std::string lastName;
        int64_t id;

        heap_node(uint8_t m1_path_step_num, std::string&& m2_lastName, int64_t m3_id)
            : path_step_num(m1_path_step_num), lastName(m2_lastName), id(m3_id) {}
        heap_node(uint8_t m1_path_step_num, brane::cb::string m2_lastName, int64_t m3_id)
            : path_step_num(m1_path_step_num), lastName(m2_lastName.ptr, m2_lastName.len), id(m3_id) {}
        heap_node(const heap_node& hn) = default;
        heap_node(heap_node&& hn) noexcept
            : path_step_num(hn.path_step_num), lastName(std::move(hn.lastName)), id(hn.id) {}

        heap_node& operator=(const heap_node& hn) = delete;
        heap_node& operator=(heap_node&& hn) noexcept {
            if(this != &hn) {
                path_step_num = hn.path_step_num;
                lastName = std::move(hn.lastName);
                id = hn.id;
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const heap_node& a, const heap_node& b){
            if (a.path_step_num != b.path_step_num) {
                return a.path_step_num < b.path_step_num;
            } else if (a.lastName != b.lastName) {
                return a.lastName < b.lastName;
            } else {
                return a.id < b.id;
            }
        }
    };
    std::priority_queue<heap_node, std::vector<heap_node>, cmp> _order_heap;
    const unsigned _limit_num = 20;
};
