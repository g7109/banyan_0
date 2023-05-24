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

class ANNOTATION(actor:reference) i_q11_op_6_globalSync_order_limit : public brane::reference_base {
public:
    void record_relations(work_relation_Batch&& input);
    void record_organisation_names(vertex_string_Batch &&input);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_6_globalSync_order_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_6_globalSync_order_limit);
};

class ANNOTATION(actor:implement) q11_op_6_globalSync_order_limit : public brane::stateful_actor {
public:
    void record_relations(work_relation_Batch&& input);
    void record_organisation_names(vertex_string_Batch &&input);
    void receive_eos(Eos&& eos);
    void finish_query();

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_6_globalSync_order_limit);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_6_globalSync_order_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t> > > _relation_map;

    struct node{
        int64_t friend_vertex;
        int64_t start_work_date;
        std::string organ_name;
        node(int64_t friend_vertex, int64_t start_work_date, char* str_ptr, uint32_t str_len)
            : friend_vertex(friend_vertex), start_work_date(start_work_date), organ_name(str_ptr, str_len) {}
        node(const node&) = default;
        node(node&& n) noexcept
            : friend_vertex(n.friend_vertex), start_work_date(n.start_work_date)
            , organ_name(std::move(n.organ_name)) {}

        node& operator=(const node& n) = delete;
        node& operator=(node&& n) noexcept {
            if(this != &n) {
                friend_vertex = n.friend_vertex;
                start_work_date = n.start_work_date;
                organ_name = std::move(n.organ_name);
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const node& a, const node& b){
            if (a.start_work_date != b.start_work_date) {
                return a.start_work_date < b.start_work_date;
            } else if (a.friend_vertex != b.friend_vertex) {
                return a.friend_vertex < b.friend_vertex;
            } else {
                return a.organ_name > b.organ_name;
            }
        }
    };
    std::priority_queue<node, std::vector<node>, cmp> _order_heap;
    const unsigned _expect_num = 10;
};