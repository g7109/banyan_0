//
// Created by everlighting on 2020/10/12.
//

#pragma once

#include <unordered_map>
#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/data_unit_type.hh"

class ANNOTATION(actor:reference) i_q12_op_9_globalSync_group_order : public brane::reference_base {
public:
    void record_tag_names(vertex_string_Batch&& input);
    void record_filtered_relations(person_comment_tag_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q12_op_9_globalSync_group_order);
    // Destructor
    ACTOR_ITFC_DTOR(i_q12_op_9_globalSync_group_order);
};

class ANNOTATION(actor:implement) q12_op_9_globalSync_group_order : public brane::stateful_actor {
public:
    void record_tag_names(vertex_string_Batch&& input);
    void record_filtered_relations(person_comment_tag_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_IMPL_CTOR(q12_op_9_globalSync_group_order);
    // Destructor
    ACTOR_IMPL_DTOR(q12_op_9_globalSync_group_order);
    // Do work
    ACTOR_DO_WORK();

private:
    void log_results();
    void benchmark_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count() * 2;

    std::unordered_map<int64_t, std::unordered_set<int64_t> > _person_reply_map{};
    std::unordered_map<int64_t, std::unordered_set<int64_t> > _person_tag_map{};
    std::unordered_map<int64_t, std::string> _tag_names{};

    struct order_node {
        int64_t friend_vertex;
        uint32_t reply_count;
        std::unordered_set<int64_t> tag_ids;

        order_node(int64_t friend_vertex, uint32_t reply_count, std::unordered_set<int64_t>&& tag_ids)
            : friend_vertex(friend_vertex), reply_count(reply_count), tag_ids(std::move(tag_ids)) {}
        order_node(const order_node&) = default;
        order_node(order_node&& n) noexcept
            : friend_vertex(n.friend_vertex), reply_count(n.reply_count)
            , tag_ids(std::move(n.tag_ids)) {}

        order_node& operator=(const order_node& n) = delete;
        order_node& operator=(order_node&& n) noexcept {
            if(this != &n) {
                friend_vertex = n.friend_vertex;
                reply_count = n.reply_count;
                tag_ids = std::move(n.tag_ids);
            }
            return *this;
        }
    };
    struct cmp{
        bool operator() (const order_node& a, const order_node& b){
            if(a.reply_count != b.reply_count) {
                return a.reply_count > b.reply_count;
            } else {
                return a.friend_vertex < b.friend_vertex;
            }
        }
    };
    std::priority_queue<order_node, std::vector<order_node>, cmp> _order_heap;
    const unsigned _limit_num = 20;
};
