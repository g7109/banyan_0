//
// Created by everlighting on 2020/10/9.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q11_op_4_filter_dedup;
class i_q11_op_6_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q11_op_3_relationStore_dedup_out : public brane::reference_base {
public:
    void process(work_relation_Batch&& input);
    void receive_eos(Eos&& eos);

    void forward_filtered_relations(vertex_string_Batch&& input);
    void receive_stage2_eos();

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_3_relationStore_dedup_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_3_relationStore_dedup_out);
};

class ANNOTATION(actor:implement) q11_op_3_relationStore_dedup_out : public brane::stateful_actor {
public:
    void process(work_relation_Batch&& input);
    void receive_eos(Eos&& eos);

    void forward_filtered_relations(vertex_string_Batch&& input);
    void receive_stage2_eos();

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_3_relationStore_dedup_out);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_3_relationStore_dedup_out);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_q11_op_4_filter_dedup, vertex_organisationId_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q11_op_6_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const std::string _out_label = "isLocatedIn";

    std::unordered_set<int64_t> _org_have_seen{};
    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t> > > _relation_map;
};

