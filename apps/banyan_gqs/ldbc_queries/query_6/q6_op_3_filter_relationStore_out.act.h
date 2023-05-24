//
// Created by everlighting on 2020/11/12.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q6_op_4_dedup_propGet;
class i_q6_op_5_globalSync_group_order;

class ANNOTATION(actor:reference) i_q6_op_3_filter_relationStore_out : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void record_filter_tag(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q6_op_3_filter_relationStore_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q6_op_3_filter_relationStore_out);
};

class ANNOTATION(actor:implement) q6_op_3_filter_relationStore_out : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void record_filter_tag(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q6_op_3_filter_relationStore_out);
    // Destructor
    ACTOR_IMPL_DTOR(q6_op_3_filter_relationStore_out);
    // Do work
    ACTOR_DO_WORK();

private:
    void count_tags();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _at_stage2 = false;

    using ds_group_t = downstream_group_handler<i_q6_op_4_dedup_propGet, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    using stage2_ds_group_t = downstream_group_handler<i_q6_op_4_dedup_propGet, vertex_count_Batch>;
    stage2_ds_group_t* _stage2_ds_group_hdl = nullptr;

    std::unordered_set<int64_t> _tags_have_seen{};
    std::unordered_map<int64_t, std::unordered_set<int64_t> > _relations{};

    int64_t _filter_tag_id;
    bool _recorded = false;

    storage::base_predicate _prop_checker;
    std::string _filter_label = "Post";

    const std::string _out_label = "hasTag";
};
