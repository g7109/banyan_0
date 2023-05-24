//
// Created by everlighting on 2020/10/12.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q12_op_8_propGet;

class ANNOTATION(actor:reference) i_q12_op_7_filter_dedup : public brane::reference_base {
public:
    void process(vertex_tagId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q12_op_7_filter_dedup);
    // Destructor
    ACTOR_ITFC_DTOR(i_q12_op_7_filter_dedup);
};

class ANNOTATION(actor:implement) q12_op_7_filter_dedup : public brane::stateful_actor {
public:
    void process(vertex_tagId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q12_op_7_filter_dedup);
    // Destructor
    ACTOR_IMPL_DTOR(q12_op_7_filter_dedup);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_group_t = downstream_group_handler<i_q12_op_8_propGet, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    int64_t _name_propId = storage::berkeleydb_helper::getPropertyID("name");
    std::string _tagClass_name;

    std::unordered_set<int64_t> _successful_tags{};
};
