//
// Created by everlighting on 2020/9/17.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/props_predictor.hh"
#include "common/downstream_handlers.hh"

class i_q4_op_5_globalSync_order;

class ANNOTATION(actor:reference) i_q4_op_4_filter_propGet : public brane::reference_base {
public:
    void record_excluded_tags(VertexBatch&& input);
    void record_candidate_tags(VertexBatch&& input);
    void process();
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q4_op_4_filter_propGet);
    // Destructor
    ACTOR_ITFC_DTOR(i_q4_op_4_filter_propGet);
};

class ANNOTATION(actor:implement) q4_op_4_filter_propGet : public brane::stateful_actor {
public:
    void record_excluded_tags(VertexBatch&& input);
    void record_candidate_tags(VertexBatch&& input);
    void process();
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q4_op_4_filter_propGet);
    // Destructor
    ACTOR_IMPL_DTOR(q4_op_4_filter_propGet);
    // Do work
    ACTOR_DO_WORK();

private:
    void filter_propGet_forward();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_sync_t = downstream_handler<i_q4_op_5_globalSync_order, true, count_name_Batch>;
    ds_sync_t* _ds_sync_hdl = nullptr;

    std::unordered_set<int64_t> _excluded_tags{};
    std::unordered_map<int64_t, unsigned> _candidate_tag_counts{};

    const long _name_propId = storage::berkeleydb_helper::getPropertyID("name");
};