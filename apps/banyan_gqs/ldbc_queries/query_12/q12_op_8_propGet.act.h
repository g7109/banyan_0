//
// Created by everlighting on 2020/10/12.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_q12_op_4_filter_and_out;
class i_q12_op_9_globalSync_group_order;

class ANNOTATION(actor:reference) i_q12_op_8_propGet : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q12_op_8_propGet);
    // Destructor
    ACTOR_ITFC_DTOR(i_q12_op_8_propGet);
};

class ANNOTATION(actor:implement) q12_op_8_propGet : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q12_op_8_propGet);
    // Destructor
    ACTOR_IMPL_DTOR(q12_op_8_propGet);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward_tag_names();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_group_t = downstream_group_handler<i_q12_op_4_filter_and_out, VertexBatch>;
    const unsigned _ds_op_id = 4;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q12_op_9_globalSync_group_order, true>;
    sync_t* _sync_hdl = nullptr;

    const long _prop_type_id = storage::berkeleydb_helper::getPropertyID("name");

    std::unordered_set<int64_t> _tags_have_seen{};
    std::unordered_map<int64_t, std::string> _tag_names;
};
