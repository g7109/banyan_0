//
// Created by everlighting on 2020/10/9.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_q8_op_4_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q8_op_3_filter_propGet : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q8_op_3_filter_propGet);
    // Destructor
    ACTOR_ITFC_DTOR(i_q8_op_3_filter_propGet);
};

class ANNOTATION(actor:implement) q8_op_3_filter_propGet : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q8_op_3_filter_propGet);
    // Destructor
    ACTOR_IMPL_DTOR(q8_op_3_filter_propGet);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_and_sync_t = downstream_handler<i_q8_op_4_globalSync_order_limit, true, create_relation_Batch>;
    ds_and_sync_t* _ds_sync_hdl = nullptr;

    storage::base_predicate _prop_checker;
    std::string _filter_label = "Comment";

    const long _prop_type_id = storage::berkeleydb_helper::getPropertyID("creationDate");
    const std::string _prop_out_label = "hasCreator";
};
