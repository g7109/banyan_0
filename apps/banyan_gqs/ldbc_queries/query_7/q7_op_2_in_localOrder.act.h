//
// Created by everlighting on 2020/10/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"
#include "q7_op_3_globalSync_order_limit.act.h"

class i_q7_op_2_friendStore;

class ANNOTATION(actor:reference) i_q7_op_2_in_localOrder : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q7_op_2_in_localOrder);
    // Destructor
    ACTOR_ITFC_DTOR(i_q7_op_2_in_localOrder);
};

class ANNOTATION(actor:implement) q7_op_2_in_localOrder : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q7_op_2_in_localOrder);
    // Destructor
    ACTOR_IMPL_DTOR(q7_op_2_in_localOrder);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward_results();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count() + 1;
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_q7_op_2_friendStore, VertexBatch>;
    const unsigned _ds_op_id = 2;
    ds_group_t* _ds_group_hdl = nullptr;

    using sync_t = downstream_handler<i_q7_op_3_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const std::string _in_label = "likes";
    const int64_t _creationDate_propId = storage::berkeleydb_helper::getPropertyID("creationDate");

    const unsigned _expect_num = 20;
    std::priority_queue<like_relation, std::vector<like_relation>, cmp> _order_heap;
};
