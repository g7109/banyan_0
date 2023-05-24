//
// Created by everlighting on 2020/11/18.
//
#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_q10_op_2_tagStore_filter;
class i_q10_op_4_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q10_op_3_filter_in : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q10_op_3_filter_in);
    // Destructor
    ACTOR_ITFC_DTOR(i_q10_op_3_filter_in);
};

class ANNOTATION(actor:implement) q10_op_3_filter_in : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q10_op_3_filter_in);
    // Destructor
    ACTOR_IMPL_DTOR(q10_op_3_filter_in);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward_friend_contents();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count() * 2;

    using ds_group_t = downstream_group_handler<i_q10_op_2_tagStore_filter, vertex_friendId_Batch>;
    const unsigned _ds_op_id = 2;
    ds_group_t* _ds_group_hdl = nullptr;

    using sync_t = downstream_handler<i_q10_op_4_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const long _prop_type_id = storage::berkeleydb_helper::getPropertyID("birthday");
    long _month;
    long _next_month;

    const std::string _in_label = "hasCreator";
    std::vector<std::pair<int64_t, int64_t> > _friend_contents;
};
