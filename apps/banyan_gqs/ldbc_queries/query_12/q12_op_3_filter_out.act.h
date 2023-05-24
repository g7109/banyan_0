//
// Created by everlighting on 2020/10/12.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q12_op_4_filter_and_out;
class i_q12_op_9_globalSync_group_order;

class ANNOTATION(actor:reference) i_q12_op_3_filter_out : public brane::reference_base {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q12_op_3_filter_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q12_op_3_filter_out);
};

class ANNOTATION(actor:implement) q12_op_3_filter_out : public brane::stateful_actor {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q12_op_3_filter_out);
    // Destructor
    ACTOR_IMPL_DTOR(q12_op_3_filter_out);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_q12_op_4_filter_and_out, person_replyOf_msg_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q12_op_9_globalSync_group_order, true>;
    sync_t* _sync_hdl = nullptr;

    const int64_t _comment_labelId = storage::berkeleydb_helper::getLabelID("Comment");

    const std::string _out_label = "replyOf";
};
