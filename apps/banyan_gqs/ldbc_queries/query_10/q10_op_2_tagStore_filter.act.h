//
// Created by everlighting on 2021/03/25.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q10_op_3_filter_in;
class i_q10_op_4_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q10_op_2_tagStore_filter : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void filter_commonCount(vertex_friendId_Batch&& input);
    void receive_stage2_eos();

    // Constructor.
    ACTOR_ITFC_CTOR(i_q10_op_2_tagStore_filter);
    // Destructor
    ACTOR_ITFC_DTOR(i_q10_op_2_tagStore_filter);
};

class ANNOTATION(actor:implement) q10_op_2_tagStore_filter : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void filter_commonCount(vertex_friendId_Batch&& input);
    void receive_stage2_eos();

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q10_op_2_tagStore_filter);
    // Destructor
    ACTOR_IMPL_DTOR(q10_op_2_tagStore_filter);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    using eos_ds_group_t = downstream_group_handler<i_q10_op_3_filter_in, VertexBatch>;
    eos_ds_group_t* _eos_ds_group_hdl = nullptr;

    unsigned _stage2_eos_rcv_num = 0;
    const unsigned _stage2_expect_eos_num = brane::global_shard_count();
    using sync_t = downstream_handler<i_q10_op_4_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    std::unordered_set<int64_t> _tags{};

    const std::string _out_label_for_filter = "hasTag";
};
