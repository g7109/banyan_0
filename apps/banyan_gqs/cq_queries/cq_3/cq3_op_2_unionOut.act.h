//
// Created by everlighting on 2021/4/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq3_op_4_dedup_whereEnter;
class i_cq3_op_6_globalSync_order_limit;

class ANNOTATION(actor:reference) i_cq3_op_2_unionOut : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3_op_2_unionOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3_op_2_unionOut);
};

class ANNOTATION(actor:implement) cq3_op_2_unionOut : public brane::stateful_actor {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3_op_2_unionOut);
    // Destructor
    ACTOR_IMPL_DTOR(cq3_op_2_unionOut);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_cq3_op_4_dedup_whereEnter, VertexBatch>;
    const unsigned _ds_op_id = 4;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_cq3_op_6_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const std::string _out_label = "knows";
};


