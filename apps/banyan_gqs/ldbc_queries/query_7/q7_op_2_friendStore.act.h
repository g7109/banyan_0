//
// Created by everlighting on 2020/10/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q7_op_2_in_localOrder;
class i_q7_op_3_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q7_op_2_friendStore : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void filter_liker_within_friends(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q7_op_2_friendStore);
    // Destructor
    ACTOR_ITFC_DTOR(i_q7_op_2_friendStore);
};

class ANNOTATION(actor:implement) q7_op_2_friendStore : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void filter_liker_within_friends(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q7_op_2_friendStore);
    // Destructor
    ACTOR_IMPL_DTOR(q7_op_2_friendStore);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    unsigned _expect_eos_num = 1;
    bool _at_stage2 = false;

    using eos_ds_group_t = downstream_group_handler<i_q7_op_2_in_localOrder, VertexBatch>;
    const unsigned _eos_ds_op_id = 2;
    eos_ds_group_t* _eos_ds_group_hdl = nullptr;

    using sync_t = downstream_handler<i_q7_op_3_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    std::unordered_set<int64_t> _friends{};
};
