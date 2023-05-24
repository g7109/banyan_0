//
// Created by everlighting on 2020/10/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q7_op_2_in_localOrder;
class i_q7_op_2_friendStore;
class i_q7_op_3_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q7_op_1_in_sideOut : public brane::reference_base {
public:
    void process(StartVertex&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q7_op_1_in_sideOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_q7_op_1_in_sideOut);
};

class ANNOTATION(actor:implement) q7_op_1_in_sideOut : public brane::stateful_actor {
public:
    void process(StartVertex&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q7_op_1_in_sideOut);
    // Destructor
    ACTOR_IMPL_DTOR(q7_op_1_in_sideOut);
    // Do work
    ACTOR_DO_WORK();

private:
    using side_ds_group_t = downstream_group_handler<i_q7_op_2_friendStore, VertexBatch>;
    side_ds_group_t* _side_ds_group_hdl = nullptr;
    const std::string _side_out_label = "knows";

    using ds_group_t = downstream_group_handler<i_q7_op_2_in_localOrder, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
    const std::string _in_label = "hasCreator";

    using sync_t = downstream_handler<i_q7_op_3_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;
};

