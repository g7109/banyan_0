//
// Created by everlighting on 2020/10/9.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q11_op_1_unionOut;
class i_q11_op_6_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q11_op_0_V_out : public brane::reference_base {
public:
    void trigger(StartVertex&& src);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_0_V_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_0_V_out);
};

class ANNOTATION(actor:implement) q11_op_0_V_out : public brane::stateless_actor {
public:
    void trigger(StartVertex&& src);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_0_V_out);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_0_V_out);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_group_t = downstream_group_handler<i_q11_op_1_unionOut, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    using sync_t = downstream_handler<i_q11_op_6_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const std::string _out_label = "knows";
};

