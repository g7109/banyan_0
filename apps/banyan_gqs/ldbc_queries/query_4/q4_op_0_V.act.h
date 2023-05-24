//
// Created by everlighting on 2020/9/16.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q4_op_1_out;

class ANNOTATION(actor:reference) i_q4_op_0_V : public brane::reference_base {
public:
    void trigger(StartVertex&& start_vertex);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q4_op_0_V);
    // Destructor
    ACTOR_ITFC_DTOR(i_q4_op_0_V);
};

class ANNOTATION(actor:implement) q4_op_0_V : public brane::stateless_actor {
public:
    void trigger(StartVertex&& start_vertex);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q4_op_0_V);
    // Destructor
    ACTOR_IMPL_DTOR(q4_op_0_V);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_group_t = downstream_group_handler<i_q4_op_1_out, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
