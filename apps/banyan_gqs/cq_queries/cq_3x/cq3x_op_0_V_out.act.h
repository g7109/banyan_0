#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq3x_op_1_unionOut;

class ANNOTATION(actor:reference) i_cq3x_op_0_V_out : public brane::reference_base {
public:
    void trigger(StartVertex&& src);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3x_op_0_V_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3x_op_0_V_out);
};

class ANNOTATION(actor:implement) cq3x_op_0_V_out : public brane::stateless_actor {
public:
    void trigger(StartVertex&& src);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3x_op_0_V_out);
    // Destructor
    ACTOR_IMPL_DTOR(cq3x_op_0_V_out);
    // Do work
    ACTOR_DO_WORK();

private:
    std::string _out_label = "knows";
    using ds_group_t = downstream_group_handler<i_cq3x_op_1_unionOut, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};

