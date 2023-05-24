#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq3_op_1_out;

class ANNOTATION(actor:reference) i_cq3_op_0_V : public brane::reference_base {
public:
    void trigger(StartVertex&& src);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3_op_0_V);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3_op_0_V);
};

class ANNOTATION(actor:implement) cq3_op_0_V : public brane::stateless_actor {
public:
    void trigger(StartVertex&& src);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3_op_0_V);
    // Destructor
    ACTOR_IMPL_DTOR(cq3_op_0_V);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_group_t = downstream_group_handler<i_cq3_op_1_out, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
