#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q2_op_1_out;

class ANNOTATION(actor:reference) i_q2_op_0_V : public brane::reference_base {
public:
    void trigger(StartVertex&& src);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q2_op_0_V);
    // Destructor
    ACTOR_ITFC_DTOR(i_q2_op_0_V);
};

class ANNOTATION(actor:implement) q2_op_0_V : public brane::stateless_actor {
public:
    void trigger(StartVertex&& src);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q2_op_0_V);
    // Destructor
    ACTOR_IMPL_DTOR(q2_op_0_V);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_t = downstream_handler<i_q2_op_1_out, false, StartVertex>;
    ds_t* _ds_hdl = nullptr;
};

