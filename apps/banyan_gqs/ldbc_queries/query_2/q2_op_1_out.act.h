#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "q2_op_2_in.act.h"

class i_q2_op_4_order_and_limit_display;

class ANNOTATION(actor:reference) i_q2_op_1_out : public brane::reference_base {
public:
    void process(StartVertex&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q2_op_1_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q2_op_1_out);
};

class ANNOTATION(actor:implement) q2_op_1_out : public brane::stateful_actor {
public:
    void process(StartVertex&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q2_op_1_out);
    // Destructor
    ACTOR_IMPL_DTOR(q2_op_1_out);
    // Do work
    ACTOR_DO_WORK();

private:
    const std::string _out_label = "knows";

    using ds_group_t = downstream_group_handler<i_q2_op_2_in, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    using sync_t = downstream_handler<i_q2_op_4_order_and_limit_display, true>;
    sync_t* _sync_hdl = nullptr;
};
