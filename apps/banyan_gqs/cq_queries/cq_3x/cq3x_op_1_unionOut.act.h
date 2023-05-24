#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "cq3x_op_2_dedup_in.act.h"

class ANNOTATION(actor:reference) i_cq3x_op_1_unionOut : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3x_op_1_unionOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3x_op_1_unionOut);
};

class ANNOTATION(actor:implement) cq3x_op_1_unionOut : public brane::stateful_actor {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3x_op_1_unionOut);
    // Destructor
    ACTOR_IMPL_DTOR(cq3x_op_1_unionOut);
    // Do work
    ACTOR_DO_WORK();

private:
    const std::string _out_label = "knows";

    using ds_group_t = downstream_group_handler<i_cq3x_op_2_dedup_in, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
