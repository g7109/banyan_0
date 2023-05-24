#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "q2_op_3_vertex_filter.act.h"

class ANNOTATION(actor:reference) i_q2_op_2_in : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q2_op_2_in);
    // Destructor
    ACTOR_ITFC_DTOR(i_q2_op_2_in);
};

class ANNOTATION(actor:implement) q2_op_2_in : public brane::stateful_actor {
public:
    void process(VertexBatch &&vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q2_op_2_in);
    // Destructor
    ACTOR_IMPL_DTOR(q2_op_2_in);
    // Do work
    ACTOR_DO_WORK();

private:
    const std::string _in_label = "hasCreator";

    using ds_group_t = downstream_group_handler<i_q2_op_3_vertex_filter, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};