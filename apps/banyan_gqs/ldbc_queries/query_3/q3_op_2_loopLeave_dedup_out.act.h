//
// Created by everlighting on 2020/11/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "q3_op_3_out.act.h"
#include "common/downstream_handlers.hh"

class i_q3_op_8_group_and_order;

class ANNOTATION(actor:reference) i_q3_op_2_loopLeave_dedup_out : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q3_op_2_loopLeave_dedup_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q3_op_2_loopLeave_dedup_out);
};

class ANNOTATION(actor:implement) q3_op_2_loopLeave_dedup_out : public brane::stateful_actor {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q3_op_2_loopLeave_dedup_out);
    // Destructor
    ACTOR_IMPL_DTOR(q3_op_2_loopLeave_dedup_out);
    // Do work
    ACTOR_DO_WORK();

private:
    std::unordered_set<int64_t> _vertices_have_seen{};
    const std::string _out_label = "isLocatedIn";

    using ds_group_t = downstream_group_handler<i_q3_op_3_out, vertex_intProp_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
