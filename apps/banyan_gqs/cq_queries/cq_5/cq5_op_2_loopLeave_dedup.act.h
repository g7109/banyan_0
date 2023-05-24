//
// Created by everlighting on 2021/4/9.
//

#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "cq5_op_4_whereEnter.act.h"

class ANNOTATION(actor:reference) i_cq5_op_2_loopLeave_dedup : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq5_op_2_loopLeave_dedup);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq5_op_2_loopLeave_dedup);
};

class ANNOTATION(actor:implement) cq5_op_2_loopLeave_dedup : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(cq5_op_2_loopLeave_dedup);
    // Destructor
    ACTOR_IMPL_DTOR(cq5_op_2_loopLeave_dedup);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_t = downstream_handler<i_cq5_op_4_whereEnter, false, VertexBatch>;
    const unsigned _ds_op_id = 4;
    ds_t* _ds_hdl = nullptr;

    std::unordered_set<int64_t> _vertices_have_seen{};
};
