#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_cq6_op_3_globalSync_limit;

class ANNOTATION(actor:reference) i_cq6_op_2_loopLeave_dedup : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6_op_2_loopLeave_dedup);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6_op_2_loopLeave_dedup);
};

class ANNOTATION(actor:implement) cq6_op_2_loopLeave_dedup : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(cq6_op_2_loopLeave_dedup);
    // Destructor
    ACTOR_IMPL_DTOR(cq6_op_2_loopLeave_dedup);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_and_sync_t = downstream_handler<i_cq6_op_3_globalSync_limit, true, VertexBatch>;
    ds_and_sync_t* _ds_sync_hdl = nullptr;

    std::unordered_set<int64_t> _vertices_have_seen{};
};


