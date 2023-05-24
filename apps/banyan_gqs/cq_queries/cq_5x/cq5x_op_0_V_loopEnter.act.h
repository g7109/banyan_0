//
// Created by everlighting on 2021/4/8.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

 class i_cq5x_op_1x0_loopOut;

class ANNOTATION(actor:reference) i_cq5x_op_0_V_loopEnter : public brane::reference_base {
public:
    void trigger(StartVertex&& src);
    seastar::future<TrivialStruct<bool> > set_params(VertexBatch&& input);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq5x_op_0_V_loopEnter);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq5x_op_0_V_loopEnter);
};

class ANNOTATION(actor:implement) cq5x_op_0_V_loopEnter : public brane::stateless_actor {
public:
    void trigger(StartVertex&& src);
    seastar::future<TrivialStruct<bool> > set_params(VertexBatch&& input);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq5x_op_0_V_loopEnter);
    // Destructor
    ACTOR_IMPL_DTOR(cq5x_op_0_V_loopEnter);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward();

private:
    using ds_group_t = downstream_group_handler<i_cq5x_op_1x0_loopOut, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    using param_setter_t = i_cq5x_op_0_V_loopEnter;
    unsigned _machine_num = brane::global_shard_count() / brane::local_shard_count();
    int64_t _src_vertex;
    const std::string _out_label = "workAt";
};

