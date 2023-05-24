//
// Created by everlighting on 2021/4/15.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq4_op_2_whereEnter;

class ANNOTATION(actor:reference) i_cq4_op_0_V_out : public brane::reference_base {
public:
    void trigger(StartVertex&& src);
    seastar::future<TrivialStruct<bool> > set_params(VertexBatch&& input);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq4_op_0_V_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq4_op_0_V_out);
};

class ANNOTATION(actor:implement) cq4_op_0_V_out : public brane::stateful_actor {
public:
    void trigger(StartVertex&& src);
    seastar::future<TrivialStruct<bool> > set_params(VertexBatch&& input);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq4_op_0_V_out);
    // Destructor
    ACTOR_IMPL_DTOR(cq4_op_0_V_out);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward();

private:
    using ds_group_t = downstream_group_handler<i_cq4_op_2_whereEnter, VertexBatch>;
    const unsigned _ds_op_id = 2;
    ds_group_t* _ds_group_hdl = nullptr;

    using param_setter_t = i_cq4_op_0_V_out;
    unsigned _machine_num = brane::global_shard_count() / brane::local_shard_count();
    const std::string _side_out_label = "workAt";

    int64_t _src_vertex;
    const std::string _out_label = "knows";
};
