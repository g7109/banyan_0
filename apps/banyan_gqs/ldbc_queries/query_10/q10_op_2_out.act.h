//
// Created by everlighting on 2020/11/18.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q10_op_3_filter_in;
class i_q10_op_4_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q10_op_2_out : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q10_op_2_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q10_op_2_out);
};

class ANNOTATION(actor:implement) q10_op_2_out : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q10_op_2_out);
    // Destructor
    ACTOR_IMPL_DTOR(q10_op_2_out);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_group_t = downstream_group_handler<i_q10_op_3_filter_in, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    const std::string _out_label = "knows";
};