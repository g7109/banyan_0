//
// Created by everlighting on 2020/11/18.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q10_op_2_out;
class i_q10_op_2_tagStore_filter;

class ANNOTATION(actor:reference) i_q10_op_1_out_sideOut : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q10_op_1_out_sideOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_q10_op_1_out_sideOut);
};

class ANNOTATION(actor:implement) q10_op_1_out_sideOut : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q10_op_1_out_sideOut);
    // Destructor
    ACTOR_IMPL_DTOR(q10_op_1_out_sideOut);
    // Do work
    ACTOR_DO_WORK();

private:
    using side_ds_group_t = downstream_group_handler<i_q10_op_2_tagStore_filter, VertexBatch>;
    side_ds_group_t* _side_ds_group_hdl = nullptr;
    const std::string _side_out_label = "hasInterest";

    using ds_group_t = downstream_group_handler<i_q10_op_2_out, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
    const std::string _out_label = "knows";
};
