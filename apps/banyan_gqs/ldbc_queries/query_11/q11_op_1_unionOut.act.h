//
// Created by everlighting on 2020/10/9.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_q11_op_2_dedup_out;

class ANNOTATION(actor:reference) i_q11_op_1_unionOut : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_1_unionOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_1_unionOut);
};

class ANNOTATION(actor:implement) q11_op_1_unionOut : public brane::stateless_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_1_unionOut);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_1_unionOut);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_group_t = downstream_group_handler<i_q11_op_2_dedup_out, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;

    const std::string _out_label = "knows";
};

