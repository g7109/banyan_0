#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "cq3x_op_5_cmp_dedup.act.h"

class ANNOTATION(actor:reference) i_cq3x_op_4_out : public brane::reference_base {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3x_op_4_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3x_op_4_out);
};

class ANNOTATION(actor:implement) cq3x_op_4_out : public brane::stateful_actor {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3x_op_4_out);
    // Destructor
    ACTOR_IMPL_DTOR(cq3x_op_4_out);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    const std::string _out_label = "hasType";

    using ds_group_t = downstream_group_handler<i_cq3x_op_5_cmp_dedup, vertex_intProp_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;

};
