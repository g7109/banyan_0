#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "cq5x_op_4_out.act.h"

class ANNOTATION(actor:reference) i_cq5x_op_3_out : public brane::reference_base {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq5x_op_3_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq5x_op_3_out);
};

class ANNOTATION(actor:implement) cq5x_op_3_out : public brane::stateful_actor {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq5x_op_3_out);
    // Destructor
    ACTOR_IMPL_DTOR(cq5x_op_3_out);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    const std::string _out_label = "hasTag";

    using ds_group_t = downstream_group_handler<i_cq5x_op_4_out, vertex_friendId_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
