//
// Created by everlighting on 2021/4/15.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq4_op_1x0x1_where_loopUntil;
class i_cq4_op_1x1_whereBranchSync;

class ANNOTATION(actor:reference) i_cq4_op_1x0x0_where_loopOut : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq4_op_1x0x0_where_loopOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq4_op_1x0x0_where_loopOut);
};

class ANNOTATION(actor:implement) cq4_op_1x0x0_where_loopOut : public brane::stateless_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq4_op_1x0x0_where_loopOut);
    // Destructor
    ACTOR_IMPL_DTOR(cq4_op_1x0x0_where_loopOut);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_manager_t = compressed_downstream_group_handler<
            i_cq4_op_1x0x1_where_loopUntil, VertexBatch, i_cq4_op_1x1_whereBranchSync>;
    ds_manager_t* _ds_manager = nullptr;

    const std::string _out_label = "knows";
};
