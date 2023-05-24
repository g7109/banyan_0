//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq6_op_1x1x2_dedup_whereOut;
class i_cq6_op_1x1x4_whereBranchSync;

class ANNOTATION(actor:reference) i_cq6_op_1x1x1_whereOut : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6_op_1x1x1_whereOut);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6_op_1x1x1_whereOut);
};

class ANNOTATION(actor:implement) cq6_op_1x1x1_whereOut : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq6_op_1x1x1_whereOut);
    // Destructor
    ACTOR_IMPL_DTOR(cq6_op_1x1x1_whereOut);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_manager_t = compressed_downstream_group_handler<
            i_cq6_op_1x1x2_dedup_whereOut, VertexBatch, i_cq6_op_1x1x4_whereBranchSync>;
    const unsigned _sync_op_id = 4;
    ds_manager_t* _ds_manager = nullptr;

    const std::string _out_label = "hasTag";
};
