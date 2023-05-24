//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq3_op_3x1_whereOut;
class i_cq3_op_3x4_whereBranchSync;

class ANNOTATION(actor:reference) i_cq3_op_3x0_whereIn : public brane::reference_base {
public:
    void process(TrivialStruct<int64_t>&& vertex);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3_op_3x0_whereIn);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3_op_3x0_whereIn);
};

class ANNOTATION(actor:implement) cq3_op_3x0_whereIn : public brane::stateful_actor {
public:
    void process(TrivialStruct<int64_t>&& vertex);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3_op_3x0_whereIn);
    // Destructor
    ACTOR_IMPL_DTOR(cq3_op_3x0_whereIn);
    // Do work
    ACTOR_DO_WORK();

private:
    using ds_manager_t = compressed_downstream_group_handler<
            i_cq3_op_3x1_whereOut, VertexBatch, i_cq3_op_3x4_whereBranchSync>;
    const unsigned _sync_op_id = 4;
    ds_manager_t* _ds_manager = nullptr;

    const std::string _in_label = "hasCreator";
};
