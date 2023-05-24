//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_cq6_op_1x0_loop_dedup_whereEnter;
class i_cq6_op_1x3_loopUntil;

class ANNOTATION(actor:reference) i_cq6_op_1x2_whereLeave_localOrder : public brane::reference_base {
public:
    void process(TrivialStruct<int64_t>&& input);
    void set_branch_num(TrivialStruct<unsigned>&& branch_num);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6_op_1x2_whereLeave_localOrder);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6_op_1x2_whereLeave_localOrder);
};

class ANNOTATION(actor:implement) cq6_op_1x2_whereLeave_localOrder : public brane::stateful_actor {
public:
    void process(TrivialStruct<int64_t>&& input);
    void set_branch_num(TrivialStruct<unsigned>&& branch_num);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq6_op_1x2_whereLeave_localOrder);
    // Destructor
    ACTOR_IMPL_DTOR(cq6_op_1x2_whereLeave_localOrder);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    unsigned _expect_eos_num = 0;
    bool _eos_num_set = false;

    using where_enter_t = downstream_handler<i_cq6_op_1x0_loop_dedup_whereEnter, false>;
    where_enter_t* _where_enter_hdl = nullptr;

    using ds_group_t = downstream_group_handler<i_cq6_op_1x3_loopUntil, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
