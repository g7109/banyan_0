//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_cq5_op_4_whereEnter;
class i_cq5_op_5_globalSync_limit;

class ANNOTATION(actor:reference) i_cq5_op_4_whereLeave : public brane::reference_base {
public:
    void process(TrivialStruct<int64_t>&& input);
    void set_branch_num(TrivialStruct<unsigned>&& branch_num);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq5_op_4_whereLeave);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq5_op_4_whereLeave);
};

class ANNOTATION(actor:implement) cq5_op_4_whereLeave : public brane::stateful_actor {
public:
    void process(TrivialStruct<int64_t>&& input);
    void set_branch_num(TrivialStruct<unsigned>&& branch_num);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq5_op_4_whereLeave);
    // Destructor
    ACTOR_IMPL_DTOR(cq5_op_4_whereLeave);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    unsigned _expect_eos_num = 0;
    bool _eos_num_set = false;

    using where_enter_t = downstream_handler<i_cq5_op_4_whereEnter, false>;
    where_enter_t* _where_enter_hdl = nullptr;

    using ds_and_sync_t = downstream_handler<i_cq5_op_5_globalSync_limit, true, TrivialStruct<int64_t> >;
    ds_and_sync_t* _ds_sync_hdl = nullptr;
};
