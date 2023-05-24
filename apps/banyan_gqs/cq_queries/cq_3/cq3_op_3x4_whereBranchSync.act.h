//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq3_op_4_whereLeave_localOrder;

class ANNOTATION(actor:reference) i_cq3_op_3x4_whereBranchSync : public brane::reference_base {
public:
    void set_owner(TrivialStruct<int64_t>&& vertex);
    void set_successful();
    void receive_eos(PathEos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3_op_3x4_whereBranchSync);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3_op_3x4_whereBranchSync);
};

class ANNOTATION(actor:implement) cq3_op_3x4_whereBranchSync : public brane::stateful_actor {
public:
    void set_owner(TrivialStruct<int64_t>&& vertex);
    void set_successful();
    void receive_eos(PathEos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3_op_3x4_whereBranchSync);
    // Destructor
    ACTOR_IMPL_DTOR(cq3_op_3x4_whereBranchSync);
    // Do work
    ACTOR_DO_WORK();

private:
    void notify_and_cancel(bool early = false);

private:
    PathEosCheckTree _eos_checker{};

    using where_leave_t = downstream_handler<i_cq3_op_4_whereLeave_localOrder, true, TrivialStruct<int64_t> >;
    where_leave_t* _where_leave_hdl = nullptr;

    int64_t _owner = 0;
    bool _owner_set = false;
    bool _filter_successful = false;
    bool _finished = false;
};