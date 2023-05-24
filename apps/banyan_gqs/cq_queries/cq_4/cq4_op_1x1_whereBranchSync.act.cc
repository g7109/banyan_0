//
// Created by everlighting on 2021/4/15.
//

#include "cq4_op_1x1_whereBranchSync.act.h"
#include "cq4_op_2_whereLeave.act.h"

void cq4_op_1x1_whereBranchSync::set_owner(TrivialStruct<int64_t> &&vertex) {
    _owner = vertex.val;
    _owner_set = true;
    if (_finished) { notify_and_cancel(true); }
}

void cq4_op_1x1_whereBranchSync::set_successful() {
    if (_finished) { return; }

    _filter_successful = true;
    _finished = true;
    if (_owner_set) { notify_and_cancel(true); }
}

void cq4_op_1x1_whereBranchSync::receive_eos(PathEos &&eos) {
    if (_finished) { return; }
    if (_eos_checker.insert_and_merge(eos)) {
        _finished = true;
        if (_owner_set) { notify_and_cancel(false); }
    }
}

void cq4_op_1x1_whereBranchSync::notify_and_cancel(bool early) {
    if (_filter_successful) {
        _where_leave_hdl->process(TrivialStruct<int64_t>{_owner});
    }
    _where_leave_hdl->receive_eos();

    if (!early || cq_using_branch_cancel) {
        brane::actor_engine().cancel_scope(get_scope());
    }
}

void cq4_op_1x1_whereBranchSync::initialize() {
    auto builder = get_scope();
    builder.back_to_parent_scope();
    auto where_leave_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    _where_leave_hdl = new where_leave_t(builder, where_leave_op_id);
}

void cq4_op_1x1_whereBranchSync::finalize() {
    delete _where_leave_hdl;
}
