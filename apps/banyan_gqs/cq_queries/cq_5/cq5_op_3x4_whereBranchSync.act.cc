//
// Created by everlighting on 2021/4/13.
//

#include "cq5_op_3x4_whereBranchSync.act.h"
#include "cq5_op_4_whereLeave.act.h"

void cq5_op_3x4_whereBranchSync::set_owner(TrivialStruct<int64_t> &&vertex) {
    _owner = vertex.val;
    _owner_set = true;
    if (_finished) { notify_and_cancel(true); }
}

void cq5_op_3x4_whereBranchSync::set_successful() {
    if (_finished) { return; }

    _filter_successful = true;
    _finished = true;
    if (_owner_set) { notify_and_cancel(true); }
}

void cq5_op_3x4_whereBranchSync::receive_eos(PathEos &&eos) {
    if (_finished) { return; }
    if (_eos_checker.insert_and_merge(eos)) {
        _finished = true;
        if (_owner_set) { notify_and_cancel(false); }
    }
}

void cq5_op_3x4_whereBranchSync::notify_and_cancel(bool early) {
    if (_filter_successful) {
        _where_leave_hdl->process(TrivialStruct<int64_t>{_owner});
    }
    _where_leave_hdl->receive_eos();

    if (!early || cq_using_branch_cancel) {
        brane::actor_engine().cancel_scope(get_scope());
    }
}

void cq5_op_3x4_whereBranchSync::initialize() {
    auto builder = get_scope();
    builder.back_to_parent_scope();
    auto where_leave_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    _where_leave_hdl = new where_leave_t(builder, where_leave_op_id);
}

void cq5_op_3x4_whereBranchSync::finalize() {
    delete _where_leave_hdl;
}
