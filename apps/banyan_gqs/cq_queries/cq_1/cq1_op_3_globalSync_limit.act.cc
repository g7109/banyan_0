#include "common/query_helpers.hh"
#include "cq1_executor.hh"
#include "cq1_op_3_globalSync_limit.act.h"

void cq1_op_3_globalSync_limit::process(VertexBatch &&input) {
    if (_cancelled) { return; }
    for (unsigned i = 0; i < input.size(); i++) {
        _friends.insert(input[i]);
        if (cq_using_limit_cancel && _friends.size() == _limit_num) {
            finish_query();
            return;
        }
    }
}

void cq1_op_3_globalSync_limit::receive_eos(Eos&& eos) {
    if (_cancelled) { return; }
    if (++_eos_rcv_num == _expect_eos_num) {
        finish_query();
    }
}

void cq1_op_3_globalSync_limit::initialize() {
    _limit_num = cq_limit_n;
}

void cq1_op_3_globalSync_limit::finalize() {
}

void cq1_op_3_globalSync_limit::finish_query() {
    _cancelled = true;
    if (at_cq) {
        cq_end_action();
    }
}

void cq1_op_3_globalSync_limit::log_results() {
    write_log("[CQ 1 finished]\n");
    for (auto& person : _friends) {
        write_log(seastar::format("  PersonId = {}\n", person));
    }
}

void cq1_op_3_globalSync_limit::cq_end_action() {
    cq1_helper.record_end_time();
//    log_results();

    auto query_id = get_scope().get_root_scope_id();
    brane::actor_engine().cancel_query_request(query_id, get_query_actor_group_typeId()).then([] {
        cq1_helper.next_round();
        if (cq1_helper.current_round() < (exec_epoch * param_num)) {
            exec_cq1(cq1_helper.get_start_person(cq1_helper.current_query_id()));
        } else {
            cq1_helper.log_cq_exec_times();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
