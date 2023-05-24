#include "common/query_helpers.hh"
#include "cq5x_executor.hh"
#include "cq5x_op_6_globalSync.act.h"

void cq5x_op_6_globalSync::process(VertexBatch &&input) {
    if (_cancelled) { return; }

    for (unsigned i = 0; i < input.size(); i++) {
        _results.push_back(input[i]);
        if (cq_using_limit_cancel && _results.size() == _limit_num) {
            finish_query();
            return;
        }
    }
}

void cq5x_op_6_globalSync::receive_eos(Eos&& eos) {
    if (_cancelled) { return; }
    if (++_eos_rcv_num == _expect_eos_num) {
        finish_query();
    }
}

void cq5x_op_6_globalSync::initialize() {
    _limit_num = cq_limit_n;
}

void cq5x_op_6_globalSync::finalize() {
}

void cq5x_op_6_globalSync::finish_query() {
    _cancelled = true;
    if (at_cq) {
        cq_end_action();
    }
}

void cq5x_op_6_globalSync::log_results() {
    write_log("[CQ 5x finished]\n");
    for (auto& person : _results) {
        write_log(seastar::format("  PersonId = {}\n", person));
    }
}

void cq5x_op_6_globalSync::cq_end_action() {
    cq5_helper.record_end_time();
    log_results();

    auto query_id = get_scope().get_root_scope_id();
    brane::actor_engine().cancel_query_request(query_id, get_query_actor_group_typeId()).then([] {
        cq5_helper.next_round();
        if (cq5_helper.current_round() < (exec_epoch * param_num)) {
            exec_cq5x(cq5_helper.get_start_person(cq5_helper.current_query_id()));
        } else {
            cq5_helper.log_cq_exec_times();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
