//
// Created by everlighting on 2021/4/13.
//

#include "common/query_helpers.hh"
#include "cq5_executor.hh"
#include "cq5_op_5_globalSync_limit.act.h"

void cq5_op_5_globalSync_limit::process(TrivialStruct<int64_t> &&input) {
    if (_cancelled) { return; }

    _results.emplace_back(input.val);
    if (cq_using_limit_cancel && _results.size() >= _limit_num) {
        finish_query();
    }
}

void cq5_op_5_globalSync_limit::receive_eos(Eos &&eos) {
    if (_cancelled) { return; }
    if (++_eos_rcv_num == _expect_eos_num) {
        finish_query();
    }
}

void cq5_op_5_globalSync_limit::initialize() {
    _limit_num = cq_limit_n;
}

void cq5_op_5_globalSync_limit::finalize() {
}

void cq5_op_5_globalSync_limit::finish_query() {
    _cancelled = true;
    if (at_cq) {
        cq_end_action();
    }
}

void cq5_op_5_globalSync_limit::log_results() {
    write_log("[CQ 5 finished]\n");
    for (auto& person : _results) {
        write_log(seastar::format("  PersonId = {}\n", person));
    }
}

void cq5_op_5_globalSync_limit::cq_end_action() {
    cq5_helper.record_end_time();
    log_results();

    auto query_id = get_scope().get_root_scope_id();
    brane::actor_engine().cancel_query_request(query_id, get_query_actor_group_typeId()).then([] {
        cq5_helper.next_round();
        if (cq5_helper.current_round() < (exec_epoch * param_num)) {
            exec_cq5(cq5_helper.get_start_person(cq5_helper.current_query_id()));
        } else {
            cq5_helper.log_cq_exec_times();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
