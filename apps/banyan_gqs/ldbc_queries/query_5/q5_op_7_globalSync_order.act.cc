//
// Created by everlighting on 2020/9/28.
//

#include "q5_executor.hh"
#include "q5_op_7_globalSync_order.act.h"

void q5_op_7_globalSync_order::process(vertex_count_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.emplace(input[i]);
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q5_op_7_globalSync_order::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q5_op_7_globalSync_order::log_results() {
    std::vector<vertex_count> results;
    results.reserve(_order_heap.size());
    while (!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 5 finished]\n");
    for (auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  Forum: {}, Post Counts: {}\n", (*i).v_id % 1000000000000000L, (*i).count));
    }
}

void q5_op_7_globalSync_order::benchmark_end_action() {
    q5_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q5_helper.next_round();
        if (q5_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_5(q5_helper.get_start_person(q5_helper.current_query_id()));
        } else {
            q5_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}

