#include "q4_executor.hh"
#include "q4_op_5_globalSync_order.act.h"

void q4_op_5_globalSync_order::process(count_name_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.emplace(input.counts[i], input.names[i].ptr, input.names[i].len);
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q4_op_5_globalSync_order::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q4_op_5_globalSync_order::log_results() {
    std::vector<node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 4 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  TagName: {}, Count: {}\n", (*i).tag_name, (*i).count));
    }
}

void q4_op_5_globalSync_order::benchmark_end_action() {
    q4_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q4_helper.next_round();
        if (q4_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_4(q4_helper.get_start_person(q4_helper.current_query_id()));
        } else {
            q4_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
