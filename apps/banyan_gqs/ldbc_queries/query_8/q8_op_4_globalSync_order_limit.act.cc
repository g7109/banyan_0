#include "q8_executor.hh"
#include "q8_op_4_globalSync_order_limit.act.h"

void q8_op_4_globalSync_order_limit::process(create_relation_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.emplace(input[i].msg_id, input[i].creator, input[i].creation_date);
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q8_op_4_globalSync_order_limit::finish_query() {
    if (at_ic) {
        benchmark_end_action();
    }
}

void q8_op_4_globalSync_order_limit::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q8_op_4_globalSync_order_limit::log_results() {
    std::vector<node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 8 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  Commenter: {}, Comment: {}, Creation Date: {}\n",
                (*i).commenter % 1000000000000000L, (*i).comment % 1000000000000000L, (*i).creation_date));
    }
}

void q8_op_4_globalSync_order_limit::benchmark_end_action() {
    q8_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q8_helper.next_round();
        if (q8_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_8(q8_helper.get_start_person(q8_helper.current_query_id()));
        } else {
            q8_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
