#include "q2_executor.hh"
#include "q2_op_4_order_and_limit_display.act.h"

void q2_op_4_order_and_limit_display::process(vertex_intProp_Batch&& vertices) {
    for(unsigned i = 0; i < vertices.size(); i++) {
        _order_heap.emplace(vertices[i].prop, vertices[i].v_id);
        if(_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q2_op_4_order_and_limit_display::finish_query() {
    if (at_ic) {
        benchmark_end_action();
    }
}

void q2_op_4_order_and_limit_display::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q2_op_4_order_and_limit_display::log_results() {
    std::vector<heap_node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 2 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  Id = {}, Creation Date = {}\n",
                (*i).id % 1000000000000000L, (*i).creation_date));
    }
}

void q2_op_4_order_and_limit_display::benchmark_end_action() {
    q2_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q2_helper.next_round();
        if (q2_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_2(q2_helper.get_start_person(q2_helper.current_query_id()));
        } else {
            q2_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
