#include "q10_executor.hh"
#include "q10_op_4_globalSync_order_limit.act.h"

void q10_op_4_globalSync_order_limit::record_post_count(vertex_intProp_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _count_map.emplace(input[i].v_id, input[i].prop);
    }
}

void q10_op_4_globalSync_order_limit::count_common(VertexBatch&& vertices) {
    for (unsigned i = 0; i < vertices.size(); i++) {
        if (_common_map.find(vertices[i]) == _count_map.end()) {
            _common_map.emplace(vertices[i], 1);
        } else {
            _common_map[vertices[i]] += 1;
        }
    }
}

void q10_op_4_globalSync_order_limit::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        int64_t common, score;
        for (auto& iter: _count_map) {
            common = _common_map.count(iter.first) > 0 ? _common_map[iter.first] : 0;
            score = 2 * common - iter.second;
            _order_heap.emplace(iter.first, score);
            if (_order_heap.size() > _expect_num) {
                _order_heap.pop();
            }
        }

        if (at_ic) {
            benchmark_end_action();
        } else if (at_e4) {
            e4_end_action();
        }
    }
}

void q10_op_4_globalSync_order_limit::log_results() {
    std::vector<heap_node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 10 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  Vertex Id: {}, Score: {}\n", (*i).id % 1000000000000000L, (*i).score));
    }
}

void q10_op_4_globalSync_order_limit::benchmark_end_action() {
    q10_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q10_helper.next_round();
        if (q10_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_10(q10_helper.get_start_person(q10_helper.current_query_id()));
        } else {
            q10_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}

void q10_op_4_globalSync_order_limit::e4_end_action() {
    auto query_id = get_scope().get_root_scope_id();
    // brane::actor_engine().cancel_query_request(query_id, brane::GActorGroupTypeId).then([query_id] {
    auto next_query_id = query_id + E4_bg_query_num;
    e4_exec_query_10(next_query_id);
    // }, nullptr);
}