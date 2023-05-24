#include "q11_executor.hh"
#include "q11_op_6_globalSync_order_limit.act.h"

void q11_op_6_globalSync_order_limit::record_organisation_names(vertex_string_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        for (auto& iter : _relation_map[input.v_ids[i]]) {
            _order_heap.emplace(iter.first, iter.second, input.strs[i].ptr, input.strs[i].len);
            if (_order_heap.size() > _expect_num) {
                _order_heap.pop();
            }
        }
    }
}

void q11_op_6_globalSync_order_limit::finish_query() {
    if (at_ic) {
        benchmark_end_action();
    }
}

void q11_op_6_globalSync_order_limit::record_relations(work_relation_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_relation_map.count(input[i].organisation_id) == 0) {
            _relation_map[input[i].organisation_id] = std::vector<std::pair<int64_t, int64_t> >{};
            _relation_map[input[i].organisation_id].reserve(batch_size);
        }
        _relation_map[input[i].organisation_id].push_back(
                std::make_pair(input[i].person_id, input[i].work_from_year));
    }
}

void q11_op_6_globalSync_order_limit::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q11_op_6_globalSync_order_limit::log_results() {
    std::vector<node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 11 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  Person Id: {}, Organisation Name: {}, Start Work Date: {}\n",
                (*i).friend_vertex % 1000000000000000L, (*i).organ_name, (*i).start_work_date));
    }
}

void q11_op_6_globalSync_order_limit::benchmark_end_action() {
    q11_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q11_helper.next_round();
        if (q11_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_11(q11_helper.get_start_person(q11_helper.current_query_id()));
        } else {
            q11_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}

