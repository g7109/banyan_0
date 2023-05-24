//
// Created by everlighting on 2020/9/29.
//

#include "q6_executor.hh"
#include "q6_op_5_globalSync_group_order.act.h"
#include "q6_op_4_dedup_propGet.act.h"

void q6_op_5_globalSync_group_order::process(count_name_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.emplace(input.counts[i], std::string{input.names[i].ptr, input.names[i].len});
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q6_op_5_globalSync_group_order::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        } else if (at_e1) {
            e1_end_action();
        } else if (at_e5) {
            e5_end_action();
        }
    }
}

void q6_op_5_globalSync_group_order::log_results() {
    std::vector<node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 6 finished]\n");
    for (auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  TagName[{}], Post Count[{}]\n",
                (*i).tag_name, (*i).count));
    }
}

void q6_op_5_globalSync_group_order::benchmark_end_action() {
    q6_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q6_helper.next_round();
        if (q6_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_6(q6_helper.get_start_person(q6_helper.current_query_id()));
        } else {
            q6_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}

void q6_op_5_globalSync_group_order::e1_end_action() {
    auto query_id = get_scope().get_root_scope_id();
    if (query_id != 0) {
        e1_helper.record_end_time();
        query_id += E1_concurrency;
        if (query_id <= E1_total_exec_rounds) {
            e1_exec_query_6(query_id);
        } else {
            if (e1_helper.get_timestamp_size() == E1_total_exec_rounds) {
                e1_helper.compute_concurrent_throughput_and_latency();
                brane::actor_engine().exit();
            }
        }
    } else {
        brane::actor_engine().cancel_query(query_id, brane::GActorGroupTypeId);
    }
}

void q6_op_5_globalSync_group_order::e5_end_action() {
    auto query_id = get_scope().get_root_scope_id();
    e5_helper.record_end_time(query_id);
    brane::actor_engine().cancel_query(query_id, brane::GActorGroupTypeId);

    if (e5_helper.get_timestamp_size() == E5_balance_round) {
        load_balancer::re_balance();
    } else if (e5_helper.get_timestamp_size() == E5_total_exec_rounds + 1) {
        e5_helper.log_skew_to_balance_exec_times();
        brane::actor_engine().exit();
    }
}
