#include "common/query_helpers.hh"
#include "q1_executor.hh"
#include "q1_op_3_globalSync_order_limit.act.h"

void q1_op_3_globalSync_order_limit::process(vertex_pathLen_StrProp_Batch&& input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.emplace(input.path_lens[i], input.props[i], input.v_ids[i]);
        if (_order_heap.size() > _limit_num) {
            _order_heap.pop();
        }
    }
}

void q1_op_3_globalSync_order_limit::receive_eos(Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (at_ic) {
            benchmark_end_action();
        } else if (at_e3) {
            e3_end_action();
        } else if (at_e4) {
            e4_end_action();
        }
    }
}

void q1_op_3_globalSync_order_limit::log_results() {
    std::vector<heap_node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 1 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  Id = {}, LastName = {}, Path Step Numbers = {}\n",
                (*i).id % 1000000000000000L, (*i).lastName, (*i).path_step_num));
    }
}

void q1_op_3_globalSync_order_limit::benchmark_end_action() {
    q1_helper.record_end_time();
    log_results();

    auto query_id = get_scope().get_root_scope_id();
    brane::actor_engine().cancel_query_request(query_id, brane::GActorGroupTypeId).then([] {
        q1_helper.next_round();
        if (q1_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_1(q1_helper.get_start_person(q1_helper.current_query_id()));
        } else {
            q1_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}

void q1_op_3_globalSync_order_limit::e3_end_action() {
    auto query_id = get_scope().get_root_scope_id();
    if (query_id >= E3_concurrency) {
        e3_helper.record_small_end_time(query_id);
        brane::actor_engine().cancel_query(query_id, brane::GActorGroupTypeId);
        if (e3_helper.get_all_timestamp_size() % E3_concurrency == 0) {
//            std::vector<seastar::future<>> futs;
//            futs.reserve(E3_concurrency);
//            for (unsigned i = e3_helper.current_round() * E3_concurrency;
//                    i < (e3_helper.current_round() + 1) * E3_concurrency; i++) {
//                futs.push_back(brane::actor_engine().cancel_query_request(i, brane::GActorGroupTypeId));
//            }
//            seastar::when_all_succeed(futs.begin(), futs.end()).then([] {
//                e3_helper.next_round();
//            }, nullptr);
            e3_helper.next_round();
        }
    } else {
        brane::actor_engine().cancel_query(query_id, brane::GActorGroupTypeId);
    }
}

void q1_op_3_globalSync_order_limit::e4_end_action() {
    e4_helper.record_fg_end_time();
    auto query_id = get_scope().get_root_scope_id();
//    brane::actor_engine().cancel_query_request(query_id, brane::GActorGroupTypeId).then([query_id] {
    auto next_round_query_id = query_id + 1;
    if (next_round_query_id <= E4_fg_exec_rounds) {
        e4_exec_query_1(next_round_query_id);
    } else {
        e4_helper.log_fg_exec_times();
        brane::actor_engine().exit();
    }
//    }, nullptr);
}
