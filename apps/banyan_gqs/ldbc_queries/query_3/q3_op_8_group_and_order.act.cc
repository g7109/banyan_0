//
// Created by everlighting on 2020/9/27.
//

#include "q3_executor.hh"
#include "q3_op_8_group_and_order.act.h"

void q3_op_8_group_and_order::process(q3_result_node_Batch&& results) {
    std::unordered_map<int64_t, q3_result_node>::iterator iter;
    for(unsigned i = 0; i < results.size(); i++) {
        iter = _group_map.find(results[i].v_id);
        if(iter != _group_map.end()) {
            iter->second.x_count += results[i].x_count;
            iter->second.y_count += results[i].y_count;
        } else {
            _group_map.emplace(results[i].v_id, results[i]);
        }
    }
}

void q3_op_8_group_and_order::receive_eos(Eos &&eos) {
        if (++_eos_rcv_num == _expect_eos_num) {
            for(auto& iter: _group_map) {
                if(iter.second.x_count > 0 && iter.second.y_count > 0) {
                    _order_heap.emplace(iter.second);
                    if(_order_heap.size() > _expect_num) {
                        _order_heap.pop();
                    }
                }
            }
            if (at_ic) {
                benchmark_end_action();
            }
    }
}

void q3_op_8_group_and_order::log_results() {
    std::vector<q3_result_node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 3 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        write_log(seastar::format(
                "  FriendId: {},  Message In CountryX Num: {},  Message In CountryY Num: {}\n",
                (*i).v_id % 1000000000000000L, (*i).x_count, (*i).y_count));
    }
}

void q3_op_8_group_and_order::benchmark_end_action() {
    q3_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q3_helper.next_round();
        if (q3_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_3(q3_helper.get_start_person(q3_helper.current_query_id()));
        } else {
            q3_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
