#include "q12_executor.hh"
#include "q12_op_9_globalSync_group_order.act.h"

void q12_op_9_globalSync_group_order::record_tag_names(vertex_string_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _tag_names[input.v_ids[i]] = std::string{input.strs[i].ptr, input.strs[i].len};
    }
}

void q12_op_9_globalSync_group_order::record_filtered_relations(person_comment_tag_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_person_reply_map.count(input[i].person_id) == 0) {
            _person_reply_map[input[i].person_id] = std::unordered_set<int64_t>{};
        }
        _person_reply_map[input[i].person_id].insert(input[i].comment_id);
        if (_person_tag_map.count(input[i].person_id) == 0) {
            _person_tag_map[input[i].person_id] = std::unordered_set<int64_t>{};
        }
        _person_tag_map[input[i].person_id].insert(input[i].tag_id);
    }
}

void q12_op_9_globalSync_group_order::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        for (auto& iter: _person_reply_map) {
            _order_heap.emplace(iter.first, iter.second.size(), std::move(_person_tag_map[iter.first]));
            if (_order_heap.size() > _limit_num) {
                _order_heap.pop();
            }
        }

        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q12_op_9_globalSync_group_order::log_results() {
    std::vector<order_node> results;
    results.reserve(_order_heap.size());
    while(!_order_heap.empty()) {
        results.push_back(_order_heap.top());
        _order_heap.pop();
    }
    write_log("[Query 12 finished]\n");
    for(auto i = results.rbegin(); i != results.rend(); i++) {
        std::string all_tag_names = "";
        for(auto& tag_id : (*i).tag_ids) {
            all_tag_names += "[" + _tag_names[tag_id] + "]";
        }
        write_log(seastar::format(
                "  FriendId: {}, ReplyCount: {}, Tags: {}\n",
                (*i).friend_vertex % 1000000000000000L, (*i).reply_count, all_tag_names));
    }
}

void q12_op_9_globalSync_group_order::benchmark_end_action() {
    q12_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q12_helper.next_round();
        if (q12_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_12(q12_helper.get_start_person(q12_helper.current_query_id()));
        } else {
            q12_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
