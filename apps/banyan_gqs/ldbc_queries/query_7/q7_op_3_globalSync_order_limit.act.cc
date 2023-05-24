#include "q7_executor.hh"
#include "q7_op_3_globalSync_order_limit.act.h"
#include "q7_op_3_propGet.act.h"

void q7_op_3_globalSync_order_limit::record_msg_props(vertex_IntProp_StrProp_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _msg_creationDates[input.v_ids[i]] = input.int_props[i];
        _msg_contents[input.v_ids[i]] = std::string{input.str_props[i].ptr, input.str_props[i].len};
    }
}

void q7_op_3_globalSync_order_limit::record_like_relations(like_relation_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.push(input[i]);
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q7_op_3_globalSync_order_limit::record_likers_within_friends(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _likers_within_friends.insert(input[i]);
    }
}

void q7_op_3_globalSync_order_limit::finish_query() {
    if (at_ic) {
        benchmark_end_action();
    }
}

void q7_op_3_globalSync_order_limit::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2 && !_order_heap.empty()) {
            _at_stage2 = true;
            _eos_rcv_num = 0;
            _expect_eos_num = brane::global_shard_count();
            get_props();
            return;
        }

        if (at_ic) {
            benchmark_end_action();
        }
    }
}

void q7_op_3_globalSync_order_limit::get_props() {
    _results.reserve(_order_heap.size());
    while (!_order_heap.empty()) {
        _results.push_back(_order_heap.top());
        _order_heap.pop();
    }

    for (auto& n: _results) {
        _prop_getter_hdl->emplace_data(locate_helper::locate(n.msg_id), n.msg_id);
    }
    _prop_getter_hdl->flush();
    _prop_getter_hdl->receive_eos();
}

void q7_op_3_globalSync_order_limit::initialize() {
    auto builder = get_scope();
    auto op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);
    _prop_getter_hdl = new prop_getter_group_t(builder, op_id);
}

void q7_op_3_globalSync_order_limit::finalize() {
    delete _prop_getter_hdl;
}

void q7_op_3_globalSync_order_limit::log_results() {
    write_log("[Query 7 finished]\n");
    for (auto i = _results.rbegin(); i != _results.rend(); i++) {
        write_log(seastar::format(
                "  LikePerson: {}, LikeDate: {}, MsgContent: {}, LikeLatency: {}, IsFriend: {}\n",
                (*i).liker % 1000000000000000L,
                (*i).like_date,
                _msg_contents[(*i).msg_id],
                (*i).like_date - _msg_creationDates[(*i).msg_id],
                _likers_within_friends.count((*i).liker) > 0 ? "true" : "false"));
    }
}

void q7_op_3_globalSync_order_limit::benchmark_end_action() {
    q7_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q7_helper.next_round();
        if (q7_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_7(q7_helper.get_start_person(q7_helper.current_query_id()));
        } else {
            q7_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}
