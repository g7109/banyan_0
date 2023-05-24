#include "q9_executor.hh"
#include "q9_op_4_globalSync_order_limit.act.h"
#include "q9_op_4_propGet.act.h"

void q9_op_4_globalSync_order_limit::record_create_relations(create_relation_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _order_heap.emplace(input[i].msg_id, input[i].creation_date, input[i].creator);
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
}

void q9_op_4_globalSync_order_limit::record_msg_contents(vertex_string_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _msg_contents[input.v_ids[i]] = std::string{input.strs[i].ptr, input.strs[i].len};
    }
}

void q9_op_4_globalSync_order_limit::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2 && !_order_heap.empty()) {
            _at_stage2 = true;
            _eos_rcv_num = 0;
            get_props();
            return;
        }

        if (at_ic) {
            benchmark_end_action();
        } else if (at_e3) {
            e3_end_action();
        } else if (at_e4) {
            e4_end_action();
        }
    }
}

void q9_op_4_globalSync_order_limit::initialize() {
    auto builder = get_scope();
    auto op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes);
    _prop_getter_hdl = new prop_getter_group_t(builder, op_id);
}

void q9_op_4_globalSync_order_limit::finalize() {
    delete _prop_getter_hdl;
}

void q9_op_4_globalSync_order_limit::get_props() {
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

void q9_op_4_globalSync_order_limit::log_results() {
    write_log("[Query 9 finished]\n");
    for (auto i = _results.rbegin(); i != _results.rend(); i++) {
        write_log(seastar::format(
                "  MsgId: {}, Content: {}, Creation Date: {}, Creator: {}\n",
                (*i).msg_id % 1000000000000000L, _msg_contents[(*i).msg_id], (*i).date, (*i).creator_id % 1000000000000000L));
    }
}

void q9_op_4_globalSync_order_limit::benchmark_end_action() {
    q9_helper.record_end_time();
    log_results();

    brane::actor_engine().cancel_query_request(get_scope().get_root_scope_id(), brane::GActorGroupTypeId).then([] {
        q9_helper.next_round();
        if (q9_helper.current_round() < (exec_epoch * param_num)) {
            benchmark_exec_query_9(q9_helper.get_start_person(q9_helper.current_query_id()));
        } else {
            q9_helper.compute_benchmark_exec_time();
            brane::actor_engine().exit();
        }
    }, nullptr);
}

void q9_op_4_globalSync_order_limit::e3_end_action() {
    auto query_id = get_scope().get_root_scope_id();
    if (query_id >= E3_concurrency) {
        e3_helper.record_big_end_time(query_id);
        brane::actor_engine().cancel_query(query_id, brane::GActorGroupTypeId);
        if (e3_helper.get_all_timestamp_size() % E3_concurrency == 0) {
//            std::vector<seastar::future<>> futs;
//            futs.reserve(E3_concurrency);
//            for (unsigned i = e3_helper.current_round() * E3_concurrency;
//                 i < (e3_helper.current_round() + 1) * E3_concurrency; i++) {
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

void q9_op_4_globalSync_order_limit::e4_end_action() {
    auto query_id = get_scope().get_root_scope_id();
    // brane::actor_engine().cancel_query_request(query_id, brane::GActorGroupTypeId).then([query_id] {
    auto next_query_id = query_id + E4_bg_query_num;
    e4_exec_query_9(next_query_id);
    // }, nullptr);
}
