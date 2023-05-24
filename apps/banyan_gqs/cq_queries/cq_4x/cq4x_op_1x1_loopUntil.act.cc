//
// Created by everlighting on 2021/4/16.
//


#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "common/schedule_helpers.hh"
#include "cq4x_op_1x1_loopUntil.act.h"
#include "cq4x_op_1x0_loopOut.act.h"
#include "cq4x_op_2_loopLeave_dedup.act.h"

thread_local std::unordered_set<int64_t> cq4x_op_1x1_loopUntil::_successful_vertices{};
thread_local std::unordered_set<int64_t> cq4x_op_1x1_loopUntil::_failed_vertices{};
thread_local std::unordered_set<int64_t> cq4x_op_1x1_loopUntil::_vertices_have_sent{};

void cq4x_op_1x1_loopUntil::process(vertex_friendId_Batch &&input) {
    VertexBatch outside_output{input.size()};
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_vertices_have_sent.count(input[i].friend_id) == 0) {
            if (_successful_vertices.count(input[i].v_id) > 0) {
                _vertices_have_sent.insert(input[i].friend_id);
                outside_output.emplace_back(input[i].friend_id);
            } else if (_failed_vertices.count(input[i].v_id) == 0) {
                bool successful = false;
                auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].v_id, _workAt_label);
                while (iter.next(out_neighbour)) {
                    if (_expect_universities.count(out_neighbour) > 0) {
                        successful = true;
                        break;
                    }
                }
                if (successful) {
                    _successful_vertices.insert(input[i].v_id);
                    _vertices_have_sent.insert(input[i].friend_id);
                    outside_output.emplace_back(input[i].friend_id);
                } else {
                    _failed_vertices.insert(input[i].v_id);
                }
            }
        }
    }
    if (outside_output.size() > 0) {
        _loop_sync_hdl->process(std::move(outside_output));
    }

    vertex_friendId_Batch next_loop_output{input.size()};
    for (unsigned i = 0; i < input.size(); i++) {
        if ((_vertices_have_sent.count(input[i].friend_id) == 0) && (_cur_loop_times < _max_loop_times)) {
            next_loop_output.emplace_back(input[i].v_id, input[i].friend_id);
        }
    }
    if (next_loop_output.size() > 0) {
        _next_loop_hdl->process(std::move(next_loop_output));
    }
}

void cq4x_op_1x1_loopUntil::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _next_loop_hdl->receive_eos() : _loop_sync_hdl->receive_eos();
    }
}

void cq4x_op_1x1_loopUntil::initialize() {
    auto builder = get_scope();

    _cur_loop_times = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    enter_loop_instance_scope(&builder, _cur_loop_times);
    _next_loop_hdl = new next_loop_t(builder, 0);

    builder.back_to_parent_scope();
    auto sync_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    _loop_sync_hdl = new loop_sync_t(builder, sync_op_id);

    for (auto& company : cq4_helper.get_organisation_ids()) {
        _expect_universities.insert(company);
    }
}

void cq4x_op_1x1_loopUntil::finalize() {
    delete _next_loop_hdl;
    delete _loop_sync_hdl;
}

