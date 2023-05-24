//
// Created by everlighting on 2021/4/15.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "common/schedule_helpers.hh"
#include "cq4_op_1x0x0_where_loopOut.act.h"
#include "cq4_op_1x0x1_where_loopUntil.act.h"
#include "cq4_op_1x1_whereBranchSync.act.h"

thread_local std::unordered_set<int64_t> cq4_op_1x0x1_where_loopUntil::_successful_vertices{};
thread_local std::unordered_set<int64_t> cq4_op_1x0x1_where_loopUntil::_failed_vertices{};

void cq4_op_1x0x1_where_loopUntil::process(VertexBatch &&input) {
    if (_have_sent) { return; }

    VertexBatch next_loop_output{input.size()};
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_vertices_have_seen.find(input[i]) == _vertices_have_seen.end()) {
            _vertices_have_seen.insert(input[i]);

            bool successful = false;
            if (_successful_vertices.count(input[i]) > 0) {
                successful = true;
            } else if (_failed_vertices.count(input[i]) == 0) {
                auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _workAt_label);
                while (iter.next(out_neighbour)) {
                    if (_expect_universities.count(out_neighbour) > 0) {
                        successful = true;
                        break;
                    }
                }
                if (successful) {
                    _successful_vertices.insert(input[i]);
                } else {
                    _failed_vertices.insert(input[i]);
                }
            }
            if (successful && !_have_sent) {
                _sync_ref->set_successful();
                _have_sent = true;
                _sync_flag = true;
                return;
            }
            if (_cur_loop_times < _max_loop_times) {
                next_loop_output.emplace_back(input[i]);
            }
        }
    }
    if (next_loop_output.size() > 0) {
        _next_loop_ref->process(std::move(next_loop_output));
        _next_loop_flag = true;
    }
}

void cq4_op_1x0x1_where_loopUntil::receive_eos(PathEos &&eos) {
    if (_sync_flag && _next_loop_flag) {
        auto sync_eos = eos.get_copy();
        sync_eos.append_step(2, 0);
        _sync_ref->receive_eos(std::move(sync_eos));
        _sync_flag = false;

        auto next_loop_eos = eos.get_copy();
        next_loop_eos.append_step(2, 1);
        _next_loop_ref->receive_eos(std::move(next_loop_eos));
        _next_loop_flag = false;
    } else if (!_sync_flag && _next_loop_flag) {
        _next_loop_ref->receive_eos(std::move(eos));
        _next_loop_flag = false;
    } else {
        _sync_ref->receive_eos(std::move(eos));
        _sync_flag = false;
    }
}

void cq4_op_1x0x1_where_loopUntil::initialize() {
    auto builder = get_scope();

    _cur_loop_times = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    enter_loop_instance_scope(&builder, _cur_loop_times);
    _next_loop_ref = builder.new_ref<next_loop_t>(0);

    builder.back_to_parent_scope();
    auto sync_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    auto where_branch_id = builder.get_current_scope_id();
    builder.reset_shard(where_branch_id % brane::global_shard_count());
    _sync_ref = builder.new_ref<sync_t>(sync_op_id);

    for (auto& company : cq4_helper.get_organisation_ids()) {
        _expect_universities.insert(company);
    }
}

void cq4_op_1x0x1_where_loopUntil::finalize() {
    delete _next_loop_ref;
    delete _sync_ref;
}

