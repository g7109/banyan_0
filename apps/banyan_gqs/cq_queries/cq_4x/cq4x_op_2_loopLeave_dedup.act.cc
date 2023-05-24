//
// Created by everlighting on 2021/4/16.
//


#include "cq4x_op_1x1_loopUntil.act.h"
#include "cq4x_op_2_loopLeave_dedup.act.h"
#include "cq4x_op_3_globalSync_limit.act.h"

void cq4x_op_2_loopLeave_dedup::process(VertexBatch &&input) {
    VertexBatch output{input.size()};
    for (unsigned i = 0; i < input.size(); i++) {
        if (_vertices_have_seen.find(input[i]) == _vertices_have_seen.end()) {
            _vertices_have_seen.insert(input[i]);
            output.emplace_back(input[i]);
        }
    }
    if (output.size() > 0) {
        _ds_sync_hdl->process(std::move(output));
    }
}

void cq4x_op_2_loopLeave_dedup::receive_eos(Eos &&eos) {
    _ds_sync_hdl->receive_eos();
}

void cq4x_op_2_loopLeave_dedup::initialize() {
    auto builder = get_scope();
    builder.reset_shard(0);
    _ds_sync_hdl = new ds_and_sync_t(builder, 3);
}

void cq4x_op_2_loopLeave_dedup::finalize() {
    cq4x_op_1x1_loopUntil::_successful_vertices.clear();
    cq4x_op_1x1_loopUntil::_failed_vertices.clear();
    cq4x_op_1x1_loopUntil::_vertices_have_sent.clear();
    delete _ds_sync_hdl;
}
