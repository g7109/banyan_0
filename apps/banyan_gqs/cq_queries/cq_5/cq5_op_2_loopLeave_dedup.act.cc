//
// Created by everlighting on 2021/4/9.
//
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq5_op_2_loopLeave_dedup.act.h"

#include "cq5_op_1x1_loopUntil.act.h"

void cq5_op_2_loopLeave_dedup::process(VertexBatch &&input) {
    VertexBatch output{input.size()};
    for (unsigned i = 0; i < input.size(); i++) {
        if (_vertices_have_seen.find(input[i]) == _vertices_have_seen.end()) {
            _vertices_have_seen.insert(input[i]);
            output.push_back(input[i]);
        }
    }
    if (output.size() > 0) {
        _ds_hdl->process(std::move(output));
    }
}

void cq5_op_2_loopLeave_dedup::receive_eos(Eos &&eos) {
    _ds_hdl->receive_eos();
}

void cq5_op_2_loopLeave_dedup::initialize() {
    auto builder = get_scope();
    _ds_hdl = new ds_t(builder, _ds_op_id);
}

void cq5_op_2_loopLeave_dedup::finalize() {
    cq5_op_1x1_loopUntil::_successful_vertices.clear();
    cq5_op_1x1_loopUntil::_failed_vertices.clear();
    delete _ds_hdl;
}
