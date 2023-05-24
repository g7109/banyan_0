#include "cq6_op_2_loopLeave_dedup.act.h"
#include "cq6_op_3_globalSync_limit.act.h"

void cq6_op_2_loopLeave_dedup::process(VertexBatch&& input) {
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

void cq6_op_2_loopLeave_dedup::receive_eos(Eos &&eos) {
    _ds_sync_hdl->receive_eos();
}

void cq6_op_2_loopLeave_dedup::initialize() {
    auto builder = get_scope();
    builder.reset_shard(0);
    _ds_sync_hdl = new ds_and_sync_t(builder, 3);
}

void cq6_op_2_loopLeave_dedup::finalize() {
    delete _ds_sync_hdl;
}






