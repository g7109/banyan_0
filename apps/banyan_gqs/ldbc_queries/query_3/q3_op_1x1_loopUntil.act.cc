#include "q3_op_1x0_loopOut.act.h"
#include "q3_op_1x1_loopUntil.act.h"
#include "q3_op_2_loopLeave_dedup_out.act.h"

void q3_op_1x1_loopUntil::process(VertexBatch &&input) {
    if (_cur_loop_times < _max_loop_times) {
        VertexBatch next_loop_output{input.size()};
        for (unsigned i = 0; i < input.size(); i++) {
            if (_vertices_have_seen.find(input[i]) == _vertices_have_seen.end()) {
                _vertices_have_seen.insert(input[i]);
                next_loop_output.emplace_back(input[i]);
            }
        }
        if (next_loop_output.size() > 0) {
            _next_loop_hdl->process(std::move(input));
        }
    }
    if (_emit_flag || _cur_loop_times == _max_loop_times) {
        _loop_sync_hdl->process(std::move(input));
    }
}

void q3_op_1x1_loopUntil::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _next_loop_hdl->receive_eos() : _loop_sync_hdl->receive_eos();
    }
}

void q3_op_1x1_loopUntil::initialize() {
    auto builder = get_scope();

    _cur_loop_times = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    builder.enter_sub_scope(brane::scope<brane::actor_group<> >(_cur_loop_times));
    _next_loop_hdl = new next_loop_t(builder, 0);

    builder.back_to_parent_scope();
    auto sync_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    _loop_sync_hdl = new sync_t(builder, sync_op_id);
}

void q3_op_1x1_loopUntil::finalize() {
    delete _next_loop_hdl;
    delete _loop_sync_hdl;
}
