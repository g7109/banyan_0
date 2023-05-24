#include "q1_op_1x1_loopUntil.act.h"
#include "q1_op_1x0_loopOut.act.h"
#include "q1_op_2_loopLeave_dedup_filter.act.h"

void q1_op_1x1_loopUntil::process(vertex_pathLen_Batch&& input) {
    if (_emit_flag || _cur_loop_times == _max_loop_times) {
        vertex_pathLen_Batch ds_output{input.size()};
        for (unsigned i = 0; i < input.size(); i++) {
            ds_output.emplace_back(input[i].v_id, input[i].path_len);
        }
        _loop_sync_hdl->process(std::move(ds_output));
    }
    if (_cur_loop_times < _max_loop_times) {
        _next_loop_hdl->process(std::move(input));
    }
}

void q1_op_1x1_loopUntil::receive_eos(Eos&& eos) {
    // Upstream Num = #shard. Data flow may be interrupted
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _next_loop_hdl->receive_eos() : _loop_sync_hdl->receive_eos();
    }
}

void q1_op_1x1_loopUntil::initialize() {
    auto builder = get_scope();

    _cur_loop_times = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    builder.enter_sub_scope(brane::scope<brane::actor_group<> >(_cur_loop_times));
    _next_loop_hdl = new next_loop_t(builder, 0);

    builder.back_to_parent_scope();
    auto sync_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    _loop_sync_hdl = new loop_sync_t(builder, sync_op_id);
}

void q1_op_1x1_loopUntil::finalize() {
    delete _next_loop_hdl;
    delete _loop_sync_hdl;
}
