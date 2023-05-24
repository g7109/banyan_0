#include "q2_op_1_out.act.h"
#include "q2_op_4_order_and_limit_display.act.h"

void q2_op_1_out::process(StartVertex &&input) {
    int64_t out_neighbour;
    auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input.val, _out_label);
    while (iter.next(out_neighbour)) {
        _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
    }
    _ds_group_hdl->flush();
}

void q2_op_1_out::receive_eos(Eos &&eos) {
    if (_ds_group_hdl->have_send_data()) {
        _ds_group_hdl->receive_eos();
    } else {
        _sync_hdl->Get()->finish_query();
    }
}

void q2_op_1_out::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto query_id = builder.get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 4);
}

void q2_op_1_out::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
