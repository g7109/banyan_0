#include "cq3x_op_0_V_out.act.h"
#include "cq3x_op_1_unionOut.act.h"
#include "berkeley_db/storage/bdb_graph_handler.hh"

void cq3x_op_0_V_out::trigger(StartVertex&& vtx) {
    int64_t out_neighbour;
    auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(vtx.val, _out_label);
    while (iter.next(out_neighbour)) {
        _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
    }
    _ds_group_hdl->flush();
    _ds_group_hdl->receive_eos();
}

void cq3x_op_0_V_out::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void cq3x_op_0_V_out::finalize() {
    delete _ds_group_hdl;
}
