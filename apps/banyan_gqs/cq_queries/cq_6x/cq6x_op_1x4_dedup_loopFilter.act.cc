//
// Created by everlighting on 2021/4/19.
//

#include "cq6x_op_1x4_dedup_loopFilter.act.h"
#include "cq6x_op_1x5_loopUntil.act.h"

void cq6x_op_1x4_loopFilter::process(vertex_friendId_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_persons.count(input[i].friend_id) > 0) {
            continue;
        }
        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i].v_id);
        auto prop_loc_info = prop_iter.next(_name_propId);
        bool match = storage::berkeleydb_helper::compareString(
                std::get<0>(prop_loc_info), std::get<1>(prop_loc_info), _tag_type_name);
        if (match) {
            _persons.insert(input[i].friend_id);
            _ds_group_hdl->emplace_data(locate_helper::locate(input[i].friend_id), input[i].friend_id);
        }
    }
    _ds_group_hdl->flush();
}

void cq6x_op_1x4_loopFilter::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void cq6x_op_1x4_loopFilter::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void cq6x_op_1x4_loopFilter::finalize() {
    delete _ds_group_hdl;
}
