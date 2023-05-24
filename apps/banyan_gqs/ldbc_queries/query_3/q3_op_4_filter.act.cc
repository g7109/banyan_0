//
// Created by everlighting on 2020/11/13.
//

#include "common/query_helpers.hh"
#include "q3_op_4_filter.act.h"
#include "q3_op_8_group_and_order.act.h"

void q3_op_4_filter::process(vertex_intProp_Batch &&vertices) {
    for (unsigned i = 0; i < vertices.size(); i++) {
        if (_neither_x_or_y.count(vertices[i].v_id) > 0) {
            _ds_group_hdl->emplace_data(locate_helper::locate(vertices[i].prop), vertices[i].prop);
            continue;
        } else if (_x_or_y.count(vertices[i].v_id) > 0) {
            continue;
        }
        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(vertices[i].v_id);
        auto prop_loc_info = prop_iter.next(_name_propId);
        auto name = storage::berkeleydb_helper::readString(
                std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
        if (name != _country_x && name != _country_y) {
            _neither_x_or_y.insert(vertices[i].v_id);
            _ds_group_hdl->emplace_data(locate_helper::locate(vertices[i].prop), vertices[i].prop);
        } else {
            _x_or_y.insert(vertices[i].v_id);
        }
    }
    _ds_group_hdl->flush();
}

void q3_op_4_filter::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void q3_op_4_filter::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto query_id = builder.get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 8);

    // Set prop checker
    _country_x = q3_helper.get_country_x(query_id);
    _country_y = q3_helper.get_country_y(query_id);
}

void q3_op_4_filter::finalize() {
    delete _sync_hdl;
    delete _ds_group_hdl;
}


