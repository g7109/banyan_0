//
// Created by everlighting on 2020/11/13.
//

#include "common/query_helpers.hh"
#include "q3_op_7_compare_count.act.h"
#include "q3_op_8_group_and_order.act.h"

void q3_op_7_compare_count::process(vertex_intProp_Batch &&vertices) {
    q3_result_node_Batch results{vertices.size()};
    unsigned x_cnt, y_cnt;
    for (unsigned i = 0; i < vertices.size(); i++) {
        if (_failed_locations.count(vertices[i].v_id) > 0) {
            continue;
        }
        if (_x_y_locations.count(vertices[i].v_id) > 0) {
            results.emplace_back(vertices[i].prop,
                                 _x_y_locations[vertices[i].v_id].first,
                                 _x_y_locations[vertices[i].v_id].second);
        } else {
            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(vertices[i].v_id);
            auto prop_loc_info = prop_iter.next(_prop_type_id);
            std::string prop_str = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
            x_cnt = (prop_str == _country_x) ? 1 : 0;
            y_cnt = (prop_str == _country_y) ? 1 : 0;
            if (x_cnt > 0 || y_cnt > 0) {
                results.emplace_back(vertices[i].prop, x_cnt, y_cnt);
                _x_y_locations[vertices[i].v_id] = std::make_pair(x_cnt, y_cnt);
            } else {
                _failed_locations.insert(vertices[i].v_id);
            }
        }
    }
    if (results.size() > 0) {
        _sync_hdl->process(std::move(results));
    }
}

void q3_op_7_compare_count::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _sync_hdl->receive_eos();
    }
}

void q3_op_7_compare_count::initialize() {
    // init sync operator ref
    auto query_id = get_scope().get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 8);

    _country_x = q3_helper.get_country_x(query_id);
    _country_y = q3_helper.get_country_y(query_id);
}

void q3_op_7_compare_count::finalize() {
    delete _sync_hdl;
}

