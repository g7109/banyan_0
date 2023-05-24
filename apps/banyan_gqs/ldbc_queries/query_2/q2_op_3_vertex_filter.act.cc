#include "common/query_helpers.hh"
#include "q2_op_3_vertex_filter.act.h"
#include "q2_op_4_order_and_limit_display.act.h"

void q2_op_3_vertex_filter::process(VertexBatch &&vertices) {
    vertex_intProp_Batch output{vertices.size()};
    for (unsigned i = 0; i < vertices.size(); i++) {
        auto exist = storage::bdb_graph_handler::_local_graph->get_vertex(vertices[i], _prop_checker, _vertex_label);
        if (exist) {
            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(vertices[i]);
            auto prop_loc_info = prop_iter.next(_prop_type_id);
            int64_t prop_value = storage::berkeleydb_helper::readLong(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
            output.emplace_back(vertices[i], prop_value);
        }
    }
    _sync_hdl->process(std::move(output));
}

void q2_op_3_vertex_filter::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _sync_hdl->receive_eos();
    }
}

void q2_op_3_vertex_filter::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 4);

    _prop_checker.property_check.checker = q2_helper.get_max_date(query_id);
}

void q2_op_3_vertex_filter::finalize() {
    delete _sync_hdl;
}