//
// Created by everlighting on 2020/10/9.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "q11_op_2_dedup_out.act.h"
#include "q11_op_3_relationStore_dedup_out.act.h"

int64_t q11_op_2_dedup_out::_work_from_year;

void q11_op_2_dedup_out::process(VertexBatch &&input) {
    int64_t out_neighbour;
    int64_t edge_prop;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_person_have_seen.find(input[i]) == _person_have_seen.end()) {
            _person_have_seen.insert(input[i]);
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v<out_edge_prop_predictor>(
                    input[i], _out_label);
            while (iter.next(out_neighbour)) {
                auto prop_iter = storage::bdb_graph_handler::_local_graph->get_out_edge_properties(
                        input[i], out_neighbour, _out_label);
                auto prop_loc_info = prop_iter.next(_edge_prop_type_id);
                edge_prop = storage::berkeleydb_helper::readLong(
                        std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
                _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour),
                                            input[i], edge_prop, out_neighbour);
            }
        }
    }
    _ds_group_hdl->flush();
}

void q11_op_2_dedup_out::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void q11_op_2_dedup_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    _work_from_year = q11_helper.get_work_from_year(query_id);
}

void q11_op_2_dedup_out::finalize() {
    delete _ds_group_hdl;
}

