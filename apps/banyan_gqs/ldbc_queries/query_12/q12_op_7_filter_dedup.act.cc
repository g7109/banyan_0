//
// Created by everlighting on 2020/10/12.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "q12_op_7_filter_dedup.act.h"
#include "q12_op_8_propGet.act.h"

void q12_op_7_filter_dedup::process(vertex_tagId_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_successful_tags.find(input[i].tag_id) == _successful_tags.end()) {
            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i].v_id);
            auto prop_loc_info = prop_iter.next(_name_propId);
            auto name = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
            if (name == _tagClass_name) {
                _successful_tags.insert(input[i].tag_id);
                _ds_group_hdl->emplace_data(locate_helper::locate(input[i].tag_id), input[i].tag_id);
            }
        }
    }
    _ds_group_hdl->flush();
}

void q12_op_7_filter_dedup::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void q12_op_7_filter_dedup::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    _tagClass_name = q12_helper.get_tag_class_name(query_id);
}

void q12_op_7_filter_dedup::finalize() {
    delete _ds_group_hdl;
}
