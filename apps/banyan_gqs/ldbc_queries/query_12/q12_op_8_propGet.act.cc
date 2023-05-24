//
// Created by everlighting on 2020/10/12.
//

#include "q12_op_4_filter_out.act.h"
#include "q12_op_8_propGet.act.h"
#include "q12_op_9_globalSync_group_order.act.h"

void q12_op_8_propGet::process(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_tags_have_seen.find(input[i]) == _tags_have_seen.end()) {
            _tags_have_seen.insert(input[i]);

            for (unsigned s_id = 0; s_id < brane::global_shard_count(); s_id++) {
                _ds_group_hdl->emplace_data<&i_q12_op_4_filter_and_out::record_filtered_tags>(s_id, input[i]);
            }

            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
            auto prop_loc_info = prop_iter.next(_prop_type_id);
            std::string prop_str = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
            _tag_names[input[i]] = std::move(prop_str);
        }
    }
    _ds_group_hdl->flush<&i_q12_op_4_filter_and_out::record_filtered_tags>();
}

void q12_op_8_propGet::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();

        forward_tag_names();
        _sync_hdl->receive_eos();
    }
}

void q12_op_8_propGet::forward_tag_names() {
    if (_tag_names.empty()) return;

    vertex_string_Batch output{static_cast<uint32_t>(_tag_names.size())};
    for (auto& iter : _tag_names) {
        output.emplace_back(iter.first, iter.second);
    }
    _sync_hdl->Get()->record_tag_names(std::move(output));
}

void q12_op_8_propGet::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 9);
}

void q12_op_8_propGet::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
