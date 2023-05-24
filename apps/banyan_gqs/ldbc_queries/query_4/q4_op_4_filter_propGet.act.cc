//
// Created by everlighting on 2020/9/17.
//

#include "q4_op_4_filter_propGet.act.h"
#include "q4_op_5_globalSync_order.act.h"

void q4_op_4_filter_propGet::record_excluded_tags(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _excluded_tags.insert(input[i]);
    }
}

void q4_op_4_filter_propGet::record_candidate_tags(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_candidate_tag_counts.find(input[i]) == _candidate_tag_counts.end()) {
            _candidate_tag_counts[input[i]] = 1;
        } else {
            _candidate_tag_counts[input[i]] += 1;
        }
    }
}

void q4_op_4_filter_propGet::process() {}

void q4_op_4_filter_propGet::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        filter_propGet_forward();
        _ds_sync_hdl->receive_eos();
    }
}

void q4_op_4_filter_propGet::filter_propGet_forward() {
    count_name_Batch output{batch_size};
    std::tuple<char*, int> prop_loc_info;
    std::string tag_name;
    for (auto& iter : _candidate_tag_counts) {
        if (_excluded_tags.find(iter.first) == _excluded_tags.end()) {
            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(iter.first);
            prop_loc_info = prop_iter.next(_name_propId);
            tag_name = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));

            output.emplace_back(iter.second, tag_name);
            if (output.size() == output.capacity()) {
                _ds_sync_hdl->process(std::move(output));
                output.reset(batch_size);
            }
        }
    }
    if (output.size() > 0) {
        _ds_sync_hdl->process(std::move(output));
    }
}

void q4_op_4_filter_propGet::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _ds_sync_hdl = new ds_sync_t(sync_builder, 5);
}

void q4_op_4_filter_propGet::finalize() {
    delete _ds_sync_hdl;
}
