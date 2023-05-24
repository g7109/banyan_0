//
// Created by everlighting on 2020/10/10.
//

#include "q11_op_3_relationStore_dedup_out.act.h"
#include "q11_op_5_propGet.act.h"
#include "q11_op_6_globalSync_order_limit.act.h"

void q11_op_5_propGet::process(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_orgs_have_seen.find(input[i]) == _orgs_have_seen.end()) {
            _orgs_have_seen.insert(input[i]);
            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
            auto prop_loc_info = prop_iter.next(_prop_type_id);
            std::string prop_str = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
            _org_names[input[i]] = std::move(prop_str);
        }
    }
}

void q11_op_5_propGet::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        if (_notify_ds) {
            forward_organisation_names();
            _ds_hdl->Get()->receive_stage2_eos();
        } else {
            _sync_hdl->receive_eos();
        }
    }
}

void q11_op_5_propGet::forward_organisation_names() {
    if (_org_names.empty()) return;

    vertex_string_Batch output{static_cast<uint32_t>(_org_names.size())};
    for (auto& iter : _org_names) {
        output.emplace_back(iter.first, iter.second);
    }
    _ds_hdl->Get()->forward_filtered_relations(std::move(output));
}

void q11_op_5_propGet::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_hdl = new ds_t(builder, _ds_op_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 6);
}

void q11_op_5_propGet::finalize() {
    delete _ds_hdl;
    delete _sync_hdl;
}