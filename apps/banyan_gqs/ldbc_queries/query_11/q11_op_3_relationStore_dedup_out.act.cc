//
// Created by everlighting on 2020/10/9.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q11_op_3_relationStore_dedup_out.act.h"
#include "q11_op_4_filter_dedup.act.h"
#include "q11_op_6_globalSync_order_limit.act.h"

void q11_op_3_relationStore_dedup_out::process(work_relation_Batch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_relation_map.count(input[i].organisation_id) == 0) {
            _relation_map[input[i].organisation_id] = std::vector<std::pair<int64_t, int64_t> >{};
            _relation_map[input[i].organisation_id].reserve(batch_size);
        }
        _relation_map[input[i].organisation_id].push_back(
                std::make_pair(input[i].person_id, input[i].work_from_year));

        if (_org_have_seen.find(input[i].organisation_id) == _org_have_seen.end()) {
            _org_have_seen.insert(input[i].organisation_id);
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].organisation_id, _out_label);
            while (iter.next(out_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour),
                                            out_neighbour, input[i].organisation_id);
            }
        }
        _ds_group_hdl->flush();
    }
}

void q11_op_3_relationStore_dedup_out::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void q11_op_3_relationStore_dedup_out::forward_filtered_relations(vertex_string_Batch &&input) {
    work_relation_Batch relations{batch_size};
    for (unsigned i = 0; i < input.size(); i++) {
        for (auto& iter : _relation_map[input.v_ids[i]]) {
            relations.emplace_back(iter.first, iter.second, input.v_ids[i]);
            if (relations.size() == relations.capacity()) {
                _sync_hdl->Get()->record_relations(std::move(relations));
                relations.reset(batch_size);
            }
        }
    }
    if (relations.size() > 0) {
        _sync_hdl->Get()->record_relations(std::move(relations));
    }
    _sync_hdl->Get()->record_organisation_names(std::move(input));
}

void q11_op_3_relationStore_dedup_out::receive_stage2_eos() {
    _sync_hdl->receive_eos();
}

void q11_op_3_relationStore_dedup_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 6);
}

void q11_op_3_relationStore_dedup_out::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
