//
// Created by everlighting on 2020/9/28.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "q5_op_3_dedup_friendStore_in.act.h"
#include "q5_op_4_dedup_out.act.h"
#include "q5_op_6_group_localOrder.act.h"
#include "q5_op_7_globalSync_order.act.h"

int64_t q5_op_3_dedup_friendStore_in::_min_date;

void q5_op_3_dedup_friendStore_in::process(VertexBatch &&input) {
    int64_t in_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_friend_store.find(input[i]) == _friend_store.end()) {
            _friend_store.insert(input[i]);

            auto iter = storage::bdb_graph_handler::_local_graph->get_in_v<in_edge_prop_predictor>(input[i], _in_label);
            while (iter.next(in_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour);
            }
        }
    }
    _ds_group_hdl->flush();
}

void q5_op_3_dedup_friendStore_in::record_msgCreators_in_forum(vertex_forumId_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _relations.push_back(std::make_pair(input[i].v_id, input[i].forum_id));
    }
}

void q5_op_3_dedup_friendStore_in::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2) {
            _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
            _eos_rcv_num = 0;
            _at_stage2 = true;
            return;
        }

        filter_count();
        _stage2_ds_group_hdl->receive_eos();
    }
}

void q5_op_3_dedup_friendStore_in::filter_count() {
    std::unordered_map<int64_t, unsigned> counts{};
    for (auto& pair : _relations) {
        if (_friend_store.find(pair.first) != _friend_store.end()) {
            if (counts.find(pair.second) == counts.end()) {
                counts[pair.second] = 1;
            } else {
                counts[pair.second] += 1;
            }
        }
    }
    for (auto& iter : counts) {
        _stage2_ds_group_hdl->emplace_data(locate_helper::locate(iter.first), iter.first, iter.second);
    }
    _stage2_ds_group_hdl->flush();
}

void q5_op_3_dedup_friendStore_in::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    _stage2_ds_group_hdl = new stage2_ds_group_t(builder, _stage2_ds_op_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 7);

    _min_date = q5_helper.get_min_date(query_id);

    _relations.reserve(10000);
}

void q5_op_3_dedup_friendStore_in::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
