//
// Created by everlighting on 2020/11/18.
//

#include "common/query_helpers.hh"
#include "q10_op_3_filter_in.act.h"
#include "q10_op_2_tagStore_filter.act.h"
#include "q10_op_4_globalSync_order_limit.act.h"

void q10_op_3_filter_in::process(VertexBatch &&input) {
    vertex_intProp_Batch postCnt_by_friend{input.size()};
    int64_t in_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
        auto prop_loc_info = prop_iter.next(_prop_type_id);
        long birthday = storage::berkeleydb_helper::readLong(
                std::get<0>(prop_loc_info),std::get<1>(prop_loc_info));
        auto day = birthday % 100;
        auto month = (birthday % 10000) / 100;
        if ((month == _month && day >= 21) || (month == _next_month && day < 22)) {
            auto neighbour_iter = storage::bdb_graph_handler::_local_graph->get_in_v(input[i], _in_label);
            int64_t post_cnt = 0;
            while (neighbour_iter.next(in_neighbour)) {
                _friend_contents.push_back(std::make_pair(in_neighbour, input[i]));
                post_cnt++;
            }
            if (post_cnt > 0) {
                postCnt_by_friend.emplace_back(input[i], post_cnt);
            }
        }
    }
    if (postCnt_by_friend.size() > 0) {
        _sync_hdl->Get()->record_post_count(std::move(postCnt_by_friend));
    }
}

void q10_op_3_filter_in::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        forward_friend_contents();
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            _ds_group_hdl->Get(i)->receive_stage2_eos();
        }
        _sync_hdl->receive_eos();
    }
}

void q10_op_3_filter_in::forward_friend_contents() {
    for (auto& iter : _friend_contents) {
        _ds_group_hdl->emplace_data<&i_q10_op_2_tagStore_filter::filter_commonCount>(
                locate_helper::locate(iter.first), iter.first, iter.second);
    }
    _ds_group_hdl->flush<&i_q10_op_2_tagStore_filter::filter_commonCount>();
}

void q10_op_3_filter_in::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);

    unsigned sync_shard = at_ic ? 0 : query_id % brane::global_shard_count();
    auto sync_builder = brane::scope_builder(sync_shard, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 4);

    _month = q10_helper.get_month(query_id);
    _next_month = (_month == 12) ? 1 : (_month + 1);
}

void q10_op_3_filter_in::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
