//
// Created by everlighting on 2021/3/25.
//

#include "q10_op_2_tagStore_filter.act.h"
#include "q10_op_3_filter_in.act.h"
#include "q10_op_4_globalSync_order_limit.act.h"

void q10_op_2_tagStore_filter::process(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _tags.insert(input[i]);
    }
}

void q10_op_2_tagStore_filter::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _eos_ds_group_hdl->receive_eos();
    }
}

void q10_op_2_tagStore_filter::filter_commonCount(vertex_friendId_Batch &&input) {
    VertexBatch output{input.size()};
    int64_t tag_id;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].v_id, _out_label_for_filter);
        while (iter.next(tag_id)) {
            if (_tags.count(tag_id) > 0) {
                output.push_back(input[i].friend_id);
                break;
            }
        }
    }
    if (output.size() > 0) {
        _sync_hdl->Get()->count_common(std::move(output));
    }
}

void q10_op_2_tagStore_filter::receive_stage2_eos() {
    if (++_stage2_eos_rcv_num == _stage2_expect_eos_num) {
        _sync_hdl->receive_eos();
    }
}

void q10_op_2_tagStore_filter::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto eos_ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _eos_ds_group_hdl = new eos_ds_group_t(builder, eos_ds_id, 1);

    unsigned sync_shard = at_ic ? 0 : query_id % brane::global_shard_count();
    auto sync_builder = brane::scope_builder(sync_shard, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 4);
}

void q10_op_2_tagStore_filter::finalize() {
    delete _eos_ds_group_hdl;
    delete _sync_hdl;
}
