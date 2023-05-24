//
// Created by everlighting on 2020/11/18.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q10_op_1_out_sideOut.act.h"
#include "q10_op_2_out.act.h"
#include "q10_op_2_tagStore_filter.act.h"

void q10_op_1_out_sideOut::process(VertexBatch &&input) {
    int64_t out_friend;
    int64_t tag_id;
    for (unsigned i = 0; i < input.size(); i++) {
        auto tag_iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _side_out_label);
        while (tag_iter.next(tag_id)) {
            for (unsigned s_id = 0; s_id < brane::global_shard_count(); s_id++) {
                _side_ds_group_hdl->emplace_data(s_id, tag_id);
            }
        }

        auto friend_iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (friend_iter.next(out_friend)) {
            _ds_group_hdl->emplace_data(locate_helper::locate(out_friend), out_friend);
        }
    }
    _side_ds_group_hdl->flush();
    _ds_group_hdl->flush();
}

void q10_op_1_out_sideOut::receive_eos(Eos &&eos) {
    _side_ds_group_hdl->receive_eos();
    _ds_group_hdl->receive_eos();
}

void q10_op_1_out_sideOut::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _side_ds_group_hdl = new side_ds_group_t(builder, ds_id);
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q10_op_1_out_sideOut::finalize() {
    delete _side_ds_group_hdl;
    delete _ds_group_hdl;
}
