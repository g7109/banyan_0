//
// Created by everlighting on 2020/8/12.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q9_op_2_loopLeave_dedup_in.act.h"
#include "q9_op_3_filter_localOrder.act.h"

void q9_op_2_loopLeave_dedup_in::process(VertexBatch &&input) {
    int64_t in_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_person_have_seen.find(input[i]) == _person_have_seen.end()) {
            _person_have_seen.insert(input[i]);
            auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(input[i], _in_label);
            while (iter.next(in_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(in_neighbour), in_neighbour, input[i]);
            }
        }
    }
    _ds_group_hdl->flush();
}

void q9_op_2_loopLeave_dedup_in::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q9_op_2_loopLeave_dedup_in::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q9_op_2_loopLeave_dedup_in::finalize() {
    delete _ds_group_hdl;
}
