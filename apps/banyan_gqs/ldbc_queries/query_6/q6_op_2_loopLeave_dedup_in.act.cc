//
// Created by everlighting on 2020/9/29.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q6_op_2_loopLeave_dedup_in.act.h"
#include "q6_op_3_filter_relationStore_out.act.h"

void q6_op_2_loopLeave_dedup_in::process(VertexBatch &&input) {
    int64_t in_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_person_have_seen.find(input[i]) == _person_have_seen.end()) {
            _person_have_seen.insert(input[i]);
            auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(input[i], _in_label);
            while (iter.next(in_neighbour)) {
                unsigned loc;
                if (at_e5) {
                    loc = load_balancer::locate(in_neighbour);
                } else {
                    loc = locate_helper::locate(in_neighbour);
                }
                _ds_group_hdl->emplace_data(loc, in_neighbour);
            }
        }
    }
    _ds_group_hdl->flush();
}

void q6_op_2_loopLeave_dedup_in::receive_eos(Eos &&eos) {
    _ds_group_hdl->receive_eos();
}

void q6_op_2_loopLeave_dedup_in::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q6_op_2_loopLeave_dedup_in::finalize() {
    delete _ds_group_hdl;
}
