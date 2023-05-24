//
// Created by everlighting on 2020/10/12.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q12_op_6_unionOut.act.h"
#include "q12_op_7_filter_dedup.act.h"

void q12_op_6_unionOut::process(vertex_tagId_Batch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        _ds_group_hdl->emplace_data(locate_helper::locate(input[i].v_id), input[i].v_id, input[i].tag_id);
        if (_intermediate_results.count(input[i].v_id) > 0) {
            _ds_group_hdl->emplace_data(locate_helper::locate(_intermediate_results[input[i].v_id]),
                                        _intermediate_results[input[i].v_id], input[i].tag_id);
        } else {
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].v_id, _out_label);
            if (iter.next(out_neighbour)) {
                _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour, input[i].tag_id);
                _intermediate_results[input[i].v_id] = out_neighbour;
            }
        }

    }
    _ds_group_hdl->flush();
}

void q12_op_6_unionOut::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        _ds_group_hdl->receive_eos();
    }
}

void q12_op_6_unionOut::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
}

void q12_op_6_unionOut::finalize() {
    delete _ds_group_hdl;
}
