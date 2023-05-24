//
// Created by everlighting on 2021/4/13.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq6_op_1x1x1_whereOut.act.h"
#include "cq6_op_1x1x2_dedup_whereOut.act.h"
#include "cq6_op_1x1x4_whereBranchSync.act.h"

void cq6_op_1x1x1_whereOut::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_manager->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_manager->flush();
}

void cq6_op_1x1x1_whereOut::receive_eos(PathEos &&eos) {
    _ds_manager->receive_eos_by(std::move(eos));
}

void cq6_op_1x1x1_whereOut::initialize() {
    auto builder = get_scope();
    auto branch_id = builder.get_current_scope_id();
    auto ds_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;

    _ds_manager = new ds_manager_t(builder, ds_op_id);

    builder.reset_shard(branch_id % brane::global_shard_count());
    _ds_manager->set_sync_op(builder, _sync_op_id);
}

void cq6_op_1x1x1_whereOut::finalize() {
    delete _ds_manager;
}
