//
// Created by everlighting on 2021/4/13.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq3_op_3x0_whereIn.act.h"
#include "cq3_op_3x1_whereOut.act.h"
#include "cq3_op_3x4_whereBranchSync.act.h"

void cq3_op_3x0_whereIn::process(TrivialStruct<int64_t>&& vertex) {
    int64_t in_neighbour;
    auto iter = storage::bdb_graph_handler::_local_graph->get_in_v(vertex.val, _in_label);
    while (iter.next(in_neighbour)) {
        _ds_manager->emplace_data(locate_helper::locate(in_neighbour), in_neighbour);
    }
    _ds_manager->flush();

    PathEos branch_init_eos;
    branch_init_eos.append_step(1, 0);
    _ds_manager->receive_eos_by(std::move(branch_init_eos));
}

void cq3_op_3x0_whereIn::initialize() {
    auto builder = get_scope();
    auto branch_id = builder.get_current_scope_id();
    auto ds_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;

    _ds_manager = new ds_manager_t(builder, ds_op_id);

    builder.reset_shard(branch_id % brane::global_shard_count());
    _ds_manager->set_sync_op(builder, _sync_op_id);
}

void cq3_op_3x0_whereIn::finalize() {
    delete _ds_manager;
}

