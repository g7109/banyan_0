//
// Created by everlighting on 2021/4/15.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq4_op_1x0x0_where_loopOut.act.h"
#include "cq4_op_1x0x1_where_loopUntil.act.h"
#include "cq4_op_1x1_whereBranchSync.act.h"

void cq4_op_1x0x0_where_loopOut::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
        while (iter.next(out_neighbour)) {
            _ds_manager->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
        }
    }
    _ds_manager->flush();
}

void cq4_op_1x0x0_where_loopOut::receive_eos(PathEos &&eos) {
    _ds_manager->receive_eos_by(std::move(eos));
}

void cq4_op_1x0x0_where_loopOut::initialize() {
    auto builder = get_scope();
    auto ds_op_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;

    _ds_manager = new ds_manager_t(builder, ds_op_id);

    builder.back_to_parent_scope();
    auto sync_op_id = builder.get_current_scope_id() + 1;
    builder.back_to_parent_scope();
    auto where_branch_id = builder.get_current_scope_id();
    builder.reset_shard(where_branch_id % brane::global_shard_count());
    _ds_manager->set_sync_op(builder, sync_op_id);
}

void cq4_op_1x0x0_where_loopOut::finalize() {
    delete _ds_manager;
}
