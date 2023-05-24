//
// Created by everlighting on 2020/10/13.
//

#include "q7_op_2_friendStore.act.h"
#include "q7_op_2_in_localOrder.act.h"
#include "q7_op_3_globalSync_order_limit.act.h"

void q7_op_2_friendStore::process(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _friends.insert(input[i]);
    }
}

void q7_op_2_friendStore::filter_liker_within_friends(VertexBatch &&input) {
    VertexBatch likers_in_friends{input.size()};
    for (unsigned i = 0; i < input.size(); i++) {
        if (_friends.find(input[i]) != _friends.end()) {
            likers_in_friends.emplace_back(input[i]);
        }
    }
    if (likers_in_friends.size() > 0) {
        _sync_hdl->Get()->record_likers_within_friends(std::move(likers_in_friends));
    }
}

void q7_op_2_friendStore::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2) {
            _eos_ds_group_hdl->receive_eos();
            _eos_rcv_num = 0;
            _expect_eos_num = brane::global_shard_count();
            _at_stage2 = true;
            return;
        }

        _sync_hdl->receive_eos();
    }
}

void q7_op_2_friendStore::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _eos_ds_group_hdl = new eos_ds_group_t(builder, _eos_ds_op_id, 1);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 3);
}

void q7_op_2_friendStore::finalize() {
    delete _eos_ds_group_hdl;
    delete _sync_hdl;
}
