#include "common/query_helpers.hh"
#include "q9_op_3_filter_localOrder.act.h"

void q9_op_3_filter_localOrder::process(vertex_friendId_Batch &&input) {
    std::tuple<char*, int> prop_loc_info;
    int64_t creation_date;
    std::string content;
    for (unsigned i = 0; i < input.size(); i++) {
        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i].v_id);

        prop_loc_info = prop_iter.next(_creationDate_propId);
        creation_date = storage::berkeleydb_helper::readLong(
                std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));
        if (_compare(creation_date, _filter_creationDate)) {
            _order_heap.emplace(input[i].v_id, creation_date, input[i].friend_id);
            if (_order_heap.size() > _expect_num) {
                _order_heap.pop();
            }
        }
    }
}

void q9_op_3_filter_localOrder::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        forward_order_results();
        _ds_sync_hdl->receive_eos();
    }
}

void q9_op_3_filter_localOrder::forward_order_results() {
    if (_order_heap.empty()) { return; }

    std::vector<heap_node> local_results;
    local_results.reserve(_order_heap.size());
    while (!_order_heap.empty()) {
        local_results.push_back(_order_heap.top());
        _order_heap.pop();
    }

    create_relation_Batch output{static_cast<uint32_t>(local_results.size())};
    for (auto& n: local_results) {
        output.emplace_back(n.msg_id, n.creator_id, n.date);
    }
    _ds_sync_hdl->Get()->record_create_relations(std::move(output));
}

void q9_op_3_filter_localOrder::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    _filter_creationDate = q9_helper.get_max_date(query_id);

    unsigned sync_shard = at_ic ? 0 : query_id % brane::global_shard_count();
    auto sync_builder = brane::scope_builder(sync_shard, brane::scope<brane::actor_group<> >(query_id));
    _ds_sync_hdl = new ds_and_sync_t(sync_builder, 4);
}

void q9_op_3_filter_localOrder::finalize() {
    delete _ds_sync_hdl;
}
