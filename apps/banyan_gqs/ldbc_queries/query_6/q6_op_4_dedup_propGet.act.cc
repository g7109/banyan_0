#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/query_helpers.hh"
#include "q6_op_3_filter_relationStore_out.act.h"
#include "q6_op_4_dedup_propGet.act.h"

void q6_op_4_dedup_propGet::process(VertexBatch&& input) {
    std::tuple<char*, int> prop_loc_info;
    std::string name;
    for (unsigned i = 0; i < input.size(); i++) {
        if (_tags_have_seen.count(input[i]) == 0) {
            _tags_have_seen.insert(input[i]);

            auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
            prop_loc_info = prop_iter.next(_name_propId);
            name = storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info));

            if (name == _filter_tag_name) {
                for (unsigned s_id = 0; s_id < brane::global_shard_count(); s_id++) {
                    _ds_group_hdl->emplace_data<&i_q6_op_3_filter_relationStore_out::record_filter_tag>(s_id, input[i]);
                }
            }

            _tag_names[input[i]] = std::move(name);
        }
    }
    _ds_group_hdl->flush<&i_q6_op_3_filter_relationStore_out::record_filter_tag>();
}

void q6_op_4_dedup_propGet::record_tag_counts(vertex_count_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_tag_counts.count(input[i].v_id) > 0) {
            _tag_counts[input[i].v_id] += input[i].count;
        } else {
            _tag_counts[input[i].v_id] = input[i].count;
        }
    }
}

void q6_op_4_dedup_propGet::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2) {
            _ds_group_hdl->receive_eos();
            _eos_rcv_num = 0;
            _at_stage2 = true;
            return;
        }
        local_order();
        _sync_hdl->receive_eos();
    }
}

void q6_op_4_dedup_propGet::local_order() {
    for (auto& iter: _tag_counts) {
        _order_heap.emplace(iter.second, std::move(_tag_names[iter.first]));
        if (_order_heap.size() > _expect_num) {
            _order_heap.pop();
        }
    }
    if (!_order_heap.empty()) {
        std::vector<node> local_results;
        local_results.reserve(_order_heap.size());
        while (!_order_heap.empty()) {
            local_results.push_back(std::move(_order_heap.top()));
            _order_heap.pop();
        }
        count_name_Batch output{static_cast<uint32_t>(local_results.size())};
        for (auto& n : local_results) {
            output.emplace_back(n.count, n.tag_name);
        }
        _sync_hdl->process(std::move(output));
    }
}

void q6_op_4_dedup_propGet::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    _ds_group_hdl = new ds_group_t(builder, _ds_op_id);

    unsigned sync_shard = at_ic ? 0 : query_id % brane::global_shard_count();
    auto sync_builder = brane::scope_builder(sync_shard, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 5);

    _filter_tag_name = q6_helper.get_tag_name(query_id);
}

void q6_op_4_dedup_propGet::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}