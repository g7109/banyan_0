#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q12_op_4_filter_out.act.h"
#include "q12_op_5_dedup_out.act.h"
#include "q12_op_9_globalSync_group_order.act.h"

void q12_op_4_filter_and_out::process(person_replyOf_msg_Batch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        auto label_id = storage::bdb_graph_handler::_local_graph->get_vertex_label_id(input[i].msg_id);
        if (label_id == _post_labelId) {
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i].msg_id, _out_label);
            while (iter.next(out_neighbour)) {
                if (_relations.count(out_neighbour) == 0) {
                    _relations[out_neighbour] = std::vector<std::pair<int64_t, int64_t> >{};
                    _relations[out_neighbour].reserve(100);

                    _ds_group_hdl->emplace_data(locate_helper::locate(out_neighbour), out_neighbour);
                }
                _relations[out_neighbour].push_back(std::make_pair(input[i].person_id, input[i].comment_id));
            }
        }
    }
    _ds_group_hdl->flush();
}

void q12_op_4_filter_and_out::record_filtered_tags(VertexBatch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        _filtered_tags.insert(input[i]);
    }
}

void q12_op_4_filter_and_out::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2) {
            if (_notify_ds) {
                _ds_group_hdl->receive_eos();
                _eos_rcv_num = 0;
                _at_stage2 = true;
                _notify_ds = false;
            } else {
                _sync_hdl->receive_eos(2);
            }
            return;
        }

        forward_filtered_relations();
        _sync_hdl->receive_eos();
    }
}

void q12_op_4_filter_and_out::forward_filtered_relations() {
    person_comment_tag_Batch output{batch_size};
    for (auto& tag : _filtered_tags) {
        if (_relations.count(tag) > 0) {
            for (auto& iter : _relations[tag]) {
                output.emplace_back(iter.first, iter.second, tag);
                if (output.size() == output.capacity()) {
                    _sync_hdl->Get()->record_filtered_relations(std::move(output));
                    output.reset(batch_size);
                }
            }
        }
    }
    if (output.size() > 0) {
        _sync_hdl->Get()->record_filtered_relations(std::move(output));
    }
}

void q12_op_4_filter_and_out::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 9);
}

void q12_op_4_filter_and_out::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
