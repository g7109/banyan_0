//
// Created by everlighting on 2020/11/12.
//

#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "q6_op_3_filter_relationStore_out.act.h"
#include "q6_op_4_dedup_propGet.act.h"
#include "q6_op_5_globalSync_group_order.act.h"

void q6_op_3_filter_relationStore_out::process(VertexBatch &&input) {
    int64_t out_neighbour;
    for (unsigned i = 0; i < input.size(); i++) {
        if (storage::bdb_graph_handler::_local_graph->get_vertex(
                input[i], _prop_checker, _filter_label)) {
            _relations[input[i]] = std::unordered_set<int64_t>{};
            auto iter = storage::bdb_graph_handler::_local_graph->get_out_v(input[i], _out_label);
            while (iter.next(out_neighbour)) {
                _relations[input[i]].insert(out_neighbour);
                if (_tags_have_seen.count(out_neighbour) == 0) {
                    _tags_have_seen.insert(out_neighbour);

                    unsigned loc;
                    if (at_e5) {
                        loc = load_balancer::locate(out_neighbour);
                    } else {
                        loc = locate_helper::locate(out_neighbour);
                    }
                    _ds_group_hdl->emplace_data(loc, out_neighbour);
                }
            }
        }
    }
    _ds_group_hdl->flush();
}

void q6_op_3_filter_relationStore_out::record_filter_tag(VertexBatch &&input) {
    _filter_tag_id = input[0];
    _recorded = true;
}

void q6_op_3_filter_relationStore_out::receive_eos(Eos &&eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_at_stage2) {
            _ds_group_hdl->receive_eos();
            _eos_rcv_num = 0;
            _at_stage2 = true;
            return;
        }

        if (_recorded) { count_tags(); }
        _stage2_ds_group_hdl->receive_eos();
    }
}

void q6_op_3_filter_relationStore_out::count_tags() {
    std::unordered_map<int64_t, unsigned> tag_counts{};
    for (auto& iter : _relations) {
        if (iter.second.count(_filter_tag_id) == 0) {
            continue;
        }
        for (auto& tag : iter.second) {
            if (tag != _filter_tag_id) {
                if (tag_counts.count(tag) == 0) {
                    tag_counts[tag] = 1;
                } else {
                    tag_counts[tag] += 1;
                }
            }
        }
    }
    for (auto& iter : tag_counts) {
        unsigned loc;
        if (at_e5) {
            loc = load_balancer::locate(iter.first);
        } else {
            loc = locate_helper::locate(iter.first);
        }
        _stage2_ds_group_hdl->emplace_data<&i_q6_op_4_dedup_propGet::record_tag_counts>(
                loc, iter.first, iter.second);
    }
    _stage2_ds_group_hdl->flush<&i_q6_op_4_dedup_propGet::record_tag_counts>();
}

void q6_op_3_filter_relationStore_out::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);
    _stage2_ds_group_hdl = new stage2_ds_group_t(builder, ds_id);
}

void q6_op_3_filter_relationStore_out::finalize() {
    delete _ds_group_hdl;
    delete _stage2_ds_group_hdl;
}
