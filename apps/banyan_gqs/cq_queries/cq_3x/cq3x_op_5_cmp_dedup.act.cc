#include "common/query_helpers.hh"
#include "cq3x_op_5_cmp_dedup.act.h"
#include "berkeley_db/storage/bdb_graph_handler.hh"

void cq3x_op_5_cmp_dedup::process(vertex_intProp_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if (_persons.count(input[i].prop) > 0) {
            continue;
        }

        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i].v_id);
        auto prop_loc_info = prop_iter.next(_name_propId);
        bool match = storage::berkeleydb_helper::compareString(
                std::get<0>(prop_loc_info), std::get<1>(prop_loc_info), _tag_type_name);
        if (match) {
            _persons.insert(input[i].prop);
            _order_heap.push(input[i].prop);
            if (_order_heap.size() > _expect_num) {
                _order_heap.pop();
            }
        }
    }
}

void cq3x_op_5_cmp_dedup::receive_eos(Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        if (!_order_heap.empty()) {
            VertexBatch output{static_cast<uint32_t>(_order_heap.size())};
            while (!_order_heap.empty()) {
                output.push_back(_order_heap.top());
                _order_heap.pop();
            }
            _ds_hdl->process(std::move(output));
        }
        _ds_hdl->receive_eos();
    }
}


void cq3x_op_5_cmp_dedup::initialize() {
    auto builder = get_scope();
    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    builder.reset_shard(0);
    _ds_hdl = new ds_t(builder, ds_id);
}

void cq3x_op_5_cmp_dedup::finalize() {
    delete _ds_hdl;
}
