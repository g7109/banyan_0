#include "q9_op_4_propGet.act.h"
#include "q9_op_4_globalSync_order_limit.act.h"

void q9_op_4_propGet::process(VertexBatch&& input) {
    vertex_string_Batch msg_contents{input.size()};
    std::tuple<char*, int> prop_loc_info;
    std::string content;
    for (unsigned i = 0; i < input.size(); i++) {
        auto label_id = storage::bdb_graph_handler::_local_graph->get_vertex_label_id(input[i]);
        auto prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);

        if (label_id == _post_labelId) {
            prop_loc_info = prop_iter.next(_imageFile_propId);
            content = std::get<0>(prop_loc_info) ? storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info)) : "";
        } else if (label_id == _comment_labelId) {
            prop_loc_info = prop_iter.next(_content_propId);
            content = std::get<0>(prop_loc_info) ? storage::berkeleydb_helper::readString(
                    std::get<0>(prop_loc_info), std::get<1>(prop_loc_info)) : "";
        } else { continue; }
        msg_contents.emplace_back(input[i], content);
    }
    _sync_hdl->Get()->record_msg_contents(std::move(msg_contents));
}

void q9_op_4_propGet::receive_eos(Eos &&eos) {
    _sync_hdl->receive_eos();
}

void q9_op_4_propGet::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    unsigned sync_shard = at_ic ? 0 : query_id % brane::global_shard_count();
    auto sync_builder = brane::scope_builder(sync_shard, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 4);
}

void q9_op_4_propGet::finalize() {
    delete _sync_hdl;
}

