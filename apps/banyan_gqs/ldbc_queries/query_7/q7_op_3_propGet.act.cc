//
// Created by everlighting on 2021/4/6.
//

#include "q7_op_3_propGet.act.h"
#include "q7_op_3_globalSync_order_limit.act.h"

void q7_op_3_propGet::process(VertexBatch &&input) {
    vertex_IntProp_StrProp_Batch msg_props{input.size()};
    int64_t msg_creationDate;
    std::string msg_content;
    for (unsigned i = 0; i < input.size(); i++) {
        auto msg_prop_iter = storage::bdb_graph_handler::_local_graph->get_vertex_properties(input[i]);
        auto msg_label_id = storage::bdb_graph_handler::_local_graph->get_vertex_label_id(input[i]);
        auto msg_creationDate_loc_info = msg_prop_iter.next(_creationDate_propId);
        msg_creationDate = storage::berkeleydb_helper::readLong(
                std::get<0>(msg_creationDate_loc_info), std::get<1>(msg_creationDate_loc_info));
        if (msg_label_id == _post_labelId) {
            auto msg_content_loc_info = msg_prop_iter.next(_imageFile_propId);
            msg_content = storage::berkeleydb_helper::readString(
                    std::get<0>(msg_content_loc_info), std::get<1>(msg_content_loc_info));
        } else if (msg_label_id == _comment_labelId) {
            auto msg_content_loc_info = msg_prop_iter.next(_content_propId);
            msg_content = storage::berkeleydb_helper::readString(
                    std::get<0>(msg_content_loc_info), std::get<1>(msg_content_loc_info));
        } else {
            std::cout << "Error dataset!\n";
            abort();
        }
        msg_props.emplace_back(input[i], msg_creationDate, msg_content);
    }
    _sync_hdl->Get()->record_msg_props(std::move(msg_props));
}

void q7_op_3_propGet::receive_eos(Eos &&eos) {
    _sync_hdl->receive_eos();
}

void q7_op_3_propGet::initialize() {
    auto query_id = get_scope().get_root_scope_id();
    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 3);
}

void q7_op_3_propGet::finalize() {
    delete _sync_hdl;
}
