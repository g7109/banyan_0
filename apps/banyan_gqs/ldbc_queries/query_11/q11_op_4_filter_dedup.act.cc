//
// Created by everlighting on 2020/10/9.
//

#include "common/query_helpers.hh"
#include "q11_op_4_filter_dedup.act.h"
#include "q11_op_5_propGet.act.h"
#include "q11_op_6_globalSync_order_limit.act.h"

void q11_op_4_filter_dedup::process(vertex_organisationId_Batch &&input) {
    for (unsigned i = 0; i < input.size(); i++) {
        if ((_successful_orgs.find(input[i].organisation_id) == _successful_orgs.end()) &&
            storage::bdb_graph_handler::_local_graph->get_vertex(
                    input[i].v_id, _prop_checker, _filter_label)) {
            _successful_orgs.insert(input[i].organisation_id);
            _ds_group_hdl->emplace_data(locate_helper::locate(input[i].organisation_id),
                                        input[i].organisation_id);
        }
    }
    _ds_group_hdl->flush();
}

void q11_op_4_filter_dedup::receive_eos(Eos &&eos) {
    _notify_ds |= eos.val;
    if (++_eos_rcv_num == _expect_eos_num) {
        _notify_ds ? _ds_group_hdl->receive_eos() : _sync_hdl->receive_eos();
    }
}

void q11_op_4_filter_dedup::initialize() {
    auto builder = get_scope();
    auto query_id = builder.get_root_scope_id();

    auto ds_id = *reinterpret_cast<uint32_t*>(_address + brane::GActorTypeInBytes) + 1;
    _ds_group_hdl = new ds_group_t(builder, ds_id);

    auto sync_builder = brane::scope_builder(0, brane::scope<brane::actor_group<> >(query_id));
    _sync_hdl = new sync_t(sync_builder, 6);

    _prop_checker.property_check.checker = q11_helper.get_country_name(query_id);
}

void q11_op_4_filter_dedup::finalize() {
    delete _ds_group_hdl;
    delete _sync_hdl;
}
