#pragma once

#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/db_initializer.hh"
#include "common/props_predictor.hh"
#include "common/query_helpers.hh"
#include "q6_op_0_V_loopEnter.act.h"

//class q6_tag_prop_predictor: public storage::base_predicate {
//public:
//    long prop_name_id = storage::berkeleydb_helper::getPropertyID("name");
//    StringPropCheck<true> property_check{query6_helper::get_filter_tag_name()};
//
//    bool operator()(Dbc *cursor, Dbt &key, Dbt &value) override {
//        return checkVertexPredicate(cursor, key, value, &prop_name_id, property_check);
//    }
//};
//
//inline seastar::future<> set_q6_filter_tag_id(unsigned q_id) {
//    return seastar::parallel_for_each(boost::irange<unsigned>(0u, brane::global_shard_count()),
//            [q_id] (unsigned id) {
//        return seastar::smp::submit_to(id, [q_id] {
//            query6_helper::set_filter_tag_name(q6_helper.get_tag_name(q_id));
//            auto partition_ids = storage::bdb_graph_handler::_local_graph->get_partition_ids(brane::global_shard_id());
//            int64_t tag_id;
//            for (auto& p_id : partition_ids) {
//                auto iter = storage::bdb_graph_handler::_local_graph->scan_vertex<q6_tag_prop_predictor>(p_id, "Tag");
//                while (iter.next(tag_id)) {
//                    if (tag_id % graph_partition_num != p_id) {
//                        continue;
//                    }
//                    q6_helper.set_filter_tag_id(tag_id);
//                    return;
//                }
//            }
//        });
//    });
//}

inline void benchmark_exec_query_6(int64_t start_vertex) {
    write_log(seastar::format(
            "[Exec query 6] query id = {}, start vertex = {}\n",
            q6_helper.current_query_id(), start_vertex));
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(q6_helper.current_query_id()));
    auto actor_ref_V = builder.build_ref<i_q6_op_0_V_loopEnter>(0);

    q6_helper.record_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e1_exec_query_6(unsigned query_id) {
    write_log(seastar::format("[Exec query 6] query id = {}\n", query_id));
    auto start_vertex = q6_helper.get_start_person(query_id);
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q6_op_0_V_loopEnter>(0);

    if (query_id != 0) {
        e1_helper.record_start_time();
    }
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e5_exec_query_6(unsigned query_id) {
    write_log(seastar::format("[Exec query 6] query id = {}\n", query_id));
    auto start_vertex = q6_helper.get_start_person(query_id);
    brane::scope_builder builder(load_balancer::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q6_op_0_V_loopEnter>(0);

    e5_helper.record_start_time(query_id);
    actor_ref_V.trigger(StartVertex{start_vertex});
};