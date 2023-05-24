#pragma once

#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/db_initializer.hh"
#include "common/query_helpers.hh"
#include "q9_op_0_V_loopEnter.act.h"

inline void benchmark_exec_query_9(int64_t start_vertex) {
    write_log(seastar::format(
            "[Exec query 9] query id = {}, start vertex = {}\n",
            q9_helper.current_query_id(), start_vertex));
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(q9_helper.current_query_id()));
    auto actor_ref_V = builder.build_ref<i_q9_op_0_V_loopEnter>(0);

    q9_helper.record_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e3_exec_query_9(unsigned query_id) {
    write_log(seastar::format("[Exec query 9] query id = {}\n", query_id));
    auto start_vertex = q9_helper.get_start_person(query_id);
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q9_op_0_V_loopEnter>(0);

    if (query_id >= E3_concurrency) {
        e3_helper.record_big_start_time(query_id);
    }
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e4_exec_query_9(unsigned query_id) {
    write_log(seastar::format("[Exec query 9] query id = {}\n", query_id));
    auto start_vertex = q9_helper.get_start_person(query_id);
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q9_op_0_V_loopEnter>(0);

    actor_ref_V.trigger(StartVertex{start_vertex});
};