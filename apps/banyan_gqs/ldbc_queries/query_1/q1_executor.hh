#pragma once

#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/db_initializer.hh"
#include "common/query_helpers.hh"
#include "q1_op_0_V_loopEnter.act.h"

inline void benchmark_exec_query_1(int64_t start_vertex) {
    write_log(seastar::format(
            "[Exec query 1] query id = {}, start vertex = {}\n",
            q1_helper.current_query_id(), start_vertex));
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(q1_helper.current_query_id()));
    auto actor_ref_V = builder.build_ref<i_q1_op_0_V_loopEnter>(0);

    q1_helper.record_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e3_exec_query_1(unsigned query_id) {
    write_log(seastar::format("[Exec query 1] query id = {}\n", query_id));
    auto start_vertex = q1_helper.get_start_person(query_id);
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q1_op_0_V_loopEnter>(0);

    if (query_id >= E3_concurrency) {
        e3_helper.record_small_start_time(query_id);
    }
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e4_exec_query_1(unsigned query_id) {
    write_log(seastar::format("[Exec query 1] query id = {}\n", query_id));
    auto start_vertex = q1_helper.get_start_person(query_id);
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q1_op_0_V_loopEnter>(0);

    e4_helper.record_fg_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};
