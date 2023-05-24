#pragma once

#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/db_initializer.hh"
#include "common/query_helpers.hh"
#include "q10_op_0_V.act.h"

inline void benchmark_exec_query_10(int64_t start_vertex) {
    write_log(seastar::format(
            "[Exec query 10] query id = {}, start vertex = {}\n",
            q10_helper.current_query_id(), start_vertex));
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(q10_helper.current_query_id()));
    auto actor_ref_V = builder.build_ref<i_q10_op_0_V>(0);

    q10_helper.record_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};

inline void e4_exec_query_10(unsigned query_id) {
    write_log(seastar::format("[Exec query 10] query id = {}\n", query_id));
    auto start_vertex = q10_helper.get_start_person(query_id);
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(query_id));
    auto actor_ref_V = builder.build_ref<i_q10_op_0_V>(0);

    actor_ref_V.trigger(StartVertex{start_vertex});
};