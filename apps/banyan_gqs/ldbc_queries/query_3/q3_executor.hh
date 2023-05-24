#pragma once

#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/db_initializer.hh"
#include "common/query_helpers.hh"
#include "q3_op_0_V_loopEnter.act.h"

inline void benchmark_exec_query_3(int64_t start_vertex) {
    write_log(seastar::format(
            "[Exec query 3] query id = {}, start vertex = {}\n",
            q3_helper.current_query_id(), start_vertex));
    brane::scope_builder builder(locate_helper::locate(start_vertex),
                                 brane::scope<brane::actor_group<> >(q3_helper.current_query_id()));
    auto actor_ref_V = builder.build_ref<i_q3_op_0_V_loopEnter>(0);

    q3_helper.record_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};