#pragma once

#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/db_initializer.hh"
#include "common/query_helpers.hh"
#include "common/schedule_helpers.hh"
#include "cq6_op_0_V_loopEnter.act.h"

inline void exec_cq6(int64_t start_vertex) {
    write_log(seastar::format(
            "[Exec CQ 6] query id = {}, start vertex = {}\n",
            cq6_helper.current_query_id(), start_vertex));
    auto builder = get_query_scope_builder(locate_helper::locate(start_vertex), cq6_helper.current_query_id());
    auto actor_ref_V = builder.build_ref<i_cq6_op_0_V_loopEnter>(0);

    cq6_helper.record_start_time();
    actor_ref_V.trigger(StartVertex{start_vertex});
};

