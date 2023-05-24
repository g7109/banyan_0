/**
 * DAIL in Alibaba Group
 *
 */
#pragma once

#include <chrono>

namespace brane {

using clock_unit = std::chrono::microseconds;

const clock_unit default_actor_quota = clock_unit(500);
const uint32_t default_execution_interval = 10;

extern __thread std::chrono::steady_clock::time_point actor_clock;
extern __thread std::chrono::microseconds actor_quota;
extern __thread uint32_t actor_execution_count;

inline std::chrono::steady_clock::time_point read_actor_clock() {
    return actor_clock;
}

inline void advance_actor_clock() {
    if (++actor_execution_count < default_execution_interval) { return; }
    actor_execution_count = 0;
    actor_clock = std::chrono::steady_clock::now();
}

inline clock_unit read_actor_quota() {
    return actor_quota;
}

inline void set_actor_quota(clock_unit quota) {
    actor_quota = quota;
}

} // namespace brane
