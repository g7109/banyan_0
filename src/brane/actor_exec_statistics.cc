/**
 * DAIL in Alibaba Group
 *
 */

#include <brane/actor/actor_exec_statistics.hh>

namespace brane {

__thread std::chrono::steady_clock::time_point actor_clock;
__thread std::chrono::microseconds actor_quota = default_actor_quota;
__thread uint32_t actor_execution_count = 0;

} // namespace brane
