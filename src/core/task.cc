/**
 * DAIL in Alibaba Group
 *
 */

#include <seastar/core/task.hh>
#include <seastar/core/reactor.hh>

namespace seastar {

task::task(seastar::scheduling_group sg) : _sg(sg), _exec_ctx(nullptr) {}

task::task(brane::actor_base* exec_ctx) : _sg(current_scheduling_group()), _exec_ctx(exec_ctx) {}

} // namespace seastar
