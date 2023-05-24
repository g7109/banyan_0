/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <brane/util/machine_info.hh>
#include <seastar/core/reactor.hh>

namespace brane {

inline unsigned local_shard_id() {
    return seastar::engine().cpu_id();
}

inline unsigned global_shard_id() {
    return machine_info::sid_anchor() + local_shard_id();
}

inline unsigned local_shard_count() {
    return seastar::smp::count;
}

inline unsigned global_shard_count() {
    return machine_info::num_shards();
}
    
} // namespace brane
