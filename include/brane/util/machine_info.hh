#pragma once

#include <cstdint>

namespace brane {

struct machine_info {
    /// global shard id anchor
    static inline uint32_t sid_anchor() {
        return _sid_anchor;
    }

    /// Number of shards in the cluster.
    static inline uint32_t num_shards() {
        return _num_shards;
    }

    static inline bool is_local(const uint32_t shard_id) {
        return shard_id >= _min_sid && shard_id <= _max_sid;
    }

private:
    static unsigned _num_shards;
    static unsigned _sid_anchor;
    static unsigned _min_sid;
    static unsigned _max_sid;

    friend class actor_system;
};

} // namespace brane
