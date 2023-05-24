#pragma once

#include <brane/actor/actor_message.hh>
#include <brane/core/message_tag.hh>
#include <brane/core/shard-config.hh>
#include <brane/net/entry.hh>
#include <brane/net/network_config.hh>
#include <brane/net/connection.hh>
#include <boost/lockfree/spsc_queue.hpp>

#include "seastar/core/cacheline.hh"
#include "seastar/core/metrics_registration.hh"

namespace seastar {

class reactor;

} // namespace seastar

namespace brane {

class network_message_queue final {
    static constexpr size_t queue_length = 128;
    static constexpr size_t batch_size = 16;
    static constexpr size_t prefetch_cnt = 2;
    struct lf_queue_remote {
        seastar::reactor* remote;
    };
    using lf_queue_base = boost::lockfree::spsc_queue<message_item,
    boost::lockfree::capacity<queue_length>>;
    // use inheritence to control placement order
    struct lf_queue : lf_queue_remote, lf_queue_base {
        lf_queue(seastar::reactor* remote) : lf_queue_remote{remote} {}
        void maybe_wakeup();
    };
    lf_queue _pending;
    lf_queue _completed;
    struct alignas(seastar::cache_line_size) {
        size_t _sent = 0;
        size_t _compl = 0;
        size_t _last_snt_batch = 0;
        size_t _last_cmpl_batch = 0;
        size_t _current_queue_length = 0;
    };
    // keep this between two structures with statistics
    // this makes sure that they have at least one cache line
    // between them, so hw prefetcher will not accidentally prefetch
    // cache line used by another cpu.
    seastar::metrics::metric_groups _metrics;
    struct alignas(seastar::cache_line_size) {
        size_t _received = 0;
        size_t _last_rcv_batch = 0;
    };
    union tx_side {
        tx_side() {}
        ~tx_side() {}
        void init() { new (&a) aa; }
        struct aa {
            std::deque<message_item> pending_fifo;
        } a;
    } _tx;
public:
    network_message_queue(seastar::reactor* from, seastar::reactor* to);
    ~network_message_queue();
    seastar::future<> send(unsigned t, message_item mi, normal_message_tag);
    seastar::future<> send(unsigned t, message_item mi, urgent_message_tag);
    void start(unsigned cpuid);
    size_t process_incoming();
    void stop();
private:
    void move_pending();
    void flush_request_batch();
    bool pure_poll_rx() const;
    friend class network_channel;
};

class network_channel {
    struct dest_info {
        uint32_t machine_id;
        uint32_t target_sid;
        dest_info(uint32_t m, uint32_t s) : machine_id(m), target_sid(s) {}
    };

    struct routing_table {
        routing_table() noexcept;
        ~routing_table();
        void build(std::vector<std::vector<uint32_t>> &sharded_mids);
        dest_info get_destination(uint32_t g_shard_id);
    private:
        void insert(uint32_t mach_id, uint32_t local_sid);
        uint32_t* _sid2mid;
        uint32_t* _cells_head;
        uint32_t  _cell_size;
    };

    struct qs_deleter {
        unsigned count;
        qs_deleter(unsigned n = 0) : count(n) {}
        qs_deleter(const qs_deleter& d) : count(d.count) {}
        void operator()(network_message_queue** qs) const;
    };
    using qs = std::unique_ptr<network_message_queue*[], qs_deleter>;
public:
    template <typename Tag = normal_message_tag>
    static inline
    seastar::future<> send(actor_message* msg) {
        auto dst = _routing_tbl.get_destination(msg->hdr.addr.get_shard_id());
        if (dst.target_sid == local_shard_id()) {
            return push_to_buffer(dst.machine_id, msg);
        }
        // forward to another local shard.
        return _qs[dst.target_sid][local_shard_id()].send(
            dst.target_sid, {dst.machine_id, msg}, Tag{});
    }

private:
    static void create_qs(const std::vector<seastar::reactor*>& reactors);
    static bool poll_queues();
    static bool pure_poll_queues();

    static inline
    seastar::future<> push_to_buffer(uint32_t mach_id, actor_message *msg) {
        auto &buf = _cms[local_shard_id()].o.t._buffers.get(mach_id);
        return buf.push_eventually(std::forward<actor_message*>(msg)
            ).handle_exception([](auto ep) {});
    }

    static qs                          _qs;
    static routing_table               _routing_tbl;
    static entry<connection_manager>*  _cms;
    static unsigned                    count;

    friend class network_io;
    friend class network_message_queue;
    friend class ::seastar::reactor;
    friend class ::seastar::smp;
};

inline
network_channel::routing_table::routing_table() noexcept
    : _sid2mid(nullptr), _cells_head(nullptr), _cell_size(0) {}

inline
network_channel::routing_table::~routing_table() {
    delete[] _sid2mid;
}

inline
void network_channel::routing_table::build(std::vector<std::vector<uint32_t>> &sharded_mids) {
    assert(uint32_t(sharded_mids.size()) == local_shard_count());
    // layout: size, cur_pos, shard id 1, shard id 2, ...
    _cell_size = 2 + local_shard_count();
    auto &nc_inst = network_config::get();
    auto num_g_shards = global_shard_count();
    auto num_machs = nc_inst.num_machines();
    _sid2mid = new uint32_t[num_g_shards + _cell_size * num_machs];
    _cells_head = _sid2mid + num_g_shards;

    // populate (global_shard_id -> machine_id) mapping indices.
    size_t pos = 0;
    for (uint32_t j = 0; j < num_machs; ++j) {
        for (size_t k = 0; k < nc_inst.node_list()[j].nr_cores; ++k) {
            _sid2mid[pos++] = j;
        }
    }

    // init cells.
    for (uint32_t i = 0; i < num_machs; ++i) {
        auto *cell_ptr = _cells_head + _cell_size * i;
        // cell[0]: size, cell[1]: cur_pos
        cell_ptr[0] = 0;
        cell_ptr[1] = 2;
    }

    for (uint32_t i = 0; i < local_shard_count(); ++i) {
        for (auto &mid : sharded_mids[i]) {
            insert(mid, i);
        }
    }
}

inline
network_channel::dest_info network_channel::routing_table::get_destination(uint32_t g_shard_id) {
    network_channel::dest_info di{0, 0};
    di.machine_id = _sid2mid[g_shard_id];
    auto *cell_ptr = _cells_head + _cell_size * di.machine_id;
    // cell[0]: size, cell[1]: cur_pos
    di.target_sid = cell_ptr[cell_ptr[1]];
    // round-robin for avaiable shard.
    cell_ptr[1] = (cell_ptr[1] + 1) % cell_ptr[0] + 2;
    return di;
}

inline
void network_channel::routing_table::insert(uint32_t mach_id, uint32_t local_sid) {
    auto *cell_ptr = _cells_head + _cell_size * mach_id;
    // cell[0]: size, cell[1]: cur_pos
    cell_ptr[cell_ptr[0] + 2] = local_sid;
    ++cell_ptr[0];
}

} // namespace brane

