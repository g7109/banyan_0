#pragma once

#include <brane/net/defs.hh>
#include <brane/net/entry.hh>
#include <brane/net/connection.hh>
#include <brane/core/coordinator.hh>
#include <brane/net/network_config.hh>
#include <brane/core/shard-config.hh>
#include <atomic>

namespace brane {

class connection_manager;

class network_io {
    using cm = entry<connection_manager>;
    using cms = std::unique_ptr<cm[], entries_deleter<cm>>;
public:
    static seastar::future<> start_and_connect();
private:
    static seastar::future<> start_server();
    static seastar::future<> establish_connections();
    static seastar::future<> stop();
    template <typename E>
    static inline E* construct_entries();

    static inline connection_manager& get_local_cm() {
        return _cms[local_shard_id()].o.t;
    }

    static cms                                  _cms;
    static bool                                 _running;
    static std::atomic<uint32_t>                _ntc;
    static std::vector<seastar::server_socket*> _listeners;
};

template <typename E>
inline
E* network_io::construct_entries() {
    auto *ptr = reinterpret_cast<E*>(operator new[] (sizeof(E) * local_shard_count()));
    for (unsigned i = 0; i < local_shard_count(); ++i) {
        new (&ptr[i]) E();
    }
    return ptr;
}

} // namespace brane
