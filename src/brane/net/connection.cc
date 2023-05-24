#include <brane/net/connection.hh>
#include <brane/net/network_io.hh>
#include <brane/net/network_config.hh>
#include <brane/util/machine_info.hh>
#include <brane/core/message-passing.hh>
#include <brane/core/root_actor_group.hh>

#include "seastar/core/reactor.hh"

namespace brane {

using tmp_buffer_t = seastar::temporary_buffer<char>;

struct network_delegater {
    static inline
    actor_message* deserialize(const tmp_buffer_t &buf) {
        // header | num_payload | ... | ... | ...
        static constexpr uint32_t offset = sizeof(actor_message::header) + 4;
        auto *ptr = buf.get();
        auto &am_hdr = *reinterpret_cast<const actor_message::header*>(ptr);
        auto nr_payload = *reinterpret_cast<const uint32_t*>(ptr + sizeof(actor_message::header));
        if (nr_payload == 0) {
            return new actor_message_without_payload{
                am_hdr.addr, am_hdr.behavior_tid,
                am_hdr.src_shard_id, am_hdr.pr_id,
                am_hdr.m_type, actor_message::network_flag{},
                };
        } else {
            serializable_unit su;
            su.num_used = nr_payload;
            ptr += offset;
            for (uint32_t i = 0; i < nr_payload; ++i) {
                auto len = *reinterpret_cast<const uint32_t*>(ptr);
                su.bufs[i] = tmp_buffer_t{ptr + 4, len};
                ptr += len + 4;
            }

            return new actor_message_with_payload<serializable_unit>{
                am_hdr.addr, am_hdr.behavior_tid, std::move(su),
                am_hdr.src_shard_id, am_hdr.pr_id, am_hdr.m_type,};
        }
    }
};

void connection_manager::buffers_helper::abort(const std::exception_ptr &ex,
                                               std::vector<uint32_t> &&mids) {
    for (auto &id : mids) {
        // delete all messages to avoid memory leak.
        _qs[id].consume([] (actor_message *msg) { delete msg; return true; });
        _qs[id].abort(ex);
    }
}

void connection_manager::buffers_helper::build() {
    _nr_bufs = network_config::get().num_machines();
    _qs = reinterpret_cast<net_buffer*>(
        operator new[] (sizeof(net_buffer) * _nr_bufs));
    for (uint32_t i = 0; i < _nr_bufs; ++i) {
        new (&_qs[i]) net_buffer(_qu_size);
    }
}

seastar::future<> connection_manager::server_side::start(connection_table &ct) {
    return optional_listen().then([this, &ct] {
        return collect_machine_ids(ct);
    });
}

seastar::future<> connection_manager::server_side::optional_listen() {
    if (init_ntc == 0) { return seastar::make_ready_future<>(); }
    return seastar::repeat([this] {
        return this_listener->accept().then_wrapped(
                [this] (seastar::future<seastar::connected_socket, seastar::socket_address> &&fut) {
            try {
                auto tup = fut.get();
            #ifdef BRANE_NETWORK_DEBUG
                fmt::print("[ Accept ] connection from peer {} on engine {}.\n",
                    std::get<1>(tup), local_shard_id());
            #endif
                _holder.emplace_back(0, connection{std::move(std::get<0>(tup))});
                // Ensure atomic operation.
                auto cur_total_remains = num_total_conns.fetch_sub(1) - 1;
                if (cur_total_remains == 0) {
                    return seastar::parallel_for_each(
                            boost::irange(0u, local_shard_count()), [this] (unsigned c) {
                        if (local_shard_id() != c) {
                            return seastar::smp::submit_to(c, [lsn = listeners[c]] {
                                lsn->abort_accept();
                            });
                        } else {
                            return seastar::make_ready_future<>();
                        }
                    }).then_wrapped([] (auto &&f) {
                        try { f.get(); } catch (...) {};
                        return seastar::make_ready_future<
                            seastar::stop_iteration>(seastar::stop_iteration::yes);
                    });
                }
                return seastar::make_ready_future<
                    seastar::stop_iteration>(seastar::stop_iteration::no);
            } catch (...) {
            #ifdef BRANE_NETWORK_DEBUG
                fmt::print("Abort accept on engine {}\n", local_shard_id());
            #endif
                return seastar::make_ready_future<
                    seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
        });
    });
}

seastar::future<> connection_manager::server_side::collect_machine_ids(connection_table &conn_table) {
    return seastar::parallel_for_each(boost::irange(size_t(0), _holder.size()), [this] (size_t pos) {
        auto &in = _holder[pos].second.in;
        return in.read_exactly(4).then([this, pos] (tmp_buffer_t buf) mutable {
            auto peer_mid = *reinterpret_cast<const uint32_t*>(buf.get());
            // assign to real peer machine id
            _holder[pos].first = peer_mid;
        #ifdef BRANE_NETWORK_DEBUG
            fmt::print("[ Collect MID ] from peer {} on engine {}\n", peer_mid, local_shard_id());
        #endif
        });
    }).then([this, &conn_table] () mutable {
        for (auto &&mc_pair : _holder) {
            conn_table.add(std::move(mc_pair));
        }
        // reclaim _holder heap memory.
        auto tmp_vec = std::move(_holder);
    });
}

seastar::future<> connection_manager::clients_side::start(connection_table &conn_table) {
    auto &server_list = network_config::get().get_server_list(local_shard_id());
    auto num_machs = static_cast<uint32_t>(server_list.size());
    return seastar::parallel_for_each(boost::irange(0u, num_machs),
            [&conn_table, &server_list] (unsigned id) mutable {
        return connect(server_list[id].first).then([id, &conn_table] (seastar::connected_socket socket) {
            auto &sv_list = network_config::get().get_server_list(local_shard_id());
            uint32_t target_mid = sv_list[id].second;
            conn_table.add(std::make_pair(target_mid, connection{std::move(socket)}));
        #ifdef BRANE_NETWORK_DEBUG
            fmt::print("[ Connected ] to server {}-{} on engine {}\n",
                sv_list[id].first, target_mid, local_shard_id());
        #endif
            auto &out = conn_table.find(target_mid).out;
            auto this_mid = network_config::get().machine_id();
            return out.write(reinterpret_cast<const char*>(&this_mid), 4).then([&out] {
                return out.flush();
            });
        });
    });
}

seastar::future<> connection_manager::run_loops(std::vector<uint32_t> &managed_mids) {
    managed_mids = _conn_table.managed_mach_ids();
    run_senders();
    run_receivers();
    return seastar::now();
}

seastar::future<> connection_manager::close_out_streams() {
    return seastar::parallel_for_each(
            _conn_table.mc_map() | boost::adaptors::map_values, [] (connection &conn) {
        return conn.out.close();
    });
}

void connection_manager::run_senders() {
    _sender_stopped = seastar::parallel_for_each(
            _conn_table.managed_mach_ids(), [this] (unsigned mid) {
        auto &conn = _conn_table.find(mid);
        auto &buf = _buffers._qs[mid];
    #ifdef BRANE_NETWORK_DEBUG
        fmt::print("connection_manager::run_senders target mid {} on engine {}\n", 
            mid, local_shard_id());
    #endif
        return seastar::repeat([&conn, &buf] () {
            return buf.pop_eventually().then_wrapped([&conn] (seastar::future<actor_message*> &&f) {
                if (__builtin_expect(f.failed(), false)) {
                    // there is only one type of exception for buffer
                    f.ignore_ready_future();
                    return seastar::make_ready_future<
                        seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                auto msg = f.get0();
                return msg->serialize(conn.out).then([msg, &conn] {
                    delete msg;
                    return conn.out.flush();
                }).then([] {
                    return seastar::stop_iteration::no;
                });
            });
        });
    }).finally([this] {
        return close_out_streams();
    });
}

void connection_manager::run_receivers() {
    _receiver_stopped = seastar::parallel_for_each(
            _conn_table.mc_map() | boost::adaptors::map_values, [this] (connection &conn) {
        return seastar::do_until([&conn, this] { return conn.in.eof() || _abort_connection; }, [&conn, this] {
            return conn.in.read_exactly(4).then([&conn, this] (tmp_buffer_t buf) {
                if (__builtin_expect(conn.in.eof() || _abort_connection, false)) {
                    return seastar::now();
                }
                auto nr_bytes = *as_u32_ptr(buf.get_write());
                return conn.in.read_exactly(nr_bytes).then([] (tmp_buffer_t &&buf) {
                    auto *msg = network_delegater::deserialize(buf);
                #ifdef BRANE_NETWORK_DEBUG
                    fmt::print("[ Receive Header ] shard_id: {}, m_type: {}\n",
                        msg->hdr.shard_id, msg->hdr.m_type);
                #endif
                    return actor_engine().send(msg);
                });
            });
        }).finally([&conn] {
            return conn.in.close();
        });
    });
}

seastar::future<> connection_manager::stop() {
    _abort_connection = true;
    _buffers.abort(std::make_exception_ptr(std::runtime_error("connection is closed")),
        _conn_table.managed_mach_ids());
    return _sender_stopped.handle_exception([] (auto ex) {}).finally([this] {
    #ifdef BRANE_NETWORK_DEBUG
        fmt::print("[ All Senders ] stopped by abort signal on shard {}\n", local_shard_id());
    #endif
        return _receiver_stopped.handle_exception([] (auto ex) {}).finally([] {
    #ifdef BRANE_NETWORK_DEBUG
            fmt::print("[ Receiver ] stopped by abort signal on shard {}\n", local_shard_id());
    #endif
        });
    });
}

} // namespace brane
