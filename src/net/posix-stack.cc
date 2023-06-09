/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <random>
#include <seastar/net/posix-stack.hh>
#include <seastar/net/net.hh>
#include <seastar/net/packet.hh>
#include <seastar/net/api.hh>
#include <seastar/util/std-compat.hh>
#include <netinet/tcp.h>
#include <netinet/sctp.h>

namespace {

// reinterpret_cast<foo*>() on a pointer that the compiler knows points to an
// // object with a different type is disliked by the compiler as it violates
// // strict aliasing rules. This safe version does the same thing but keeps the
// // compiler happy.
template <typename T>
T copy_reinterpret_cast(const void* ptr) {
	T tmp;
	std::memcpy(&tmp, ptr, sizeof(T));
	return tmp;
}

} // namespace

namespace seastar {

namespace net {

using namespace seastar;

template <transport Transport>
class posix_connected_socket_operations;

template <>
class posix_connected_socket_operations<transport::TCP> {
public:
    void set_nodelay(file_desc& _fd, bool nodelay) {
        _fd.setsockopt(IPPROTO_TCP, TCP_NODELAY, int(nodelay));
    }
    bool get_nodelay(file_desc& _fd) const {
        return _fd.getsockopt<int>(IPPROTO_TCP, TCP_NODELAY);
    }
    void set_keepalive(file_desc& _fd, bool keepalive) {
        _fd.setsockopt(SOL_SOCKET, SO_KEEPALIVE, int(keepalive));
    }
    bool get_keepalive(file_desc& _fd) const {
        return _fd.getsockopt<int>(SOL_SOCKET, SO_KEEPALIVE);
    }
    void set_keepalive_parameters(file_desc& _fd, const keepalive_params& params) {
        const tcp_keepalive_params& pms = compat::get<tcp_keepalive_params>(params);
        _fd.setsockopt(IPPROTO_TCP, TCP_KEEPCNT, pms.count);
        _fd.setsockopt(IPPROTO_TCP, TCP_KEEPIDLE, int(pms.idle.count()));
        _fd.setsockopt(IPPROTO_TCP, TCP_KEEPINTVL, int(pms.interval.count()));
    }
    keepalive_params get_keepalive_parameters(file_desc& _fd) const {
        return tcp_keepalive_params {
            std::chrono::seconds(_fd.getsockopt<int>(IPPROTO_TCP, TCP_KEEPIDLE)),
            std::chrono::seconds(_fd.getsockopt<int>(IPPROTO_TCP, TCP_KEEPINTVL)),
            _fd.getsockopt<unsigned>(IPPROTO_TCP, TCP_KEEPCNT)
        };
    }
};

template <>
class posix_connected_socket_operations<transport::SCTP> {
public:
    void set_nodelay(file_desc& _fd, bool nodelay) {
        _fd.setsockopt(SOL_SCTP, SCTP_NODELAY, int(nodelay));
    }
    bool get_nodelay(file_desc& _fd) const {
        return _fd.getsockopt<int>(SOL_SCTP, SCTP_NODELAY);
    }
    void set_keepalive(file_desc& _fd, bool keepalive) {
        auto heartbeat = _fd.getsockopt<sctp_paddrparams>(SOL_SCTP, SCTP_PEER_ADDR_PARAMS);
        if (keepalive) {
            heartbeat.spp_flags |= SPP_HB_ENABLE;
        } else {
            heartbeat.spp_flags &= ~SPP_HB_ENABLE;
        }
        _fd.setsockopt(SOL_SCTP, SCTP_PEER_ADDR_PARAMS, heartbeat);
    }
    bool get_keepalive(file_desc& _fd) const {
        return _fd.getsockopt<sctp_paddrparams>(SOL_SCTP, SCTP_PEER_ADDR_PARAMS).spp_flags & SPP_HB_ENABLE;
    }
    void set_keepalive_parameters(file_desc& _fd, const keepalive_params& kpms) {
        const sctp_keepalive_params& pms = compat::get<sctp_keepalive_params>(kpms);
        auto params = _fd.getsockopt<sctp_paddrparams>(SOL_SCTP, SCTP_PEER_ADDR_PARAMS);
        params.spp_hbinterval = pms.interval.count() * 1000; // in milliseconds
        params.spp_pathmaxrxt = pms.count;
        _fd.setsockopt(SOL_SCTP, SCTP_PEER_ADDR_PARAMS, params);
    }
    keepalive_params get_keepalive_parameters(file_desc& _fd) const {
        auto params = _fd.getsockopt<sctp_paddrparams>(SOL_SCTP, SCTP_PEER_ADDR_PARAMS);
        return sctp_keepalive_params {
            std::chrono::seconds(params.spp_hbinterval/1000), // in seconds
            params.spp_pathmaxrxt
        };
    }
};

template <transport Transport>
class posix_connected_socket_impl final : public connected_socket_impl, posix_connected_socket_operations<Transport> {
    lw_shared_ptr<pollable_fd> _fd;
    using _ops = posix_connected_socket_operations<Transport>;
    conntrack::handle _handle;
    compat::polymorphic_allocator<char>* _allocator;
private:
    explicit posix_connected_socket_impl(lw_shared_ptr<pollable_fd> fd, compat::polymorphic_allocator<char>* allocator=memory::malloc_allocator) :
        _fd(std::move(fd)), _allocator(allocator) {}
    explicit posix_connected_socket_impl(lw_shared_ptr<pollable_fd> fd, conntrack::handle&& handle,
        compat::polymorphic_allocator<char>* allocator=memory::malloc_allocator) : _fd(std::move(fd)), _handle(std::move(handle)), _allocator(allocator) {}
public:
    virtual data_source source() override {
        return data_source(std::make_unique< posix_data_source_impl>(_fd, _allocator));
    }
    virtual data_sink sink() override {
        return data_sink(std::make_unique< posix_data_sink_impl>(_fd));
    }
    virtual void shutdown_input() override {
        _fd->shutdown(SHUT_RD);
    }
    virtual void shutdown_output() override {
        _fd->shutdown(SHUT_WR);
    }
    virtual void set_nodelay(bool nodelay) override {
        return _ops::set_nodelay(_fd->get_file_desc(), nodelay);
    }
    virtual bool get_nodelay() const override {
        return _ops::get_nodelay(_fd->get_file_desc());
    }
    void set_keepalive(bool keepalive) override {
        return _ops::set_keepalive(_fd->get_file_desc(), keepalive);
    }
    bool get_keepalive() const override {
        return _ops::get_keepalive(_fd->get_file_desc());
    }
    void set_keepalive_parameters(const keepalive_params& p) override {
        return _ops::set_keepalive_parameters(_fd->get_file_desc(), p);
    }
    keepalive_params get_keepalive_parameters() const override {
        return _ops::get_keepalive_parameters(_fd->get_file_desc());
    }
    friend class posix_server_socket_impl<Transport>;
    friend class posix_ap_server_socket_impl<Transport>;
    friend class posix_reuseport_server_socket_impl<Transport>;
    friend class posix_network_stack;
    friend class posix_ap_network_stack;
    friend class posix_socket_impl;
};
using posix_connected_tcp_socket_impl = posix_connected_socket_impl<transport::TCP>;
using posix_connected_sctp_socket_impl = posix_connected_socket_impl<transport::SCTP>;

class posix_socket_impl final : public socket_impl {
    lw_shared_ptr<pollable_fd> _fd;
    compat::polymorphic_allocator<char>* _allocator;

    future<> find_port_and_connect(socket_address sa, socket_address local, transport proto = transport::TCP) {
        static thread_local std::default_random_engine random_engine{std::random_device{}()};
        static thread_local std::uniform_int_distribution<uint16_t> u(49152/smp::count + 1, 65535/smp::count - 1);
        return repeat([this, sa, local, proto, attempts = 0, requested_port = ntoh(local.as_posix_sockaddr_in().sin_port)] () mutable {
            uint16_t port = attempts++ < 5 && requested_port == 0 && proto == transport::TCP ? u(random_engine) * smp::count + engine().cpu_id() : requested_port;
            local.as_posix_sockaddr_in().sin_port = hton(port);
            return futurize_apply([this, sa, local] { return engine().posix_connect(_fd, sa, local); }).then_wrapped([] (future<> f) {
                try {
                    f.get();
                    return stop_iteration::yes;
                } catch (std::system_error& err) {
                    if (err.code().value() == EADDRINUSE) {
                        return stop_iteration::no;
                    }
                    throw;
                }
            });
        });
    }

public:
    explicit posix_socket_impl(compat::polymorphic_allocator<char>* allocator=memory::malloc_allocator) : _allocator(allocator) {}

    virtual future<connected_socket> connect(socket_address sa, socket_address local, transport proto = transport::TCP) override {
        _fd = engine().make_pollable_fd(sa, proto);
        return find_port_and_connect(sa, local, proto).then([fd = _fd, proto, allocator = _allocator] () mutable {
            std::unique_ptr<connected_socket_impl> csi;
            if (proto == transport::TCP) {
                csi.reset(new posix_connected_tcp_socket_impl(std::move(fd), allocator));
            } else {
                csi.reset(new posix_connected_sctp_socket_impl(std::move(fd), allocator));
            }
            return make_ready_future<connected_socket>(connected_socket(std::move(csi)));
        });
    }

    virtual void shutdown() override {
        if (_fd) {
            try {
                _fd->shutdown(SHUT_RDWR);
            } catch (std::system_error& e) {
                if (e.code().value() != ENOTCONN) {
                    throw;
                }
            }
        }
    }
};

template <transport Transport>
future<connected_socket, socket_address>
posix_server_socket_impl<Transport>::accept() {
    return _lfd.accept().then([this] (pollable_fd fd, socket_address sa) {
        auto cth = _lba == server_socket::load_balancing_algorithm::connection_distribution ?
            _conntrack.get_handle() : (_lba == server_socket::load_balancing_algorithm::port ?
            _conntrack.get_handle(ntoh(sa.as_posix_sockaddr_in().sin_port) % smp::count) :
            _conntrack.get_handle(sa));
        auto cpu = cth.cpu();
        if (cpu == engine().cpu_id()) {
            std::unique_ptr<connected_socket_impl> csi(
                    new posix_connected_socket_impl<Transport>(make_lw_shared(std::move(fd)), std::move(cth), _allocator));
            return make_ready_future<connected_socket, socket_address>(
                    connected_socket(std::move(csi)), sa);
        } else {
            smp::submit_to(cpu, [ssa = _sa, fd = std::move(fd.get_file_desc()), sa, cth = std::move(cth), allocator = _allocator] () mutable {
                posix_ap_server_socket_impl<Transport>::move_connected_socket(ssa, pollable_fd(std::move(fd)), sa, std::move(cth), allocator);
            });
            return accept();
        }
    });
}

template <transport Transport>
void
posix_server_socket_impl<Transport>::abort_accept() {
    _lfd.abort_reader();
}

template <transport Transport>
socket_address posix_server_socket_impl<Transport>::local_address() const {
    return _lfd.get_file_desc().get_address();
}

template <transport Transport>
future<connected_socket, socket_address> posix_ap_server_socket_impl<Transport>::accept() {
    auto conni = conn_q.find(_sa);
    if (conni != conn_q.end()) {
        connection c = std::move(conni->second);
        conn_q.erase(conni);
        try {
            std::unique_ptr<connected_socket_impl> csi(
                    new posix_connected_socket_impl<Transport>(make_lw_shared(std::move(c.fd)), std::move(c.connection_tracking_handle)));
            return make_ready_future<connected_socket, socket_address>(connected_socket(std::move(csi)), std::move(c.addr));
        } catch (...) {
            return make_exception_future<connected_socket, socket_address>(std::current_exception());
        }
    } else {
        try {
            auto i = sockets.emplace(std::piecewise_construct, std::make_tuple(_sa), std::make_tuple());
            assert(i.second);
            return i.first->second.get_future();
        } catch (...) {
            return make_exception_future<connected_socket, socket_address>(std::current_exception());
        }
    }
}

template <transport Transport>
void
posix_ap_server_socket_impl<Transport>::abort_accept() {
    conn_q.erase(_sa);
    auto i = sockets.find(_sa.as_posix_sockaddr_in());
    if (i != sockets.end()) {
        i->second.set_exception(std::system_error(ECONNABORTED, std::system_category()));
        sockets.erase(i);
    }
}

template <transport Transport>
future<connected_socket, socket_address>
posix_reuseport_server_socket_impl<Transport>::accept() {
    return _lfd.accept().then([allocator = _allocator] (pollable_fd fd, socket_address sa) {
        std::unique_ptr<connected_socket_impl> csi(
                new posix_connected_socket_impl<Transport>(make_lw_shared(std::move(fd)), allocator));
        return make_ready_future<connected_socket, socket_address>(
            connected_socket(std::move(csi)), sa);
    });
}

template <transport Transport>
void
posix_reuseport_server_socket_impl<Transport>::abort_accept() {
    _lfd.abort_reader();
}

template <transport Transport>
socket_address posix_reuseport_server_socket_impl<Transport>::local_address() const {
    return _lfd.get_file_desc().get_address();
}

template <transport Transport>
void
posix_ap_server_socket_impl<Transport>::move_connected_socket(socket_address sa, pollable_fd fd, socket_address addr, conntrack::handle cth, compat::polymorphic_allocator<char>* allocator) {
    auto i = sockets.find(sa);
    if (i != sockets.end()) {
        try {
            std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl<Transport>(make_lw_shared(std::move(fd)), std::move(cth), allocator));
            i->second.set_value(connected_socket(std::move(csi)), std::move(addr));
        } catch (...) {
            i->second.set_exception(std::current_exception());
        }
        sockets.erase(i);
    } else {
        conn_q.emplace(std::piecewise_construct, std::make_tuple(sa), std::make_tuple(std::move(fd), std::move(addr), std::move(cth)));
    }
}

future<temporary_buffer<char>>
posix_data_source_impl::get() {
    return _fd->read_some(_buf.get_write(), _buf_size).then([this] (size_t size) {
        _buf.trim(size);
        auto ret = std::move(_buf);
        _buf = make_temporary_buffer<char>(_buffer_allocator, _buf_size);
        return make_ready_future<temporary_buffer<char>>(std::move(ret));
    });
}

future<> posix_data_source_impl::close() {
    _fd->shutdown(SHUT_RD);
    return make_ready_future<>();
}

std::vector<struct iovec> to_iovec(const packet& p) {
    std::vector<struct iovec> v;
    v.reserve(p.nr_frags());
    for (auto&& f : p.fragments()) {
        v.push_back({.iov_base = f.base, .iov_len = f.size});
    }
    return v;
}

std::vector<iovec> to_iovec(std::vector<temporary_buffer<char>>& buf_vec) {
    std::vector<iovec> v;
    v.reserve(buf_vec.size());
    for (auto& buf : buf_vec) {
        v.push_back({.iov_base = buf.get_write(), .iov_len = buf.size()});
    }
    return v;
}

future<>
posix_data_sink_impl::put(temporary_buffer<char> buf) {
    return _fd->write_all(buf.get(), buf.size()).then([d = buf.release()] {});
}

future<>
posix_data_sink_impl::put(packet p) {
    _p = std::move(p);
    return _fd->write_all(_p).then([this] { _p.reset(); });
}

future<>
posix_data_sink_impl::close() {
    _fd->shutdown(SHUT_WR);
    return make_ready_future<>();
}

server_socket
posix_network_stack::listen(socket_address sa, listen_options opt) {
    if (opt.proto == transport::TCP) {
        return _reuseport ?
            server_socket(std::make_unique<posix_reuseport_server_tcp_socket_impl>(sa, engine().posix_listen(sa, opt), _allocator))
            :
            server_socket(std::make_unique<posix_server_tcp_socket_impl>(sa, engine().posix_listen(sa, opt), opt.lba, _allocator));
    } else {
        return _reuseport ?
            server_socket(std::make_unique<posix_reuseport_server_sctp_socket_impl>(sa, engine().posix_listen(sa, opt), _allocator))
            :
            server_socket(std::make_unique<posix_server_sctp_socket_impl>(sa, engine().posix_listen(sa, opt), opt.lba, _allocator));
    }
}

::seastar::socket posix_network_stack::socket() {
    return ::seastar::socket(std::make_unique<posix_socket_impl>(_allocator));
}

template<transport Transport>
thread_local std::unordered_map<socket_address, promise<connected_socket, socket_address>> posix_ap_server_socket_impl<Transport>::sockets;
template<transport Transport>
thread_local std::unordered_multimap<socket_address, typename posix_ap_server_socket_impl<Transport>::connection> posix_ap_server_socket_impl<Transport>::conn_q;

server_socket
posix_ap_network_stack::listen(socket_address sa, listen_options opt) {
    if (opt.proto == transport::TCP) {
        return _reuseport ?
            server_socket(std::make_unique<posix_reuseport_server_tcp_socket_impl>(sa, engine().posix_listen(sa, opt)))
            :
            server_socket(std::make_unique<posix_tcp_ap_server_socket_impl>(sa));
    } else {
        return _reuseport ?
            server_socket(std::make_unique<posix_reuseport_server_sctp_socket_impl>(sa, engine().posix_listen(sa, opt)))
            :
            server_socket(std::make_unique<posix_sctp_ap_server_socket_impl>(sa));
    }
}

struct cmsg_with_pktinfo {
    struct cmsghdrcmh;
    union {
        struct in_pktinfo pktinfo;
        struct in6_pktinfo pkt6info;
    };
};

class posix_udp_channel : public udp_channel_impl {
private:
    static constexpr int MAX_DATAGRAM_SIZE = 65507;
    struct recv_ctx {
        struct msghdr _hdr;
        struct iovec _iov;
        socket_address _src_addr;
        char* _buffer;
        cmsg_with_pktinfo _cmsg;

        recv_ctx() {
            memset(&_hdr, 0, sizeof(_hdr));
            _hdr.msg_iov = &_iov;
            _hdr.msg_iovlen = 1;
            _hdr.msg_name = &_src_addr.u.sa;
            _hdr.msg_namelen = sizeof(_src_addr.u.sas);
            memset(&_cmsg, 0, sizeof(_cmsg));
            _hdr.msg_control = &_cmsg;
            _hdr.msg_controllen = sizeof(_cmsg);
        }

        void prepare() {
            _buffer = new char[MAX_DATAGRAM_SIZE];
            _iov.iov_base = _buffer;
            _iov.iov_len = MAX_DATAGRAM_SIZE;
        }
    };
    struct send_ctx {
        struct msghdr _hdr;
        std::vector<struct iovec> _iovecs;
        socket_address _dst;
        packet _p;

        send_ctx() {
            memset(&_hdr, 0, sizeof(_hdr));
            _hdr.msg_name = &_dst.u.sa;
            _hdr.msg_namelen = sizeof(_dst.u.sas);
        }

        void prepare(const socket_address& dst, packet p) {
            _dst = dst;
            _p = std::move(p);
            _iovecs = to_iovec(_p);
            _hdr.msg_iov = _iovecs.data();
            _hdr.msg_iovlen = _iovecs.size();
        }
    };
    std::unique_ptr<pollable_fd> _fd;
    socket_address _address;
    recv_ctx _recv;
    send_ctx _send;
    bool _closed;
public:
    posix_udp_channel(const socket_address& bind_address)
            : _closed(false) {
        auto sa = bind_address;
        file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        fd.setsockopt(SOL_IP, IP_PKTINFO, true);
        if (engine().posix_reuseport_available()) {
            fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);
        }
        fd.bind(sa.u.sa, sizeof(sa.u.sas));
        _address = fd.get_address();
        _fd = std::make_unique<pollable_fd>(std::move(fd));
    }
    virtual ~posix_udp_channel() { if (!_closed) close(); };
    virtual future<udp_datagram> receive() override;
    virtual future<> send(const socket_address& dst, const char *msg) override;
    virtual future<> send(const socket_address& dst, packet p) override;
    virtual void shutdown_input() override {
        _fd->abort_reader();
    }
    virtual void shutdown_output() override {
        _fd->abort_writer();
    }
    virtual void close() override {
        _closed = true;
        _fd.reset();
    }
    virtual bool is_closed() const override { return _closed; }
    socket_address local_address() const override {
        return _address;
    }
};

future<> posix_udp_channel::send(const socket_address& dst, const char *message) {
    auto len = strlen(message);
    return _fd->sendto(dst, message, len)
            .then([len] (size_t size) { assert(size == len); });
}

future<> posix_udp_channel::send(const socket_address& dst, packet p) {
    auto len = p.len();
    _send.prepare(dst, std::move(p));
    return _fd->sendmsg(&_send._hdr)
            .then([len] (size_t size) { assert(size == len); });
}

udp_channel
posix_network_stack::make_udp_channel(const socket_address& addr) {
    return udp_channel(std::make_unique<posix_udp_channel>(addr));
}

bool
posix_network_stack::supports_ipv6() const {
    static bool has_ipv6 = [] {
        try {
            posix_udp_channel c(ipv6_addr{"::1"});
            c.close();
            return true;
        } catch (...) {}
        return false;
    }();

    return has_ipv6;
}

class posix_datagram : public udp_datagram_impl {
private:
    socket_address _src;
    socket_address _dst;
    packet _p;
public:
    posix_datagram(const socket_address& src, const socket_address& dst, packet p) : _src(src), _dst(dst), _p(std::move(p)) {}
    virtual socket_address get_src() override { return _src; }
    virtual socket_address get_dst() override { return _dst; }
    virtual uint16_t get_dst_port() override { return _dst.port(); }
    virtual packet& get_data() override { return _p; }
};

future<udp_datagram>
posix_udp_channel::receive() {
    _recv.prepare();
    return _fd->recvmsg(&_recv._hdr).then([this] (size_t size) {
        socket_address dst;
        for (auto* cmsg = CMSG_FIRSTHDR(&_recv._hdr); cmsg != nullptr; cmsg = CMSG_NXTHDR(&_recv._hdr, cmsg)) {
            if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_PKTINFO) {
                dst = ipv4_addr(copy_reinterpret_cast<in_pktinfo>(CMSG_DATA(cmsg)).ipi_addr, _address.port());
				break;
            } else if (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_PKTINFO) {
                dst = ipv6_addr(copy_reinterpret_cast<in6_pktinfo>(CMSG_DATA(cmsg)).ipi6_addr, _address.port());
				break;
            }
        }
        return make_ready_future<udp_datagram>(udp_datagram(std::make_unique<posix_datagram>(
            _recv._src_addr, dst, packet(fragment{_recv._buffer, size}, make_deleter([buf = _recv._buffer] { delete[] buf; })))));
    }).handle_exception([p = _recv._buffer](auto ep) {
        delete[] p;
        return make_exception_future<udp_datagram>(std::move(ep));
    });
}

void register_posix_stack() {
    register_network_stack("posix", boost::program_options::options_description(),
        [](boost::program_options::variables_map ops) {
            return smp::main_thread() ? posix_network_stack::create(ops)
                                      : posix_ap_network_stack::create(ops);
        },
        true);
}
}

}
