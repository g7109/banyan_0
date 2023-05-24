#include <brane/actor/actor-system.hh>
#include <brane/core/coordinator.hh>
#include <brane/net/network_io.hh>
#include <brane/net/network_config.hh>
#include <brane/util/machine_info.hh>
#include <sstream>
#include <fstream>

#include "seastar/core/future-util.hh"
#include "seastar/net/byteorder.hh"

namespace brane {

namespace bpo = boost::program_options;

unsigned machine_info::_num_shards = 0;
unsigned machine_info::_sid_anchor = 0;
unsigned machine_info::_min_sid = 0;
unsigned machine_info::_max_sid = 0;

void load_worker_node_list(const std::string& fname,
                           std::vector<worker_node_info> &configs,
                           const uint32_t this_mid) {
    std::ifstream in(fname.c_str());
    std::stringstream ss;
    std::string line, ip;
    uint32_t mach = 0, nr_core = 0;
    // Ignore the first line.
    std::getline(in, line);
    while (in >> mach >> nr_core >> ip) {
#ifdef BRANE_NETWORK_DEBUG
        fmt::print("machine id: {}, #cores: {}, ip addr: {}\n", mach, nr_core, ip);
#endif
        configs.emplace_back(worker_node_info{mach, nr_core,
            seastar::make_ipv4_address(seastar::ipv4_addr(ip))});
    }

    if (this_mid >= configs.size()) {
        throw bpo::error("wrong machine_id (machine_id >= #machines) in `worker-node-list`.");
    }

    if (local_shard_count() != configs[this_mid].nr_cores) {
        throw bpo::error("incorrect number of cores for this machine in `worker-node-list`.");
    }

    for (uint32_t i = 0; i < static_cast<uint32_t>(configs.size()); ++i) {
        if (configs[i].machine_id != i) {
            throw bpo::error("machine ids must be range(0, #machines) in `worker-node-list`.");
        }
    }
}

void actor_system::set_network_config(uint32_t this_mid, uint32_t p2p_conn_count) {
    auto &nc_instance = network_config::get();
    auto &configs = nc_instance._node_list;
    auto this_mach_cores = local_shard_count();
    nc_instance._machine_id = this_mid;
    nc_instance._wait_conn_count = 0;
    nc_instance._p2p_conn_count = p2p_conn_count;
    nc_instance._listen_port = configs[this_mid].addr.port();
    nc_instance._num_machines = uint32_t((configs.size()));
    nc_instance._server_lists.resize(this_mach_cores);
    unsigned num_global_shards = 0;
    unsigned sid_anchor = 0;
    uint32_t dst_shard = 0;

    for (auto &conf : configs) {
        auto conf_mid = conf.machine_id;
        if (this_mid != conf_mid) {
            if (this_mid < conf_mid) {
                // As server.
                nc_instance._wait_conn_count += p2p_conn_count;
                auto ia = seastar::net::ntoh(conf.addr.as_posix_sockaddr_in().sin_addr.s_addr);
                // For pseudo-distributed mode
                if (nc_instance._addr_lsid_map.find(ia) == nc_instance._addr_lsid_map.end()) {
                    nc_instance._addr_lsid_map[ia] = dst_shard;
                }
                dst_shard = (dst_shard + p2p_conn_count) % this_mach_cores;
            } else {
                // Count shard id anchor for this machine.
                sid_anchor += conf.nr_cores;
                // As clients
                for (uint32_t i = 0; i < p2p_conn_count; ++i) {
                    nc_instance._server_lists[dst_shard].push_back(
                        std::make_pair(conf.addr, conf_mid));
                    dst_shard = (dst_shard + 1) % this_mach_cores;
                }
            }
        }
        num_global_shards += conf.nr_cores;
    }
    machine_info::_num_shards = num_global_shards;
    machine_info::_sid_anchor = sid_anchor;

#ifdef BRANE_NETWORK_DEBUG
    fmt::print("nc_instance._wait_conn_count: {}\n", nc_instance._wait_conn_count);
    fmt::print("nc_instance._listen_port: {}\n", nc_instance._listen_port);
    fmt::print("nc_instance._num_machines: {} \n", nc_instance._num_machines);
    fmt::print("nc_instance._machine_id: {} \n", nc_instance._machine_id);

    fmt::print("server list to connect: ");
    for (size_t i = 0; i < nc_instance._server_lists.size(); ++i) {
        fmt::print("({} -> size {}), ", i, nc_instance._server_lists[i].size());
    }
    fmt::print("\n");
#endif
}

seastar::future<> actor_system::configure() {
    auto &configuration = _app.configuration();
    if (configuration.count("worker-node-list")) {
        if (!configuration.count("machine-id")) {
            auto ex = bpo::error(
                "command line argument `--machine-id` must be specified in distributed mode.");
            return seastar::make_exception_future<>(std::move(ex));
        }
        uint32_t machine_id = configuration["machine-id"].as<uint32_t>();
        auto list_file = configuration["worker-node-list"].as<std::string>();
        auto p2p_conn_count = configuration["p2p-connection-count"].as<uint32_t>();
        try {
            load_worker_node_list(list_file,
                network_config::get()._node_list,
                machine_id);
        } catch (...) {
            return seastar::make_exception_future<>(std::current_exception());
        }
        set_network_config(machine_id, p2p_conn_count);
    } else {
        // non-distributed mode
        machine_info::_num_shards = local_shard_count();
        machine_info::_sid_anchor = 0;
    }
    machine_info::_min_sid = machine_info::sid_anchor();
    machine_info::_max_sid = machine_info::sid_anchor() + local_shard_count() - 1;
    coordinator::get().set_sync_size(machine_info::_num_shards);

    return seastar::make_ready_future<>();
}

seastar::future<> actor_system::start() {
    if (network_config::get().num_machines() > 1) {
        return network_io::start_and_connect();
    }
    return seastar::make_ready_future<>();
}

int actor_system::run(int ac, char **av, std::function<void()> &&func) {
    _app.add_options()
        ("machine-id", bpo::value<uint32_t>(), "machine-id")
        ("worker-node-list", bpo::value<std::string>(), "worker node list file.")
        ("p2p-connection-count", bpo::value<uint32_t>()->default_value(1), 
            "connection count between each two workers");

    return _app.run_deprecated(ac, av, [this, fn=std::move(func)] () mutable {
        return configure().then([this] () {
            return start();
        }).then([fn=std::move(fn)] () mutable {
            return seastar::futurize_apply(std::move(fn));
        }).then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (bpo::error &e) {
                std::cerr << "Actor system failed with configuration exception: "
                          << e.what() << std::endl;
                seastar::engine().exit(1);
            } catch (std::runtime_error &e) {
                std::cerr << "Actor system failed with runtime exception: "
                          << e.what() << std::endl;
                seastar::engine().exit(1);
            } catch (...) {
                std::cerr << "Actor system failed with uncaught exception: "
                          << std::current_exception() << std::endl;
                seastar::engine().exit(1);
            }
        });
    });
}

} // namespace brane
