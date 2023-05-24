#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/haf/net/network_io.hh>
#include <seastar/core/sleep.hh>
#include <seastar/haf/actor_implementation.hh>
#include <seastar/haf/query/column_batch.hh>

using namespace std::chrono_literals;
using namespace seastar;

future<> send_peace_stop_message() {
    return parallel_for_each(boost::irange<unsigned>(0, 4), [] (unsigned id) {
        address dst_addr{};
        dst_addr.length = 0;
        auto *msg = make_actor_message(id, message_type::PEACE_STOP, dst_addr);
        return actor::channel::send(msg);
    });
}

class cb_sender : public stateless_actor {
public:
    explicit cb_sender(execution_context_base *exec_ctx, actor::actor_reference *parent, const byte_t *addr, byte_t addr_len)
        : stateless_actor(exec_ctx, parent, addr, addr_len) {}
    ~cb_sender() override = default;

    future<stop_reaction> do_work(actor_message* msg) override {
        fmt::print("[ Do work ] shard id: {}, message type: {}, from_network: {} on shard {}\n"
            , msg->hdr.shard_id, msg->hdr.m_type, msg->hdr.from_network, engine().cpu_id());
        return do_real_work();
    }

    inline future<stop_reaction> do_real_work() {
        cb::column_batch<int64_t> data{10};
        for (int i = 0; i < 6; ++i) {
            data.push_back(5102 + i);
        }

        address dst_addr{};
        unsigned dst_id[2] = {0, 2};
        dst_addr.length = sizeof(unsigned) * 2;
        memcpy(dst_addr.data, dst_id, dst_addr.length);

        auto *msg = make_actor_message(0, message_type::USER, dst_addr, std::move(data));
        return network_io::send_to(msg).then([] {
            return send_peace_stop_message();
        }).then([] {
            return make_ready_future<stop_reaction>(stop_reaction::yes);
        });
    }
};

class cb_receiver : public stateless_actor {
    using data_type = cb::column_batch<int64_t>;
public:
    explicit cb_receiver(execution_context_base *exec_ctx, actor::actor_reference *parent, const byte_t *addr, byte_t addr_len)
        : stateless_actor(exec_ctx, parent, addr, addr_len) {}
    ~cb_receiver() override = default;

    future<stop_reaction> do_work(actor_message* msg) override {
        if (msg->hdr.from_network) {
            using raw_msg_t = actor_message_with_payload<serializable_unit>;
            auto *raw_msg = reinterpret_cast<raw_msg_t*>(msg);
            return do_real_work(data_type{std::move(raw_msg->data.bufs[0])});
        } else {
            auto *real_msg = reinterpret_cast<actor_message_with_payload<data_type>*>(msg);
            return do_real_work(std::move(real_msg->data));
        }
    }

    inline future<stop_reaction> do_real_work(cb::column_batch<int64_t> &&data) {
        for (size_t i = 0; i < data.size(); ++i) {
            fmt::print("{} ", data[i]);
        }
        fmt::print("\n");
        return send_peace_stop_message().then([] {
            return make_ready_future<stop_reaction>(stop_reaction::yes);
        });
    }
};

namespace auto_registeration {

seastar::registration::actor_registration<cb_sender> _cb_sender(1);
seastar::registration::actor_registration<cb_receiver> _cb_receiver(2);

} // namespace auto_registeration

int main(int ac, char** av) {
    app_template app;
    namespace bpo = boost::program_options;

    app.add_options()
        ("machine-id", bpo::value<uint32_t>()->default_value(0), "machine-id")
        ("nr-shards", bpo::value<uint32_t>()->default_value(1), "nr-shards")
        ("net-config", bpo::value<std::string>()->default_value("net.config"), "net-config");

    app.run_deprecated(ac, av, [&] {
        auto& config = app.configuration();
        auto machine_id = config["machine-id"].as<uint32_t>();
        auto net_config = config["net-config"].as<std::string>();

        return network_io::start(net_config, machine_id).then([machine_id] {
            if (machine_id == 0) {
                address dst_addr{};
                unsigned dst_id[2] = {0, 1};
                dst_addr.length = sizeof(unsigned) * 2;
                memcpy(dst_addr.data, dst_id, dst_addr.length);
                auto *msg = make_actor_message(5, message_type::USER, dst_addr);
                return network_io::send_to(msg);
            } else {
                return now();
            }
        });
    });
    return 0;
}

