/**
 * DAIL in Alibaba Group
 *
 */

#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/haf/actor_message.hh>
#include <seastar/core/print.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/haf/channel.hh>
#include <seastar/haf/actor_implementation.hh>
#include <seastar/haf/util/data_type.hh>
#include <seastar/core/sleep.hh>

using namespace seastar;
using namespace std::chrono_literals;

unsigned MAX_THROUPUT;
unsigned shd_id = 1, usr_id = 2, op_id = 1;

class print_out : public stateless_actor {
    int count_res = 0;
public:
    explicit print_out(execution_context_base *exec_ctx, actor::actor_reference *parent,
        const byte_t *addr, byte_t addr_len) : stateless_actor(exec_ctx, parent, addr, addr_len) {}

    future<stop_reaction> do_work(actor_message* msg) override {
        auto *real_msg = reinterpret_cast<actor_message_with_payload<simple_cstring>*>(msg);
        return do_real_work(std::move(real_msg->data));
    }

    inline future<stop_reaction> do_real_work(simple_cstring&& data) {
        ++count_res;
        fmt::print("[Op Print Out] engine {:d} receive : {:s}\n", engine().cpu_id(), data.c_str);
        if (count_res < 50) {
            return make_ready_future<stop_reaction>(stop_reaction::no);
        } else {
            fmt::print("Child actor: op {:d} stopped reaction after processing message: {:s}\n", get_priority(), data.c_str);
            return make_ready_future<stop_reaction>(stop_reaction::yes);
        }
    }
};

class count : public stateful_actor {
    int count_res = 0;
public:
    explicit count(execution_context_base *exec_ctx, actor::actor_reference *parent,
        const byte_t *addr, byte_t addr_len) : stateful_actor(exec_ctx, parent, addr, addr_len) {}

    future<stop_reaction> do_work(actor_message* msg) override {
        auto *real_msg = reinterpret_cast<actor_message_with_payload<simple_cstring>*>(msg);
        return do_real_work(std::move(real_msg->data));
    }

    inline future<stop_reaction> do_real_work(simple_cstring&& data) {
        ++count_res;
        fmt::print("{:s} count: {:d}\n", data.c_str, count_res);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        if (count_res < 5) {
            return make_ready_future<stop_reaction>(stop_reaction::no);
        } else {
            fmt::print("Child actor: op {:d} stopped reaction after processing message: {:s}\n"
                , get_priority(), data.c_str);
            return make_ready_future<stop_reaction>(stop_reaction::yes);
        }
    }
};

namespace auto_registeration {

using namespace seastar::registration;
actor_registration<print_out> _print_out_op(1);
actor_registration<count> _count_op(2);

} // namespace auto_registeration

actor_message* generate_single_msg(unsigned shd_id, unsigned usr_id, unsigned op_id) {
    address dst_addr{};
    unsigned dst_id[2] = {usr_id, op_id};
    dst_addr.length = sizeof(unsigned) * 2;
    memcpy(dst_addr.data, dst_id, dst_addr.length);

    std::string data{"engine " + std::to_string(shd_id) + " processed message from engine "
        + std::to_string(engine().cpu_id())};
    return make_actor_message(shd_id, message_type::USER, dst_addr,
        simple_cstring{data.c_str(), data.size() + 1});
}

future<> test_poller_single_send_to() {
    MAX_THROUPUT = 1;
    return now().then([] {
        actor::actor_msg_service_group_config ssgc1;
        ssgc1.max_nonlocal_requests = MAX_THROUPUT * (smp::count - 1);
        return actor::create_actor_msg_service_group(ssgc1);
    }).then([] (auto ssg) {
        actor_message* msg = generate_single_msg(shd_id, usr_id, op_id);
        return when_all(make_ready_future<actor::actor_msg_service_group>(ssg),
                        actor::channel::send(msg->hdr.shard_id, ssg, msg));
    }).then([] (auto &&tup) {
        return actor::destroy_actor_msg_service_group(std::get<0>(tup).get0());
    }).then([] {
        fmt::print("test_poller_single_send_to passed.\n");
    });
}

future<> test_poller_multiple_send_to(unsigned op_id) {
    MAX_THROUPUT = 10;
    return now().then([] {
        actor::actor_msg_service_group_config ssgc1;
        ssgc1.max_nonlocal_requests = MAX_THROUPUT * (smp::count - 1);
        return actor::create_actor_msg_service_group(ssgc1);
    }).then([op_id] (auto ssg) {
        return when_all(make_ready_future<actor::actor_msg_service_group>(ssg),
            parallel_for_each(boost::irange(0, static_cast<int>(MAX_THROUPUT)), [&] (int cnt) {
                auto* msg = generate_single_msg(shd_id, usr_id, op_id);
                return actor::channel::send(shd_id, ssg, msg);
        }));
    }).then([] (auto &&tup) {
        return actor::destroy_actor_msg_service_group(std::get<0>(tup).get0());
    }).then([] {
        fmt::print("test_poller_multiple_send_to() passed.");
    });
}

future<> send_peace_stop_message() {
    return parallel_for_each(boost::irange<unsigned>(0, 4), [=] (unsigned id) {
        address dst_addr{};
        dst_addr.length = 0;
        auto *msg = make_actor_message(id, message_type::PEACE_STOP, dst_addr);
        return actor::channel::send(msg);
    });
}

int main(int ac, char** av) {
    app_template app;
    app.run_deprecated(ac, av, [] {
        return now().then([] {
            fmt::print("Message Poller Test start!\n");
        }).then([] {
            fmt::print("----- single message test ------\n");
            return test_poller_single_send_to();
        }).then([] {
            fmt::print("----- multi messages test ------\n");
            return test_poller_multiple_send_to(1);
        }).then([] {
            send_peace_stop_message();
            fmt::print("Message Poller Test Passed!\n");
            engine().exit(0);
        });
    });
    return 0;
}
