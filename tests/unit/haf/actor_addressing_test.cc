/**
 * DAIL in Alibaba Group
 *
 */

#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/haf/actor_message.hh>
#include <seastar/haf/util/data_type.hh>
#include <seastar/core/print.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/haf/channel.hh>
#include <seastar/haf/actor_implementation.hh>
#include <seastar/haf/actor_factory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace seastar;
using namespace std::chrono_literals;

unsigned MAX_THROUPUT;
unsigned shd_id = 1, usr_id = 2, op_id = 1, dst_id_len = 8;

class count : public stateful_actor {
    int count_res = 0;
public:
    explicit count(execution_context_base *exec_ctx, actor::actor_reference *parent, const byte_t *addr, byte_t addr_len)
        : stateful_actor(exec_ctx, parent, addr, addr_len) {}

    ~count() override {
        fmt::print("[Destructor] Stateful Count operator in shard {}\n", engine().cpu_id());
    }

    future<stop_reaction> do_work(actor_message* msg) override {
        auto *real_msg = reinterpret_cast<actor_message_with_payload<simple_cstring>*>(msg);
        return do_real_work(std::move(real_msg->data));
    }

    inline future<stop_reaction> do_real_work(simple_cstring&& data) {
        ++count_res;
        fmt::print("Engine {} : {} with current count res {:d}\n", engine().cpu_id(), data.c_str, count_res);
        if(count_res == 50)
            return make_ready_future<stop_reaction>(stop_reaction::yes);
        return make_ready_future<stop_reaction>(stop_reaction::no);
    }
};

namespace auto_registeration {

seastar::registration::actor_registration<count> _count_op(1);

} // namespace auto_registeration

future<> single_message_test() {
    MAX_THROUPUT = 1;
    return now().then([] {
        fmt::print("Sending message...\n");
        address dst_addr{};
        unsigned dst_id[2] = {usr_id, op_id};
        dst_addr.length = sizeof(unsigned) * 2;
        memcpy(dst_addr.data, dst_id, dst_addr.length);

        std::string data{"processed message from engine " + std::to_string(engine().cpu_id()) + "."};
        auto *msg = make_actor_message(shd_id, message_type::USER, dst_addr,
            simple_cstring{data.c_str(), data.size() + 1});
        return actor::channel::send(msg);
    }).then([] {
        fmt::print("Single message test passed!\n--------------------\n\n");
    });
}

future<> single_sys_message_test() {
    MAX_THROUPUT = 1;
    return now().then([] {
        fmt::print("Sending CANCEL_SCOPE message...\n");
        address dst_addr{};
        unsigned dst_id[2] = {usr_id, op_id};
        dst_addr.length = sizeof(unsigned) * 2;
        memcpy(dst_addr.data, dst_id, dst_addr.length);

        auto *msg = make_actor_message(shd_id, message_type::CANCEL_SCOPE, dst_addr);
        return actor::channel::send_urgent(msg);
    }).then([] {
        fmt::print("Single system message test passed!\n--------------------\n\n");
    });
}

future<> multiple_message_test() {
    MAX_THROUPUT = 10;
    return now().then([] {
        fmt::print("Sending message...\n");
        return parallel_for_each(boost::irange(0, static_cast<int>(MAX_THROUPUT)), [] (int cnt) {
            address dst_addr{};
            unsigned dst_id[2] = {usr_id, op_id};
            dst_addr.length = sizeof(unsigned) * 2;
            memcpy(dst_addr.data, dst_id, dst_addr.length);

            std::string data{"processed message from engine " + std::to_string(engine().cpu_id()) + "."};
            auto *msg = make_actor_message(shd_id, message_type::USER, dst_addr,
                simple_cstring{data.c_str(), data.size() + 1});
            return actor::channel::send(msg);
        });
    }).then([] {
        fmt::print("Multiple message test passed!\n--------------------\n\n");
    });
}

future<> multiple_system_message_test() {
    MAX_THROUPUT = 1;
    return now().then([] {
        fmt::print("Sending CANCEL_SCOPE message...\n");
        return parallel_for_each(boost::irange(0, static_cast<int>(MAX_THROUPUT)), [] (int cnt) {
            auto shard_id = static_cast<unsigned int>(cnt % 2); // #cores = 2
            usr_id = static_cast<unsigned int>(cnt % 3) + 1;
            fmt::print("---- Create msg with dst_id {}, usr_id {}, op_id {}\n", shard_id, usr_id, op_id);
            address dst_addr{};
            unsigned dst_id[2] = {usr_id, op_id};
            dst_addr.length = sizeof(unsigned) * 2;
            memcpy(dst_addr.data, dst_id, dst_addr.length);

            auto *msg = make_actor_message(shard_id, message_type::CANCEL_SCOPE, dst_addr);
            return actor::channel::send(msg);
        });
    }).then([] {
        fmt::print("Multiple system message test passed!\n--------------------\n\n");
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
    app_template().run(ac, av, [] {
        return now().then([] {
            single_message_test();
        }).then([] {
            multiple_message_test();
        }).then([] {
            single_sys_message_test();
        }).then([] {
            multiple_system_message_test();
        }).then([] {
            send_peace_stop_message();
            return sleep(1s);
        }).then([] {
            fmt::print("**** Test Success!!! ****\n");
        });
    });
    return 0;
}
