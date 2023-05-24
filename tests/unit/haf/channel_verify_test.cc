/**
 * DAIL in Alibaba Group
 *
 */

#include <seastar/haf/actor_implementation.hh>
#include <seastar/haf/actor_factory.hh>
#include <seastar/haf/util/unaligned_int.hh>
#include <seastar/haf/util/data_type.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/print.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <thread>
#include <random>
#include <ctime>

using namespace seastar;
using namespace std::chrono_literals;

std::map<std::pair<unsigned, unsigned>, unsigned> num_tsk_per_usr_op;

actor_message* generate_single_msg(unsigned shard_id, unsigned vals = 0) {
    address dst_addr{};
    unsigned dst_id[2] = {1, 1};
    dst_addr.length = sizeof(unsigned) * 2;
    memcpy(dst_addr.data, dst_id, dst_addr.length);

    auto *msg = make_actor_message(shard_id, message_type::USER,
        dst_addr, simple_array<unsigned, 2>{});
    msg->data.arr[0] = engine().cpu_id();
    msg->data.arr[1] = vals;
    return msg;
}

class resend : public stateless_actor {
public:
    explicit resend(execution_context_base *exec_ctx, actor::actor_reference *parent,
        const byte_t *addr, byte_t addr_len) : stateless_actor(exec_ctx, parent, addr, addr_len) {}

    future<stop_reaction> do_work(actor_message* msg) override {
        auto *real_msg = reinterpret_cast<actor_message_with_payload<simple_array<unsigned, 2>>*>(msg);
        return do_real_work(std::move(real_msg->data));
    }

    inline future<stop_reaction> do_real_work(simple_array<unsigned, 2>&& data) {
        return now().then([this, data = std::move(data)] () mutable {
            fmt::print("Shard {:d} receives value {:d} from Shard {:d}\n", engine().cpu_id(), data.arr[1], data.arr[0]);
            if (data.arr[1] < 1) {
                auto *msg = generate_single_msg((engine().cpu_id() + 1) % 4, data.arr[1] + 1);
                actor::actor_reference* actor = get_actor(msg->hdr.shard_id, msg->hdr.addr);
                fmt::print("Shard {:d} sends message to Shard {:d}\n", engine().cpu_id(), msg->hdr.shard_id);
                actor->enque_message(msg);
                return make_ready_future<stop_reaction>(stop_reaction::no);
            } else {
                return make_ready_future<stop_reaction>(stop_reaction::yes);
            }
        }, this);
    }
};

namespace auto_registeration {

seastar::registration::actor_registration<resend> _resend_op(1);

} // namespace auto_registeration

void print_msg_summary() {
    for (auto &it : num_tsk_per_usr_op) {
        fmt::print("Usr {}, op {} has msg {} in total\n", it.first.first, it.first.second, it.second);
    }
}

future<> test_dataflow() {
    return now().then([] () {
        return parallel_for_each(boost::irange<unsigned>(0, 1), [] (unsigned t) {
            actor_message* msg = generate_single_msg(t);
            return actor::channel::send(msg);
        });
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
        return test_dataflow().then([] {
            return sleep(1s);
        }).then([] {
            send_peace_stop_message();
            return sleep(1s);
        }).then([] {
            engine().exit(0);
        });
    });
    return 0;
}
