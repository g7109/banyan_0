/**
 * DAIL in Alibaba Group
 *
 */

#include <seastar/haf/actor_implementation.hh>
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
#include <math.h>
#include <string.h>
using namespace seastar;
using namespace std::chrono_literals;

class out : public stateless_actor {
public:
    explicit out(execution_context_base *exec_ctx, actor::actor_reference *parent, const byte_t *addr,
        byte_t addr_len) : stateless_actor(exec_ctx, parent, addr, addr_len) {}

    future<stop_reaction> do_work(actor_message* msg) override {
        return do_real_work();
    }

    inline future<stop_reaction> do_real_work() {
        return make_ready_future<stop_reaction>(stop_reaction::no);
    }
};

class get_actor_tester : public stateless_actor {
public:
    explicit get_actor_tester(execution_context_base *exec_ctx, actor::actor_reference *parent,
        const byte_t *addr, byte_t addr_len) : stateless_actor(exec_ctx, parent, addr, addr_len) {
        fmt::print("get_actor_tester is created \n");
    }

    future<stop_reaction> do_work(actor_message* msg) override {
        auto *real_msg = reinterpret_cast<actor_message_with_payload<simple_cstring>*>(msg);
        return do_real_work(std::move(real_msg->data));
    }

    inline future<stop_reaction> do_real_work(simple_cstring&& data) {
        return now().then([this, data = std::move(data)] () mutable {
            fmt::print("Start calling get_actor()\n");

            auto *c_str = new char[data.len];
            memcpy(c_str, data.c_str, data.len);

            char *s1 = strtok(c_str, " ");
            char *s2 = strtok(NULL, " ");
            int N = atoi(s1);
            int F = atoi(s2);
            int msg_count = pow(F, N);
            auto actor_addrs = new address[msg_count];
            auto actor_references = new actor_reference*[msg_count];
            for (int idx = 0; idx < msg_count; idx++) {
                auto dist = new unsigned[N];
                for (int i = 0; i < N; ++i) {
                    if (i != (N - 1)) {
                        auto ag_id = ((idx + 1) / (int) pow(F, N - i)) % F + 11;
                        dist[i] = ag_id;
                    } else {
                        auto op_id = (idx + 1) % F == 0 ? F : (idx + 1) % F;
                        dist[i] = op_id;
                    }
                }
                address dst_addr{};
                dst_addr.length = N * sizeof(unsigned);
                memcpy(dst_addr.data, dist, dst_addr.length);

                auto *msg = make_actor_message(1, message_type::USER, dst_addr);
                actor_addrs[idx].length = msg->hdr.addr.length;
                std::memcpy(actor_addrs[idx].data, msg->hdr.addr.data, msg->hdr.addr.length);
                delete[] dist;
                delete msg;
            }

            int R = pow(F, 5 - N);
            std::chrono::system_clock::time_point t1, t2;

            for(int t = 0; t < 5; t++) {
                t1 = std::chrono::system_clock::now();
                for(int r = 0; r< R; r++) {
                    for(int idx = 0; idx < msg_count; idx++) {
                        actor_references[idx] = get_actor(1, actor_addrs[idx]);
                    }
                }
                t2 = std::chrono::system_clock::now();
                fmt::print("A get_actor() call takes {} ns. \n", (t2 - t1).count() / pow(F, 5));
            }
            delete[] actor_addrs;
            delete[] actor_references;
            delete[] c_str;
            return make_ready_future<stop_reaction>(stop_reaction::no);
        }, this);
    }
};

class dummy_actor_group : public actor_group {
public:
    explicit dummy_actor_group(execution_context_base *exec_ctx, actor::actor_reference *parent,
        const byte_t *addr, byte_t addr_len) : actor_group(exec_ctx, parent, addr, addr_len) {
    }
};

namespace auto_registeration {

using namespace seastar::registration;
actor_registration<out> _out_1_op(1);
actor_registration<out> _out_2_op(2);
actor_registration<out> _out_3_op(3);
actor_registration<out> _out_4_op(4);
actor_registration<out> _out_5_op(5);
actor_registration<out> _out_6_op(6);
actor_registration<out> _out_7_op(7);
actor_registration<out> _out_8_op(8);
actor_registration<out> _out_9_op(9);
actor_registration<out> _out_10_op(10);

actor_registration<dummy_actor_group> _dag_1{11};
actor_registration<dummy_actor_group> _dag_2{12};
actor_registration<dummy_actor_group> _dag_3{13};
actor_registration<dummy_actor_group> _dag_4{14};
actor_registration<dummy_actor_group> _dag_5{15};
actor_registration<dummy_actor_group> _dag_6{16};
actor_registration<dummy_actor_group> _dag_7{17};
actor_registration<dummy_actor_group> _dag_8{18};
actor_registration<dummy_actor_group> _dag_9{19};
actor_registration<dummy_actor_group> _dag_10{20};

actor_registration<dummy_actor_group> _dag_11{22};
actor_registration<get_actor_tester> _gat{23};

} // namespace auto_registeration

future<> generate_input_messages(int N, int F) {
    int msg_count = pow(F, N);
    return now().then([N, F, msg_count] () mutable {
        return parallel_for_each(boost::irange(0, msg_count), [&] (int idx) {
            auto dist = new unsigned[N];
            for (int i = 0; i < N; ++i) {
                if (i != (N - 1)) {
                    auto ag_id = ((idx + 1) / (int) pow(F, N - i)) % F + 11;
                    dist[i] = ag_id;
                    // fmt::print("Ag : {} \n", ag_id);
                } else {
                    // auto op_id = ((idx + 1) / (int) pow(F, N - i)) % F + 1;
                    auto op_id = (idx + 1) % F == 0 ? F : (idx + 1) % F;
                    dist[i] = op_id;
                    // fmt::print("Actor: {} \n", op_id);
                }
            }
            address dst_addr{};
            dst_addr.length = N * sizeof(unsigned);
            memcpy(dst_addr.data, dist, dst_addr.length);

            auto *msg = make_actor_message(1, message_type::USER, dst_addr);
//             fmt::print("Message {} address {}, length {} \n", idx,
//                 char_arr_to_str(msg->hdr.addr.data, msg->hdr.addr.length), msg->hdr.addr.length);
            delete[] dist;
            return actor::channel::send(msg);
        });
    }).then([msg_count] {
        fmt::print("Completed dispatching {} messages. \n", msg_count);
    });
}

future<> generate_test_message(int N, int F) {
    return now().then([N, F] () mutable {
        return parallel_for_each(boost::irange(0, 1), [=] (int idx) {

            address dst_addr{};
            unsigned dst_id[2] = {22, 23};
            dst_addr.length = sizeof(unsigned) * 2;
            memcpy(dst_addr.data, dst_id, dst_addr.length);

            std::string data{std::to_string(N)+" "+std::to_string(F)};
            auto *msg = make_actor_message(1, message_type::USER, dst_addr,
                simple_cstring{data.c_str(), data.size() + 1});
            fmt::print("Message {} address {}, length {} \n", idx,
                char_arr_to_str(msg->hdr.addr.data, msg->hdr.addr.length), msg->hdr.addr.length);
            return actor::channel::send(msg);
        });
    }).then([] {
        fmt::print("Completed dispatching test messages. \n");
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
        fmt::print("Get actor performance test started.\n");
        int N = 4, F = 10;
        return now().then([N, F] {
            generate_input_messages(
                N   /** =num_of_layer */,
                F   /** =fanout */);
        }).then([] {
            return sleep(5s);
        }).then([N, F] {
            generate_test_message(N, F);
        }).then([] {
            return sleep(5s);
        }).then([] {
            fmt::print("**** Test Success!!! ****\n");
            return send_peace_stop_message();
        });
    });
    return 0;
}
