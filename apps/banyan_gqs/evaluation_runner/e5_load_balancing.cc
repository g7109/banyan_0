#include <brane/actor/actor-system.hh>
#include "ldbc_queries/query_6/q6_executor.hh"

unsigned cur_round = 1;

seastar::future<> submit_queries() {
    return seastar::repeat([] {
        e5_exec_query_6(cur_round);
        auto interval_t = cur_round < E5_change_input_rate_round ?
                E5_query_submit_interval_in_ms : E5_query_submit_interval_2_in_ms;
        return seastar::sleep(std::chrono::milliseconds(interval_t)).then([] {
            if (++cur_round <= E5_total_exec_rounds) {
                return seastar::stop_iteration::no;
            }
            return seastar::stop_iteration::yes;
        });
    });
}

int main(int ac, char** av) {
    auto remaining_args = configure(ac, av);
    at_e5 = true;
    load_balancer::set_skewed();
    initialize_log_env(std::string(banyan_gqs_dir) + "/results/logs/e5_load_balancing.log");
    q6_helper.set_fixed_params(4017592186292997L, "Stephen_King");

    brane::actor_system sys;
    sys.run(remaining_args->size(), remaining_args->data(), [] {
        return init_graph_store().then([] {
            e5_exec_query_6(0);
            return seastar::sleep(std::chrono::seconds(1));
        }).then([] {
            return submit_queries();
        });
    });

    finalize_log_env();
    std::cout << "[Exec finished] Results have been writen into "
                 "apps/banyan_gqs/results/logs/e5_load_balancing.log\n";
    return 0;
}