#include <brane/actor/actor-system.hh>
#include "ldbc_queries/query_1/q1_executor.hh"
#include "ldbc_queries/query_9/q9_executor.hh"
#include "ldbc_queries/query_10/q10_executor.hh"

void run_background() {
    for(unsigned i = 1; i <= E4_bg_query_num; i++) {
        seastar::sleep(std::chrono::milliseconds(50 * i)).then([i] {
            if (E4_bg_query_type == background_query_type::Small) {
                e4_exec_query_10(E4_fg_exec_rounds + i);
            } else if (E4_bg_query_type == background_query_type::Large) {
                e4_exec_query_9(E4_fg_exec_rounds + i);
            }
        });
    }
}

void run_foreground() {
    e4_exec_query_1(0);
}

int main(int ac, char** av) {
    auto remaining_args = configure(ac, av);
    at_e4 = true;
    initialize_log_env(std::string(banyan_gqs_dir)
        + "/results/logs/e4_background_workload.log");
    q1_helper.set_fixed_params(4015393162790365L, "John");
    q9_helper.set_fixed_params(4004398046595696L, 1337040000000L);
    q10_helper.set_fixed_params(4008796093197501L, 4L);

    brane::actor_system sys;
    sys.run(remaining_args->size(), remaining_args->data(), [] {
        return init_graph_store().then([] {
            run_background();
            return seastar::sleep(std::chrono::seconds(2));
        }).then([] {
            run_foreground();
        });
    });

    finalize_log_env();
    std::cout << "[Exec finished] Results have been writen into "
                 "apps/banyan_gqs/results/logs/e4_background_workload.log\n";
    return 0;
}