#include <brane/actor/actor-system.hh>
#include "ldbc_queries/query_6/q6_executor.hh"

void concurrent_exec_6() {
    for(unsigned i = 1; i <= E1_concurrency; i++) {
        e1_exec_query_6(i);
    }
}

int main(int ac, char** av) {
    auto remaining_args = configure(ac, av);
    at_e1 = true;
    initialize_log_env(std::string(banyan_gqs_dir) + "/results/logs/e1_concurrent_scalability.log");
    q6_helper.set_fixed_params(4010995116542546L, "Universal_Studios");

    brane::actor_system sys;
    sys.run(remaining_args->size(), remaining_args->data(), [] {
        return init_graph_store().then([] {
            e1_exec_query_6(0);
        }).then([] {
            return seastar::sleep(2s);
        }).then([] {
            concurrent_exec_6();
        });
    });

    finalize_log_env();
    std::cout << "[Exec finished] Results have been writen into "
                 "apps/banyan_gqs/results/logs/e1_concurrent_scalability.log\n";
    return 0;
}

