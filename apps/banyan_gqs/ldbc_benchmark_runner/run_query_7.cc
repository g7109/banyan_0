#include <brane/actor/actor-system.hh>
#include "ldbc_queries/query_7/q7_executor.hh"

int main(int ac, char** av) {
    auto remaining_args = configure(ac, av);
    at_ic = true;
    initialize_log_env(std::string(banyan_gqs_dir) + "/results/logs/ic_7.log");
    q7_helper.load_benchmark_params();

    brane::actor_system sys;
    sys.run(remaining_args->size(), remaining_args->data(), [] {
        return init_graph_store().then([] {
            if (brane::global_shard_id() == 0) {
                benchmark_exec_query_7(q7_helper.get_start_person(q7_helper.current_query_id()));
            }
        });
    });

    finalize_log_env();
    std::cout << "[Exec finished] Results have been writen into apps/banyan_gqs/results/logs/ic_7.log\n";
    return 0;
}
