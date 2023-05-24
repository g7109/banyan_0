//
// Created by lisu on 2021-04-15.
//

#include <brane/actor/actor-system.hh>
#include "cq_queries/cq_5x/cq5x_executor.hh"

int main(int ac, char** av) {
    auto remaining_args = configure(ac, av);
    at_cq = true;
    initialize_log_env(std::string(banyan_gqs_dir) + "/results/logs/cq_5x.log");
    cq5_helper.load_cq_params();

    brane::actor_system sys;
    sys.run(remaining_args->size(), remaining_args->data(), [] {
        return init_graph_store().then([] {
            if (brane::global_shard_id() == 0) {
                exec_cq5x(cq5_helper.get_start_person(cq5_helper.current_query_id()));
            }
        });
    });

    finalize_log_env();
    std::cout << "[Exec finished] Results have been writen into apps/banyan_gqs/results/logs/cq_5x.log\n";
    return 0;
}