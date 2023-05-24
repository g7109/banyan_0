//
// Created by lisu on 2021-04-16.
//

#include <brane/actor/actor-system.hh>
#include "cq_queries/cq_6/cq6_executor.hh"

int main(int ac, char** av) {
    /// set default best policy
    cq_query_policy = DFS;
    cq_loop_policy = DFS;
    cq_loop_instance_policy = DFS;
    cq_where_policy = BFS;
    cq_where_branch_policy = DFS;

    auto remaining_args = configure(ac, av);
    at_cq = true;
    initialize_log_env(std::string(banyan_gqs_dir) + "/results/logs/cq_6.log");
    cq6_helper.load_cq_params();

    brane::actor_system sys;
    sys.run(remaining_args->size(), remaining_args->data(), [] {
        return init_graph_store().then([] {
            if (brane::global_shard_id() == 0) {
                exec_cq6(cq6_helper.get_start_person(cq6_helper.current_query_id()));
            }
        });
    });

    finalize_log_env();
    std::cout << "[Exec finished] Results have been writen into apps/banyan_gqs/results/logs/cq_6.log\n";
    return 0;
}