#include <algorithm>
#include <cstring>
#include <cassert>
#include <iostream>
#include <vector>
#include "common/configs.hh"

/// graph storage
std::string dataset = "ldbc1";

/// query
unsigned batch_size = 64;
unsigned param_num = 10;
unsigned exec_epoch = 3;

/// Runner Environments
bool at_ic = false;
bool at_cq = false;
bool at_e1 = false;
bool at_e3 = false;
bool at_e4 = false;
bool at_e5 = false;

/// CQ query configurations
unsigned cq_max_si = 10;
unsigned cq_limit_n = 10;

bool cq_using_limit_cancel = true;
bool cq_using_branch_cancel = true;

cq_schedule_policy cq_query_policy = FIFO;
cq_schedule_policy cq_loop_policy = FIFO;
cq_schedule_policy cq_loop_instance_policy = FIFO;
cq_schedule_policy cq_where_policy = FIFO;
cq_schedule_policy cq_where_branch_policy = FIFO;

/// Evaluation Configurations
unsigned E1_concurrency = 1;

unsigned E3_concurrency = 32;
unsigned E3_rounds = 5;
unsigned E3_param_start_offset = 0;

unsigned E4_bg_query_num = 0;
unsigned E4_fg_exec_rounds = 51;
background_query_type E4_bg_query_type = Small;

unsigned E5_query_submit_interval_in_ms = 270;
unsigned E5_total_exec_rounds = 90;
unsigned E5_balance_round = 28;

unsigned E5_query_submit_interval_2_in_ms = 210;
unsigned E5_change_input_rate_round = 64;

/// Configuration Parse Functions
char* convert(const std::string& s)
{
    char *pc = new char[s.size() + 1];
    std::strcpy(pc, s.c_str());
    return pc;
}

args_type configure(int ac, char** av) {
    std::vector<std::string> remaining_args;
    remaining_args.reserve(ac);
    for (int i = 0; i < ac; i++) {
        std::string arg(av[i]);
        if (arg.rfind("-dataset=", 0) == 0) {
            auto d_name = arg.substr(9, arg.size() - 9);
            if (d_name != "ldbc1" && d_name != "ldbc100") {
                std::cout << "Unsupported dataset name! "
                             "Please use \"ldbc1\" or \"ldbc100\"!\n";
                abort();
            }
            dataset = std::move(d_name);
        } else if (arg.rfind("-batch-size=", 0) == 0) {
            try {
                batch_size = std::stoul(arg.substr(12, arg.size() - 12));
            } catch (...) {
                std::cout << "Error configuration with batch-size!\n";
                abort();
            }
        } else if (arg.rfind("-param-num=", 0) == 0) {
            try {
                param_num = std::stoul(arg.substr(11, arg.size() - 11));
                assert(param_num > 0 && param_num <= 50);
            } catch (...) {
                std::cout << "Error configuration with param-num!\n";
                abort();
            }
        } else if (arg.rfind("-exec-epoch=", 0) == 0) {
            try {
                exec_epoch = std::stoul(arg.substr(12, arg.size() - 12));
            } catch (...) {
                std::cout << "Error configuration with exec-epoch!\n";
                abort();
            }
        } else if (arg.rfind("-cq-max-si=", 0) == 0) {
            try {
                cq_max_si = std::stoul(arg.substr(11, arg.size() - 11));
            } catch (...) {
                std::cout << "Error configuration with cq-max-si!\n";
                abort();
            }
        } else if (arg.rfind("-cq-limit-n=", 0) == 0) {
            try {
                cq_limit_n = std::stoul(arg.substr(12, arg.size() - 12));
            } catch (...) {
                std::cout << "Error configuration with cq-limit-n!\n";
                abort();
            }
        } else if (arg == "--cq-close-limit-cancel") {
            cq_using_limit_cancel = false;
        } else if (arg == "--cq-close-branch-cancel") {
            cq_using_branch_cancel = false;
        } else if (arg.rfind("-cq-query-policy=", 0) == 0) {
            auto policy_name = arg.substr(17, arg.size() - 17);
            std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(), ::tolower);
            if (policy_name == "fifo") {
                cq_query_policy = FIFO;
            } else if (policy_name == "dfs") {
                cq_query_policy = DFS;
            } else if (policy_name == "bfs") {
                cq_query_policy = BFS;
            } else {
                std::cout << "Unsupported policy type! "
                             "Please use \"FIFO\", \"DFS\" or \"BFS\"!\n";
                abort();
            }
        } else if (arg.rfind("-cq-loop-policy=", 0) == 0) {
            auto policy_name = arg.substr(16, arg.size() - 16);
            std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(), ::tolower);
            if (policy_name == "fifo") {
                cq_loop_policy = FIFO;
            } else if (policy_name == "dfs") {
                cq_loop_policy = DFS;
            } else if (policy_name == "bfs") {
                cq_loop_policy = BFS;
            } else {
                std::cout << "Unsupported policy type! "
                             "Please use \"FIFO\", \"DFS\" or \"BFS\"!\n";
                abort();
            }
        } else if (arg.rfind("-cq-loop-instance-policy=", 0) == 0) {
            auto policy_name = arg.substr(25, arg.size() - 25);
            std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(), ::tolower);
            if (policy_name == "fifo") {
                cq_loop_instance_policy = FIFO;
            } else if (policy_name == "dfs") {
                cq_loop_instance_policy = DFS;
            } else if (policy_name == "bfs") {
                cq_loop_instance_policy = BFS;
            } else {
                std::cout << "Unsupported policy type! "
                             "Please use \"FIFO\", \"DFS\" or \"BFS\"!\n";
                abort();
            }
        } else if (arg.rfind("-cq-where-policy=", 0) == 0) {
            auto policy_name = arg.substr(17, arg.size() - 17);
            std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(), ::tolower);
            if (policy_name == "fifo") {
                cq_where_policy = FIFO;
            } else if (policy_name == "dfs") {
                cq_where_policy = DFS;
            } else if (policy_name == "bfs") {
                cq_where_policy = BFS;
            } else {
                std::cout << "Unsupported policy type! "
                             "Please use \"FIFO\", \"DFS\" or \"BFS\"!\n";
                abort();
            }
        } else if (arg.rfind("-cq-where-branch-policy=", 0) == 0) {
            auto policy_name = arg.substr(24, arg.size() - 24);
            std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(), ::tolower);
            if (policy_name == "fifo") {
                cq_where_branch_policy = FIFO;
            } else if (policy_name == "dfs") {
                cq_where_branch_policy = DFS;
            } else if (policy_name == "bfs") {
                cq_where_branch_policy = BFS;
            } else {
                std::cout << "Unsupported policy type! "
                             "Please use \"FIFO\", \"DFS\" or \"BFS\"!\n";
                abort();
            }
        } else if (arg.rfind("-e1-concurrency=", 0) == 0) {
            try {
                E1_concurrency = std::stoul(arg.substr(16, arg.size() - 16));
            } catch (...) {
                std::cout << "Error configuration with e1-concurrency!\n";
                abort();
            }
        } else if (arg.rfind("-e3-concurrency=", 0) == 0) {
            try {
                E3_concurrency = std::stoul(arg.substr(16, arg.size() - 16));
            } catch (...) {
                std::cout << "Error configuration with e3-concurrency!\n";
                abort();
            }
        } else if (arg.rfind("-e3-exec-rounds=", 0) == 0) {
            try {
                E3_rounds = std::stoul(arg.substr(16, arg.size() - 16));
                if (E3_rounds > 20) {
                    std::cout << "Error configuration! e3-exec-rounds should be no more than 20!\n";
                    abort();
                }
            } catch (...) {
                std::cout << "Error configuration with e3-exec-rounds!\n";
                abort();
            }
        } else if (arg.rfind("-e3-param-start-offset=", 0) == 0) {
            try {
                E3_param_start_offset = std::stoul(arg.substr(23, arg.size() - 23));
            } catch (...) {
                std::cout << "Error configuration with -e3-param-start-offset=!\n";
                abort();
            }
        } else if (arg.rfind("-e4-foreground-exec-times=", 0) == 0) {
            try {
                E4_fg_exec_rounds = std::stoul(arg.substr(26, arg.size() - 26));
            } catch (...) {
                std::cout << "Error configuration with e4-foreground-exec-times!\n";
                abort();
            }
        } else if (arg.rfind("-e4-background-num=", 0) == 0) {
            try {
                E4_bg_query_num = std::stoul(arg.substr(19, arg.size() - 19));
            } catch (...) {
                std::cout << "Error configuration with e4-background-num!\n";
                abort();
            }
        } else if (arg.rfind("-e4-background-type=", 0) == 0) {
            auto bg_type = arg.substr(20, arg.size() - 20);
            if (bg_type == "small") {
                E4_bg_query_type = background_query_type::Small;
            } else if (bg_type == "large") {
                E4_bg_query_type = background_query_type::Large;
            } else {
                std::cout << "Error configuration with e4-background-type! "
                             "Please use \"small\" or \"large\"!\n";
                abort();
            }
        } else if (arg.rfind("-e5-submit-interval-ms=", 0) == 0) {
            try {
                E5_query_submit_interval_in_ms = std::stoul(arg.substr(23, arg.size() - 23));
            } catch (...) {
                std::cout << "Error configuration with e5-submit-interval-ms!\n";
                abort();
            }
        } else if (arg.rfind("-e5-exec-rounds=", 0) == 0) {
            try {
                E5_total_exec_rounds = std::stoul(arg.substr(16, arg.size() - 16));
            } catch (...) {
                std::cout << "Error configuration with e5-exec-rounds!\n";
                abort();
            }
        } else if (arg.rfind("-e5-balance-round=", 0) == 0) {
            try {
                E5_balance_round = std::stoul(arg.substr(18, arg.size() - 18));
            } catch (...) {
                std::cout << "Error configuration with e5-balance-round!\n";
                abort();
            }
        } else if (arg.rfind("-e5-stage2-submit-interval-ms=", 0) == 0) {
            try {
                E5_query_submit_interval_2_in_ms = std::stoul(arg.substr(30, arg.size() - 30));
            } catch (...) {
                std::cout << "Error configuration with e5-stage2-submit-interval-ms!\n";
                abort();
            }
        } else if (arg.rfind("-e5-change-input-rate-round=", 0) == 0) {
            try {
                E5_change_input_rate_round = std::stoul(arg.substr(28, arg.size() - 28));
            } catch (...) {
                std::cout << "Error configuration with e5-change-input-rate-round!\n";
                abort();
            }
        } else {
            remaining_args.push_back(std::move(arg));
        }
    }
    remaining_args.shrink_to_fit();

    auto* char_type_remaining_args = new std::vector<char*>;
    std::transform(remaining_args.begin(), remaining_args.end(),
                   std::back_inserter(*char_type_remaining_args), convert);
    return {char_type_remaining_args, [] (std::vector<char*>* args) {
        for (auto arg : *args) {
            delete [] arg;
        }
    }};
}