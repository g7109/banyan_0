#pragma once

#include <functional>
#include <memory>
#include <string>

/// Dir
#ifdef BANYAN_GQS_REPO
    #define banyan_gqs_dir BANYAN_GQS_REPO
#endif

/// graph storage
extern std::string dataset;
const unsigned graph_partition_num = 64;

/// query
extern unsigned batch_size;
extern unsigned param_num;
extern unsigned exec_epoch;

/// Runner Environments
extern bool at_ic;
extern bool at_cq;
extern bool at_e1;
extern bool at_e3;
extern bool at_e4;
extern bool at_e5;

/// LDBC interactive benchmark configurations
const unsigned ic_query_num = 12;

/// CQ query configurations
const unsigned cq_num = 6;
extern unsigned cq_max_si;
extern unsigned cq_limit_n;

extern bool cq_using_limit_cancel;
extern bool cq_using_branch_cancel;

enum cq_schedule_policy { FIFO, DFS, BFS };
extern cq_schedule_policy cq_query_policy;
extern cq_schedule_policy cq_loop_policy;
extern cq_schedule_policy cq_loop_instance_policy;
extern cq_schedule_policy cq_where_policy;
extern cq_schedule_policy cq_where_branch_policy;

/// Evaluation Configurations
// [E1] Scalability with concurrent queries
extern unsigned E1_concurrency;
const unsigned E1_total_exec_rounds = 128;

// [E3] Performance Isolation : concurrent workload with mixed big and small queries
extern unsigned E3_concurrency;  // 32 64 128
extern unsigned E3_rounds;
extern unsigned E3_param_start_offset;

// [E4] Performance Isolation : foreground query under different background workload
extern unsigned E4_bg_query_num; // 1 2 3 4
extern unsigned E4_fg_exec_rounds;
enum background_query_type { Small, Large };
extern background_query_type E4_bg_query_type;

// [E5] Load Balancing
extern unsigned E5_query_submit_interval_in_ms;
extern unsigned E5_total_exec_rounds;
extern unsigned E5_balance_round;

extern unsigned E5_query_submit_interval_2_in_ms;
extern unsigned E5_change_input_rate_round;

/// Configuration Parse Function
/// Return remaining args for seastar.
using args_type = std::unique_ptr<std::vector<char*>, std::function<void(std::vector<char*>*)>>;
args_type configure(int ac, char** av);
