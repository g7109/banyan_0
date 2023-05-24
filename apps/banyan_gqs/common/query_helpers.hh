//
// Created by everlighting on 2021/3/8.
//

#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <seastar/core/print.hh>
#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/logs.hh"

template <unsigned QueryID>
class benchmark_query_helper {
protected:
    unsigned cur_round = 0;
    bool using_benchmark = true;
    std::vector<uint64_t> start_timestamps{};
    std::vector<uint64_t> end_timestamps{};
public:
    inline void next_round() { cur_round++; }
    inline unsigned current_round() { return cur_round; }
    inline unsigned current_query_id() { return QueryID + cur_round * ic_query_num; }

    inline size_t get_timestamp_size() {
        return end_timestamps.size();
    }

    inline void record_start_time() {
        start_timestamps.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    inline void record_end_time() {
        end_timestamps.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }

    inline void compute_benchmark_exec_time() {
        double total[exec_epoch];
        for (unsigned i = 0; i < exec_epoch; i++) {
            total[i] = 0;
        }
        for (unsigned i = 0; i < start_timestamps.size(); i++) {
            write_log(seastar::format(
                    "param group {} of epoch {} exec time(us) : {}\n",
                    i / exec_epoch, i % exec_epoch, end_timestamps[i] - start_timestamps[i]));
            total[i % exec_epoch] += (end_timestamps[i] - start_timestamps[i]);
        }
        for (unsigned i = 0; i < exec_epoch; i++) {
            write_log(seastar::format(
                    "[LDBC Query {}] [Epoch {}] Average exec time(ms) : {}\n",
                    QueryID, i, (total[i] / 1000) / param_num));
        }
    }
};

class query1_helper : public benchmark_query_helper<1> {
    std::vector<int64_t> start_person_ids{};
    std::vector<std::string> first_names{};
    int64_t fix_start_person_id{};
    std::string fix_first_name;
public:
    query1_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, const std::string& first_name) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_first_name = first_name;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline std::string get_first_name(unsigned q_id) {
        if (using_benchmark) {
            return first_names[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_first_name;
    }
};
extern query1_helper q1_helper;

class query2_helper : public benchmark_query_helper<2> {
    std::vector<int64_t> start_person_ids;
    std::vector<int64_t> max_dates;
    int64_t fix_start_person_id{};
    int64_t fix_max_date{};
public:
    query2_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, int64_t max_date) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_max_date = max_date;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline int64_t get_max_date(unsigned q_id) const {
        if (using_benchmark) {
            return max_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_max_date;
    }
};
extern query2_helper q2_helper;

class query3_helper : public benchmark_query_helper<3> {
    std::vector<int64_t> start_person_ids;
    std::vector<int64_t> start_dates;
    std::vector<int64_t> end_dates;
    std::vector<std::string> country_x_names;
    std::vector<std::string> country_y_names;
    int64_t fix_start_person_id{};
    int64_t fix_start_date{};
    int64_t fix_end_date{};
    std::string fix_country_x;
    std::string fix_country_y;
public:
    query3_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, int64_t start_date, int64_t end_date,
            const std::string& country_x, const std::string& country_y) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_start_date = start_date;
        fix_end_date = end_date;
        fix_country_x = country_x;
        fix_country_y = country_y;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline int64_t get_start_date(unsigned q_id) const {
        if (using_benchmark) {
            return start_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_date;
    }
    inline int64_t get_end_date(unsigned q_id) const {
        if (using_benchmark) {
            return end_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_end_date;
    }
    inline std::string get_country_x(unsigned q_id) {
        if (using_benchmark) {
            return country_x_names[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_country_x;
    }
    inline std::string get_country_y(unsigned q_id) {
        if (using_benchmark) {
            return country_y_names[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_country_y;
    }
};
extern query3_helper q3_helper;

class query4_helper : public benchmark_query_helper<4> {
    std::vector<int64_t> start_person_ids;
    std::vector<int64_t> start_dates;
    std::vector<int64_t> end_dates;
    int64_t fix_start_person_id{};
    int64_t fix_start_date{};
    int64_t fix_end_date{};
public:
    query4_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, int64_t start_date, int64_t end_date) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_start_date = start_date;
        fix_end_date = end_date;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline int64_t get_start_date(unsigned q_id) {
        if (using_benchmark) {
            return start_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_date;
    }
    inline int64_t get_end_date(unsigned q_id) {
        if (using_benchmark) {
            return end_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_end_date;
    }
};
extern query4_helper q4_helper;

class query5_helper : public benchmark_query_helper<5> {
    std::vector<int64_t> start_person_ids;
    std::vector<int64_t> min_dates;
    int64_t fix_start_person_id{};
    int64_t fix_min_date{};
public:
    query5_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, int64_t min_date) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_min_date = min_date;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    int64_t get_min_date(unsigned q_id) const {
        if (using_benchmark) {
            return min_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_min_date;
    }
};
extern query5_helper q5_helper;

class query6_helper : public benchmark_query_helper<6> {
    std::vector<int64_t> start_person_ids{};
    std::vector<std::string> tag_names{};
    int64_t fix_start_person_id{};
    std::string fix_tag_name;

//    static thread_local std::string filter_tag_name;
//    int64_t filter_tag_id{};
public:
    query6_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, const std::string& tag_name) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_tag_name = tag_name;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline std::string get_tag_name(unsigned q_id) {
        if (using_benchmark) {
            return tag_names[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_tag_name;
    }

//    static inline void set_filter_tag_name(std::string&& name) {
//        filter_tag_name = std::move(name);
//    }
//
//    static inline const std::string& get_filter_tag_name() {
//        return filter_tag_name;
//    }
//
//    inline void set_filter_tag_id(int64_t tag_id) {
//        filter_tag_id = tag_id;
//    }
//
//    inline int64_t get_filter_tag_id() {
//        return filter_tag_id;
//    }
};
extern query6_helper q6_helper;

class query7_helper : public benchmark_query_helper<7> {
    std::vector<int64_t> start_person_ids{};
    int64_t fix_start_person_id{};
public:
    query7_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id) {
        using_benchmark = false;
        fix_start_person_id = person_id;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
};
extern query7_helper q7_helper;

class query8_helper : public benchmark_query_helper<8> {
    std::vector<int64_t> start_person_ids{};
    int64_t fix_start_person_id{};
public:
    query8_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id) {
        using_benchmark = false;
        fix_start_person_id = person_id;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
};
extern query8_helper q8_helper;

class query9_helper : public benchmark_query_helper<9> {
    std::vector<int64_t> start_person_ids;
    std::vector<int64_t> max_dates;
    int64_t fix_start_person_id{};
    int64_t fix_max_date{};
public:
    query9_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, int64_t max_date) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_max_date = max_date;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline int64_t get_max_date(unsigned q_id) const {
        if (using_benchmark) {
            return max_dates[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_max_date;
    }
};
extern query9_helper q9_helper;

class query10_helper : public benchmark_query_helper<10> {
    std::vector<int64_t> start_person_ids;
    std::vector<int64_t> months;
    int64_t fix_start_person_id{};
    int64_t fix_month{};
public:
    query10_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, int64_t month) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_month = month;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline int64_t get_month(unsigned q_id) const {
        if (using_benchmark) {
            return months[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_month;
    }
};
extern query10_helper q10_helper;

class query11_helper : public benchmark_query_helper<11> {
    std::vector<int64_t> start_person_ids;
    std::vector<std::string> country_names;
    std::vector<int64_t> work_from_years;
    int64_t fix_start_person_id{};
    std::string fix_country_name;
    int64_t fix_work_from_year{};
public:
    query11_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, const std::string& country_name, int64_t work_from_year) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_country_name = country_name;
        fix_work_from_year = work_from_year;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    inline std::string get_country_name(unsigned q_id) {
        if (using_benchmark) {
            return country_names[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_country_name;
    }
    inline int64_t get_work_from_year(unsigned q_id) const {
        if (using_benchmark) {
            return work_from_years[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_work_from_year;
    }
};
extern query11_helper q11_helper;

class query12_helper : public benchmark_query_helper<12> {
    std::vector<int64_t> start_person_ids;
    std::vector<std::string> tag_class_names;
    int64_t fix_start_person_id{};
    std::string fix_tag_class_name;
public:
    query12_helper() = default;
    void load_benchmark_params();
    inline void set_fixed_params(int64_t person_id, const std::string& tag_class_name) {
        using_benchmark = false;
        fix_start_person_id = person_id;
        fix_tag_class_name = tag_class_name;
    }
    inline int64_t get_start_person(unsigned q_id) const {
        if (using_benchmark) {
            return start_person_ids[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_start_person_id;
    }
    std::string get_tag_class_name(unsigned q_id) {
        if (using_benchmark) {
            return tag_class_names[((q_id - 1) / ic_query_num) / exec_epoch];
        }
        return fix_tag_class_name;
    }
};
extern query12_helper q12_helper;

/// ---

class evaluation1_helper {
    std::vector<uint64_t> start_timestamps{};
    std::vector<uint64_t> end_timestamps{};
public:
    inline size_t get_timestamp_size() {
        return end_timestamps.size();
    }
    inline void record_start_time() {
        start_timestamps.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    inline void record_end_time() {
        end_timestamps.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    inline void compute_concurrent_throughput_and_latency() {
        auto exec_num = std::min(start_timestamps.size(), end_timestamps.size());
        uint64_t total_start = 0;
        uint64_t total_end = 0;
        auto min_start = start_timestamps[0];
        auto max_end = end_timestamps[0];
        for(unsigned i = 0; i < exec_num; i++) {
            total_start += start_timestamps[i];
            total_end += end_timestamps[i];
            min_start = std::min(min_start, start_timestamps[i]);
            max_end = std::max(max_end, end_timestamps[i]);
        }
        write_log(seastar::format("E1 concurrency : {}\n", E1_concurrency));
        write_log(seastar::format("Throughput(1/s) : {}\n",
              static_cast<double>(exec_num * 1000) / static_cast<double>(max_end - min_start)));
        write_log(seastar::format("Average Latency(ms) : {}\n",
              (total_end - total_start) / exec_num));
    }
};
extern evaluation1_helper e1_helper;

class evaluation3_helper {
    std::unordered_map<unsigned, int64_t> small_start_timestamps{};
    std::unordered_map<unsigned, int64_t> small_end_timestamps{};
    std::unordered_map<unsigned, int64_t> big_start_timestamps{};
    std::unordered_map<unsigned, int64_t> big_end_timestamps{};
    unsigned cur_round = 1;
public:
    inline void next_round() { cur_round++; }
    inline unsigned current_round() const { return cur_round; }

    inline void record_small_start_time(unsigned qid) {
        small_start_timestamps[qid] = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    inline void record_small_end_time(unsigned qid) {
        small_end_timestamps[qid] = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    inline void log_small_exec_time() {
        write_log("[Small query exec times(ms)]\n");
        std::string exec_times = "";
        for(auto& iter : small_end_timestamps) {
            exec_times += std::to_string(iter.second - small_start_timestamps[iter.first]) + "\n";
        }
        write_log(exec_times);
    }

    inline void record_big_start_time(unsigned qid) {
        big_start_timestamps[qid] = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    inline void record_big_end_time(unsigned qid) {
        big_end_timestamps[qid] = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    inline void log_big_exec_time() {
        write_log("[Big query exec times(ms)]\n");
        std::string exec_times = "";
        for(auto& iter : big_end_timestamps) {
            exec_times += std::to_string(iter.second - big_start_timestamps[iter.first]) + "\n";
        }
        write_log(exec_times);
    }

    inline size_t get_all_timestamp_size() {
        return small_end_timestamps.size() + big_end_timestamps.size();
    }
};
extern evaluation3_helper e3_helper;

class evaluation4_helper {
    std::vector<uint64_t> fg_start_timestamps{};
    std::vector<uint64_t> fg_end_timestamps{};
public:
    inline void record_fg_start_time() {
        fg_start_timestamps.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    inline void record_fg_end_time() {
        fg_end_timestamps.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    inline void log_fg_exec_times() {
        write_log(seastar::format(
                "[Foreground Query Exec Times(ms)] background_query_type: {}, background_query_number: {}\n",
                E4_bg_query_type == background_query_type::Small? "Small" : "Large", E4_bg_query_num));
        std::string exec_times = "";
        for(unsigned i = 1; i < fg_end_timestamps.size(); i++) {
            exec_times += std::to_string(static_cast<double>(fg_end_timestamps[i] - fg_start_timestamps[i]) / 1000) + "\n";
        }
        write_log(exec_times);
    }
};
extern evaluation4_helper e4_helper;

class evaluation5_helper {
    std::unordered_map<unsigned, int64_t> start_timestamps{};
    std::unordered_map<unsigned, int64_t> end_timestamps{};
public:
    inline size_t get_timestamp_size() {
        return end_timestamps.size();
    }
    inline void record_start_time(unsigned qid) {
        start_timestamps[qid] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    inline void record_end_time(unsigned qid) {
        end_timestamps[qid] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    inline void log_skew_to_balance_exec_times() {
        start_timestamps.erase(0);
        end_timestamps.erase(0);
        auto min_time = LONG_MAX;
        for(auto& iter : start_timestamps) {
            min_time = std::min(min_time, iter.second);
        }
        std::string begin_results = "";
        std::string end_results = "";
        std::string latency_results = "";
        for (unsigned i = 1; i <= E5_total_exec_rounds; i++) {
            begin_results += std::to_string(start_timestamps[i] - min_time) + "\n";
            end_results += std::to_string(end_timestamps[i] - min_time) + "\n";
            latency_results += std::to_string(end_timestamps[i] - start_timestamps[i]) + "\n";
        }
        write_log("[Start Timestamp(ms)]\n" + begin_results);
        write_log("[End Timestamp(ms)]\n" + end_results);
        write_log("[Exec Times(ms)]\n" + latency_results);
//        std::cout << "[Start Timestamp(ms)]\n" + begin_results
//                  << "[End Timestamp(ms)]\n" + end_results
//                  << "[Exec Times(ms)]\n" + latency_results;
    }
};
extern evaluation5_helper e5_helper;

/// ---

template <unsigned CQ_ID>
class cq_basic_helper {
protected:
    unsigned cur_round = 0;
    std::vector<uint64_t> start_timestamps{};
    std::vector<uint64_t> end_timestamps{};
public:
    inline void next_round() { cur_round++; }
    inline unsigned current_round() { return cur_round; }
    inline unsigned current_query_id() { return CQ_ID + cur_round * cq_num; }

    inline void record_start_time() {
        start_timestamps.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    inline void record_end_time() {
        end_timestamps.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }

    inline void log_cq_exec_times() {
        log_time_by_param();
        log_time_by_epoch();
    }

private:
    inline void log_time_by_epoch() {
        uint64_t total[exec_epoch];
        for (unsigned i = 0; i < exec_epoch; i++) {
            total[i] = 0;
        }
        for (unsigned i = 0; i < start_timestamps.size(); i++) {
//            write_log(seastar::format(
//                    "param group {} of epoch {} exec time(us) : {}\n",
//                    i / exec_epoch, i % exec_epoch, end_timestamps[i] - start_timestamps[i]));
            total[i % exec_epoch] += (end_timestamps[i] - start_timestamps[i]);
        }
        double max = 0.0;
        double min = 10000000.0;
        double total_t = 0.0;
        for (unsigned i = exec_epoch - 8; i < exec_epoch; i++) {
            double t = (static_cast<double>(total[i]) / 1000) / param_num;
            write_log(seastar::format(
                    "[CQ {}] [Epoch {}] Average exec time(ms) : {}\n", CQ_ID, i, t));
            total_t += t;
            if (t > max) { max = t; }
            if (t < min) { min = t; }
        }
        write_log(seastar::format("CQ_{} Min epoch exec time(ms) : {}\n", CQ_ID, min));
        write_log(seastar::format("CQ_{} Max epoch exec time(ms) : {}\n", CQ_ID, max));
        write_log(seastar::format("CQ_{} Average epoch exec time(ms) : {}\n", CQ_ID, total_t / 8));
    }

    inline void log_time_by_param() {
        uint64_t total[param_num];
        for (unsigned i = 0; i < param_num; i++) {
            total[i] = 0;
        }
        for (unsigned i = 0; i < param_num; i++) {
            for (unsigned j = 0; j < exec_epoch; j++) {
                write_log(seastar::format(
                        "param group {} of epoch {} exec time(us) : {}\n",
                        i, j, end_timestamps[i * exec_epoch + j] - start_timestamps[i * exec_epoch + j]));
                if (j != 0) {
                    total[i] += (end_timestamps[i * exec_epoch + j] - start_timestamps[i * exec_epoch + j]);
                }
            }
        }
        if (exec_epoch == 1) {
            return;
        }
        for (unsigned i = 0; i < param_num; i++) {
            double total_time = static_cast<double>(total[i]) / 1000;
            write_log(seastar::format(
                    "[CQ {}]\t[Param Group {}{}]\tAverage exec time(ms) : {}\n",
                    CQ_ID, i < 10 ? "0" : "", i, total_time / (exec_epoch - 1)));
        }
    }
};

class cq1_query_helper : public cq_basic_helper<1> {
    std::vector<int64_t> start_person_ids{};
public:
    cq1_query_helper() = default;
    void load_cq_params();

    inline int64_t get_start_person(unsigned q_id) const {
        return start_person_ids[((q_id - 1) / cq_num) / exec_epoch];
    }
};
extern cq1_query_helper cq1_helper;

class cq2_query_helper : public cq_basic_helper<2> {
    std::vector<int64_t> start_person_ids{};
    std::vector<int64_t> cur_organisation_ids{};
public:
    cq2_query_helper() = default;
    void load_cq_params();

    inline int64_t get_start_person(unsigned q_id) const {
        return start_person_ids[((q_id - 1) / cq_num) / exec_epoch];
    }

    inline void set_organisation_ids(const std::vector<int64_t>& companies) {
        cur_organisation_ids = companies;
    }
    inline void set_organisation_ids(VertexBatch&& companies) {
        cur_organisation_ids.clear();
        cur_organisation_ids.reserve(companies.size());
        for (unsigned i = 0; i < companies.size(); i++) {
            cur_organisation_ids.push_back(companies[i]);
        }
    }
    inline const std::vector<int64_t>& get_organisation_ids() const {
        return cur_organisation_ids;
    }
};
extern cq2_query_helper cq2_helper;

class cq3_query_helper : public cq_basic_helper<3> {
    std::vector<int64_t> start_person_ids{};
public:
    cq3_query_helper() = default;
    void load_cq_params();

    inline int64_t get_start_person(unsigned q_id) const {
        return start_person_ids[((q_id - 1) / cq_num) / exec_epoch];
    }
};
extern cq3_query_helper cq3_helper;

class cq4_query_helper : public cq_basic_helper<4> {
    std::vector<int64_t> start_person_ids{};
    std::vector<int64_t> cur_organisation_ids{};
public:
    cq4_query_helper() = default;
    void load_cq_params();

    inline int64_t get_start_person(unsigned q_id) const {
        return start_person_ids[((q_id - 1) / cq_num) / exec_epoch];
    }

    inline void set_organisation_ids(const std::vector<int64_t>& companies) {
        cur_organisation_ids = companies;
    }
    inline void set_organisation_ids(VertexBatch&& companies) {
        cur_organisation_ids.clear();
        cur_organisation_ids.reserve(companies.size());
        for (unsigned i = 0; i < companies.size(); i++) {
            cur_organisation_ids.push_back(companies[i]);
        }
    }
    inline const std::vector<int64_t>& get_organisation_ids() const {
        return cur_organisation_ids;
    }
};
extern cq4_query_helper cq4_helper;

class cq5_query_helper : public cq_basic_helper<5> {
    std::vector<int64_t> start_person_ids{};
    std::vector<int64_t> cur_organisation_ids{};
public:
    cq5_query_helper() = default;
    void load_cq_params();

    inline int64_t get_start_person(unsigned q_id) const {
        return start_person_ids[((q_id - 1) / cq_num) / exec_epoch];
    }

    inline void set_organisation_ids(const std::vector<int64_t>& companies) {
        cur_organisation_ids = companies;
    }
    inline void set_organisation_ids(VertexBatch&& companies) {
        cur_organisation_ids.clear();
        cur_organisation_ids.reserve(companies.size());
        for (unsigned i = 0; i < companies.size(); i++) {
            cur_organisation_ids.push_back(companies[i]);
        }
    }
    inline const std::vector<int64_t>& get_organisation_ids() const {
        return cur_organisation_ids;
    }
};
extern cq5_query_helper cq5_helper;

class cq6_query_helper : public cq_basic_helper<6> {
    std::vector<int64_t> start_person_ids{};
public:
    cq6_query_helper() = default;
    void load_cq_params();

    inline int64_t get_start_person(unsigned q_id) const {
        return start_person_ids[((q_id - 1) / cq_num) / exec_epoch];
    }
};
extern cq6_query_helper cq6_helper;