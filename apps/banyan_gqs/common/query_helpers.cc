#include <cstring>
#include <fstream>
#include "common/query_helpers.hh"

query1_helper q1_helper{};
query2_helper q2_helper{};
query3_helper q3_helper{};
query4_helper q4_helper{};
query5_helper q5_helper{};
query6_helper q6_helper{};
query7_helper q7_helper{};
query8_helper q8_helper{};
query9_helper q9_helper{};
query10_helper q10_helper{};
query11_helper q11_helper{};
query12_helper q12_helper{};

evaluation1_helper e1_helper{};
evaluation3_helper e3_helper{};
evaluation4_helper e4_helper{};
evaluation5_helper e5_helper{};

cq1_query_helper cq1_helper{};
cq2_query_helper cq2_helper{};
cq3_query_helper cq3_helper{};
cq4_query_helper cq4_helper{};
cq5_query_helper cq5_helper{};
cq6_query_helper cq6_helper{};

std::vector<std::string> split(const std::string& s, const char* delim = "|") {
    std::vector<std::string> sv;
    char* buffer = new char[s.size() + 1];
    buffer[s.size()] = '\0';
    std::copy(s.begin(), s.end(), buffer);
    char* p = std::strtok(buffer, delim);
    do {
        sv.emplace_back(p);
    } while ((p = std::strtok(nullptr, delim)));
    delete[] buffer;
    return sv;
}

std::string param_file_of_ldbc_query(unsigned query_id) {
    std::string path = std::string(banyan_gqs_dir) + "/ldbc_interactive_benchmark_params/" + dataset
        + "/interactive_" + std::to_string(query_id) + "_param.txt";
    return path;
}

void query1_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(1));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    std::string f_name;
    for(unsigned i = 0; i < param_num; i++) {
        getline(in_file, line);
        auto values = split(line, " ");
        p_id = std::stol(values[0]);
        f_name = line.substr(values[0].size() + 1);
        start_person_ids.push_back(p_id);
        first_names.push_back(std::move(f_name));
    }
    in_file.close();
}

void query2_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(2));
    std::string param_name1, param_name2;
    in_file >> param_name1 >> param_name2;

    int64_t p_id, max_date;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> max_date;
        start_person_ids.push_back(p_id);
        max_dates.push_back(max_date);
    }
    in_file.close();
}

void query3_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(3));
    std::string param_name1, param_name2, param_name3, param_name4, param_name5;
    in_file >> param_name1 >> param_name2 >> param_name3 >> param_name4 >> param_name5;

    int64_t p_id, start_date, end_date;
    std::string country_x, country_y;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> start_date >> end_date >> country_x >> country_y;
        start_person_ids.push_back(p_id);
        start_dates.push_back(start_date);
        end_dates.push_back(end_date);
        country_x_names.push_back(std::move(country_x));
        country_y_names.push_back(std::move(country_y));
    }
    in_file.close();
}

void query4_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(4));
    std::string param_name1, param_name2, param_name3;
    in_file >> param_name1 >> param_name2 >> param_name3;

    int64_t p_id, start_date, end_date;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> start_date >> end_date;
        start_person_ids.push_back(p_id);
        start_dates.push_back(start_date);
        end_dates.push_back(end_date);
    }
    in_file.close();
}

void query5_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(5));
    std::string param_name1, param_name2;
    in_file >> param_name1 >> param_name2;

    int64_t p_id, min_date;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> min_date;
        start_person_ids.push_back(p_id);
        min_dates.push_back(min_date);
    }
    in_file.close();
}

// thread_local std::string query6_helper::filter_tag_name{};

void query6_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(6));
    std::string param_name1, param_name2;
    in_file >> param_name1 >> param_name2;

    int64_t p_id;
    std::string tag_name;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> tag_name;
        start_person_ids.push_back(p_id);
        tag_names.push_back(std::move(tag_name));
    }
    in_file.close();
}

void query7_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(7));
    std::string param_name1;
    in_file >> param_name1;

    int64_t p_id;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void query8_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(8));
    std::string param_name1;
    in_file >> param_name1;

    int64_t p_id;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void query9_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(9));
    std::string param_name1, param_name2;
    in_file >> param_name1 >> param_name2;

    int64_t p_id, max_date;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> max_date;
        start_person_ids.push_back(p_id);
        max_dates.push_back(max_date);
    }
    in_file.close();
}

void query10_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(10));
    std::string param_name1, param_name2;
    in_file >> param_name1 >> param_name2;

    int64_t p_id, month;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> month;
        start_person_ids.push_back(p_id);
        months.push_back(month);
    }
    in_file.close();
}

void query11_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(11));
    std::string param_name1, param_name2, param_name3;
    in_file >> param_name1 >> param_name2 >> param_name3;

    int64_t p_id, work_year;
    std::string country;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> country >> work_year;
        start_person_ids.push_back(p_id);
        country_names.push_back(std::move(country));
        work_from_years.push_back(work_year);
    }
    in_file.close();
}

void query12_helper::load_benchmark_params() {
    std::ifstream in_file(param_file_of_ldbc_query(12));
    std::string param_name1, param_name2;
    in_file >> param_name1 >> param_name2;

    int64_t p_id;
    std::string tag_class;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id >> tag_class;
        start_person_ids.push_back(p_id);
        tag_class_names.push_back(std::move(tag_class));
    }
    in_file.close();
}

/// ---

std::string param_file_of_cq(unsigned query_id) {
    std::string path = std::string(banyan_gqs_dir) + "/cq_interactive_params/" + dataset
            + "/cq_" + std::to_string(query_id) + "_param.txt";
    return path;
}

void cq1_query_helper::load_cq_params() {
    std::ifstream in_file(param_file_of_cq(1));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    for (unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void cq2_query_helper::load_cq_params() {
    std::ifstream in_file(param_file_of_cq(2));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void cq3_query_helper::load_cq_params() {
    std::ifstream in_file(param_file_of_cq(3));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    for (unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void cq4_query_helper::load_cq_params() {
    std::ifstream in_file(param_file_of_cq(4));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    for (unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void cq5_query_helper::load_cq_params() {
    std::ifstream in_file(param_file_of_cq(5));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    for(unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}

void cq6_query_helper::load_cq_params() {
    std::ifstream in_file(param_file_of_cq(6));
    std::string line;
    getline(in_file, line);

    int64_t p_id;
    for (unsigned i = 0; i < param_num; i++) {
        in_file >> p_id;
        start_person_ids.push_back(p_id);
    }
    in_file.close();
}
