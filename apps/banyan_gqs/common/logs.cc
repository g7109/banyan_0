#include "common/logs.hh"

std::ofstream log_output;

void initialize_log_env(const std::string& log_file_path) {
    if (log_output.is_open()) {
        log_output.close();
    }
    log_output.open(log_file_path);
}
void finalize_log_env() {
    log_output.close();
}

void write_log(const char* output_str) {
    log_output << output_str;
}

void write_log(const std::string& output_str) {
    log_output << output_str;
}