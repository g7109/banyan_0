#pragma once

#include <fstream>

extern std::ofstream log_output;

void initialize_log_env(const std::string& log_file_path);
void finalize_log_env();

void write_log(const char* output_str);
void write_log(const std::string& output_str);
