/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <seastar/core/app-template.hh>

namespace brane {

class actor_system {
public:
    explicit actor_system(seastar::app_template::config cfg = seastar::app_template::config())
        : _app(std::move(cfg)) {}

    int run(int ac, char **av, std::function<void()> &&func);
private:
    seastar::future<> configure();
    seastar::future<> start();
    void set_network_config(uint32_t this_mid, uint32_t conn_count);

    seastar::app_template _app;
};

} // namespace brane
