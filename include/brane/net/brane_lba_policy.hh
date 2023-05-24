#pragma once

#include <brane/net/customized_lba.hh>
#include <brane/net/network_config.hh>

namespace brane {

struct brane_lba_policy : public lba_policy {
    brane_lba_policy() = default;
    ~brane_lba_policy() override {}
    unsigned get_cpu(uint32_t addr, uint16_t port) override {
        return network_config::get().get_client_target_shard(addr);
    }
};

} // namespace brane
