#pragma once

#include <cstdint>
#include <brane/actor/actor_message.hh>
#include <brane/core/dynamic-queue.hh>
#include "seastar/net/socket_defs.hh"

namespace brane {

struct worker_node_info {
    const uint32_t machine_id;
    const uint32_t nr_cores;
    const seastar::socket_address addr;

    worker_node_info(uint32_t machine_id, uint32_t nr_cores, seastar::socket_address &&addr)
        : machine_id(machine_id), nr_cores(nr_cores), addr(std::move(addr)) {}
};

struct message_item {
    uint32_t mach_id;
    actor_message *msg;
    message_item() = default;
    message_item(uint32_t mid, actor_message *msg)
        : mach_id(mid), msg(msg) {}
};

using net_buffer = dynamic_queue<actor_message *>;

} // namespace brane
