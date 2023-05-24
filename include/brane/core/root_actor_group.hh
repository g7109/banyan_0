#pragma once

#include <cstddef>
#include <climits>
#include <brane/actor/actor_core.hh>
#include <brane/actor/actor_message.hh>
#include <brane/core/promise_manager.hh>

#include "seastar/core/semaphore.hh"

namespace brane {

class actor_message_queue;

// Semaphore count type is ssize_t.
const size_t GMaxActiveChildNum = SSIZE_MAX;
const size_t GInitTwoWayPromiseNum = 100;

class root_actor_group final : public actor_base {
    uint32_t _num_actors_managed = 0;
    std::unordered_map<uint64_t, actor_base*> _actor_group_map;
    pr_manager<actor_message*> _response_pr_manager{GInitTwoWayPromiseNum};
    seastar::semaphore _actor_activation_sem{GMaxActiveChildNum};
public:
    seastar::future<> send(actor_message* msg);
    void exit(bool force=false);
    void cancel_query(uint32_t query_id, uint16_t query_actor_group_type);
    /// temporarily used in gqs
    seastar::future<> cancel_query_request(uint32_t query_id, uint16_t query_actor_group_type);
    void cancel_scope(const scope_builder& scope);
    void clear_all();

    explicit root_actor_group();
    ~root_actor_group() override;
private:
    seastar::future<> stopped_actor_enqueue(actor_message* msg);
    void run_and_dispose() noexcept override {}
    void stop_child_actor(byte_t *child_addr, bool force) override;
    void notify_child_stopped() override;
    void stop_actor_system(bool force);
    void wait_others_and_exit();
    seastar::future<> receive(actor_message* msg);

    friend class actor_message_queue;
    friend struct actor_client_helper;
};

extern __thread root_actor_group *local_actor_engine;

inline root_actor_group& actor_engine() {
    return *local_actor_engine;
}

} // namespace brane
