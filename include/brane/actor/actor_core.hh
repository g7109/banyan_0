/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <brane/actor/actor_exec_statistics.hh>
#include <brane/actor/actor_message.hh>
#include <brane/actor/reference_base.hh>
#include <algorithm>

#include "seastar/core/task.hh"
#include "seastar/core/circular_buffer.hh"
#include "seastar/util/bool_class.hh"

namespace brane {

enum schedule_status : uint8_t {
    INITIALIZED,
    SCHEDULABLE,
    SCHEDULED
};

enum actor_status : uint8_t {
    RUNNING,
    PEACE_STOPPING,
    FORCE_STOPPING
};

struct stop_reaction_tag {};
using stop_reaction = seastar::bool_class<stop_reaction_tag>;

class actor_factory;

class actor_base : public seastar::task {
protected:
    clock_unit _unused_quota;
    std::chrono::steady_clock::time_point _stop_time;
    schedule_status _sched_status = schedule_status::INITIALIZED;
    actor_status _stop_status = actor_status::RUNNING;
    uint8_t _total_addr_length;
    byte_t _address[GLocalActorAddrLength];
    seastar::circular_buffer<actor_message*> _mailbox;     // NOT thread-safe queue

    /// temporarily used in gqs
    // seastar::circular_buffer<std::unique_ptr<task>> _task_queue;
    std::vector<std::unique_ptr<task>> _task_queue;
    bool if_new_task_enqueued = false;
    uint32_t _cur_task_id = 0;

    void activate();
    actor_message* deque_message();
    bool need_yield();
    void reset_timer();
    void advance_timer();
    void log_exception(const char* log, std::exception_ptr eptr);
    void clean_mailbox();
    ~actor_base() override;
    virtual void initialize() {}
    virtual void finalize() {}

public:
    explicit actor_base(actor_base *exec_ctx, const byte_t *addr);
    explicit actor_base(actor_base *exec_ctx);

    seastar::future<> enque_message(actor_message* msg);
    seastar::future<> enque_urgent_message(actor_message* msg);
    void add_task(std::unique_ptr<seastar::task>&& t);
    void add_urgent_task(std::unique_ptr<seastar::task>&& t);

    scope_builder get_scope();
    uint8_t get_address_length() const { return _total_addr_length; }
    virtual actor_base* get_actor_local(actor_message::header &hdr);
    virtual void stop_child_actor(byte_t *child_addr, bool force) {};
    virtual void notify_child_stopped() {};
    virtual void stop(bool force) {};

    bool activatable();
    void set_activatable();
    void schedule();

    bool stopping();
    bool force_stopping();

    /// Broadcast message to the specified address in all shards
    void broadcast(actor_message::header &hdr);
    template <typename D>
    void broadcast(actor_message::header &hdr, D&& data);

    friend class actor_factory;
};

inline
bool actor_base::activatable() {
    return _sched_status == schedule_status::SCHEDULABLE;
}

inline
void actor_base::set_activatable() {
    _sched_status = schedule_status::SCHEDULABLE;
}

inline
void actor_base::schedule() {
    assert(_sched_status == schedule_status::INITIALIZED);
    set_activatable();
    activate();
}

inline
bool actor_base::stopping() {
    return _stop_status != actor_status::RUNNING;
}

inline
bool actor_base::force_stopping() {
    return _stop_status == actor_status::FORCE_STOPPING;
}

} // namespace brane
