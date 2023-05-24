/**
 * DAIL in Alibaba Group
 *
 */

#include <brane/actor/actor_core.hh>
#include <brane/core/local_channel.hh>
#include <brane/core/message_reclaimer.hh>
#include <brane/core/root_actor_group.hh>
#include "seastar/core/print.hh"
#include "seastar/core/reactor.hh"

namespace brane {

actor_base::actor_base(actor_base *exec_ctx, const byte_t *addr) : seastar::task(exec_ctx)
{
    _total_addr_length = exec_ctx->get_address_length() + GLocalActorAddrLength;
    memcpy(_address, addr, GLocalActorAddrLength);
}

/// This constructor is only used by the root actor of each shard
actor_base::actor_base(actor_base *exec_ctx) : seastar::task(exec_ctx)
{
    _total_addr_length = GShardIdInBytes;
}

actor_base::~actor_base() {
    _mailbox.for_each([] (actor_message* msg) { delete msg; });
}

seastar::future<> actor_base::enque_message(actor_message* message) {
    _mailbox.push_back(message);
    activate();
    return seastar::make_ready_future<>();
}

seastar::future<> actor_base::enque_urgent_message(actor_message* message) {
    _mailbox.push_front(message);
    activate();
    return seastar::make_ready_future<>();
}

actor_message* actor_base::deque_message() {
    auto message = _mailbox.front();
    _mailbox.pop_front();
    return message;
}

void actor_base::add_task(std::unique_ptr<seastar::task> &&t) {
    if (_exec_ctx == nullptr) {
        seastar::engine().add_task(std::move(t));
    }
    else {
        t->set_task_id(_cur_task_id++);
        _task_queue.push_back(std::move(t));
        if_new_task_enqueued = true;
        activate();
    }
}

void actor_base::add_urgent_task(std::unique_ptr<seastar::task> &&t) {
    if (_exec_ctx == nullptr) {
        seastar::engine().add_urgent_task(std::move(t));
    }
    else {
        t->set_task_id(_cur_task_id++);
        _task_queue.push_back(std::move(t));
        if_new_task_enqueued = true;
        activate();
    }
}

void actor_base::activate() {
    if (activatable()) {
        _sched_status = schedule_status::SCHEDULED;
        if (_exec_ctx == nullptr)
            seastar::engine().add_task(std::unique_ptr<seastar::task>(this));
        else
            _exec_ctx->add_task(std::unique_ptr<seastar::task>(this));
    }
}

void actor_base::reset_timer() {
    _unused_quota = read_actor_quota();
    _stop_time = read_actor_clock() + _unused_quota;
}

void actor_base::advance_timer() {
    advance_actor_clock();
}

bool actor_base::need_yield() {
    if (seastar::need_preempt()) { return true; }
    _unused_quota = std::chrono::duration_cast<clock_unit>(_stop_time - read_actor_clock());
    return _unused_quota.count() <= 0;
}

void actor_base::log_exception(const char* log, std::exception_ptr eptr) {
    const char* s;
    try {
        std::rethrow_exception(eptr);
    } catch (std::exception& ex) {
        s = ex.what();
    } catch (...) {
        s = "unknown exception";
    }
    std::cerr << seastar::format("{}: {}", log, s) << std::endl;
}

scope_builder actor_base::get_scope() {
    scope_builder scope(global_shard_id());
    auto layer_num = ((_total_addr_length - GShardIdInBytes - GLocalActorAddrLength) / GLocalActorAddrLength);
    auto* actor_g = get_execution_context();
    for(auto i = layer_num; i > 0; i--) {
        memcpy(scope.addr.data + GShardIdInBytes + (i - 1) * GLocalActorAddrLength,
                &(actor_g->_address), GLocalActorAddrLength);
        actor_g = actor_g->get_execution_context();
    }
    scope.addr.length = (_total_addr_length - GLocalActorAddrLength);
    return scope;
}

actor_base* actor_base::get_actor_local(actor_message::header &hdr) {
    //TODO[DAIL] consider the case that the destination address is longer then this actor's address
    return this;
}

template <typename D>
void actor_base::broadcast(actor_message::header &hdr, D&& data) {
    static_assert(std::is_rvalue_reference<D&&>::value, "T&& should be rvalue reference.");
    unsigned i = 0;
    for (; i < global_shard_count() - 1; ++i) {
        auto *msg = brane::make_one_way_request_message(hdr.addr, hdr.behavior_tid, data.share(), hdr.src_shard_id);
        actor_engine().send(msg);
    }
    auto *msg = brane::make_one_way_request_message(hdr.addr, hdr.behavior_tid, std::forward<D>(data), hdr.src_shard_id);
    actor_engine().send(msg);
}

void actor_base::broadcast(actor_message::header &hdr) {
    for (unsigned i = 0; i < global_shard_count(); ++i) {
        auto *msg = brane::make_one_way_request_message(hdr.addr, hdr.behavior_tid, hdr.src_shard_id, hdr.m_type);
        actor_engine().send(msg);
    }
}

void actor_base::clean_mailbox() {
    while (!_mailbox.empty()) {
        actor_message* msg = deque_message();
        reclaim_actor_message(msg);
    }
}

} // namespace brane
