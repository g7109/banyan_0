/**
 * DAIL in Alibaba Group
 *
 */

#include <brane/actor/actor_implementation.hh>
#include <brane/actor/actor_message.hh>
#include <brane/core/local_channel.hh>
#include <brane/core/message_reclaimer.hh>
#include <brane/actor/actor_factory.hh>

#include "seastar/core/reactor.hh"
#include "seastar/core/print.hh"

namespace brane {

/// Recursively looking for the destination actor for the incoming message;
/// If the destination actor is disabled, return nullptr;
/// If the destination actor or its ancester not exists, create an unactivatable actor
template<class ComparatorType>
actor_base* actor_group<ComparatorType>::get_actor_local(actor_message::header &hdr) {
    auto &addr = hdr.addr;
    void* child_ptr = nullptr;
    const byte_t* child_radix_addr = &addr.data[_total_addr_length];
    auto child_radix_addr_length = addr.length - _total_addr_length;
    int search = _actor_tree.lookup(child_radix_addr, &child_ptr, child_radix_addr_length);
    switch (search) {
    case 2:     // prefix match, an actor whose address is a prefix of the lookup key is found
        return reinterpret_cast<actor_base *>(child_ptr)->get_actor_local(hdr);
    case 1 :    // exact match, the destination actor is found
        return reinterpret_cast<actor_base *>(child_ptr);
    case 0 :    // actor not exists
    {
        uint16_t actor_type = load_unaligned_int<uint16_t>(child_radix_addr);
        actor_base* child = actor_factory::get().create(actor_type, this, child_radix_addr);
        auto child_priority = load_unaligned_int<uint32_t>(addr.data + _total_addr_length + GActorTypeInBytes);
        child->set_priority(child_priority);
        _actor_tree.insert(child_radix_addr, child, GLocalActorAddrLength);
        ++_num_actors_managed;

        auto sem_f = _actor_activation_sem.wait(1);
        if (sem_f.available()) {
            child->schedule();
        }
        else {
            auto activate_actor_func = [child](const seastar::future_state<>&& state) {
                child->schedule();
            };
            using continuationized_func = seastar::continuation<
                std::function<void(const seastar::future_state<>&&)>>;
            seastar::internal::set_callback(sem_f, 
                std::make_unique<continuationized_func>(activate_actor_func, this));
        }

        if (addr.length == child->get_address_length()) {
            return child;
        }
        else {
            return child->get_actor_local(hdr);
        }
    }
    case -1 :   // actor is disabled
        return nullptr;
    default :
        assert(false);
        return this;
    }
}

template<class ComparatorType>
void actor_group<ComparatorType>::process_message(actor_message *msg) {
    switch (msg->hdr.m_type) {
    case message_type::PEACE_STOP : {
        _exec_ctx->stop_child_actor(_address, false);
        break;
    }
    case message_type::FORCE_STOP : {
        _exec_ctx->stop_child_actor(_address, true);
        /// temporarily used in gqs
        _cancel_src_sid = msg->hdr.src_shard_id;
        _cancel_pr_id = msg->hdr.pr_id;
        break;
    }
    default :
        assert(false);
        break;
    }
    reclaim_actor_message(msg);
}

/// For default schedule(). heap_sort() for circular buffer.
void heapify_helper(seastar::circular_buffer<std::unique_ptr<seastar::task>>& buf, int n, int i) {
    int smallest = i;
    int l = 2 * i + 1;
    int r = 2 * i + 2;
    if (l < n && buf[l]->get_priority() < buf[smallest]->get_priority()) {
        smallest = l;
    }
    if (r < n && buf[r]->get_priority() < buf[smallest]->get_priority()) {
        smallest = r;
    }
    if (smallest != i) {
        buf[i].swap(buf[smallest]);
        return heapify_helper(buf, n, smallest);
    }
}

/// For default schedule(). heap_sort() for circular buffer.
void heap_sort(seastar::circular_buffer<std::unique_ptr<seastar::task>>& buf) {
    int size = static_cast<int>(buf.size());
    for (int i = size / 2 - 1; i >= 0; i--) {
        heapify_helper(buf, size, i);
    }
    for (int i = size - 1; i >= 0; i--) {
        buf[0].swap(buf[i]);
        heapify_helper(buf, i, 0);
    }
}

/// Sort tasks in _task_queue in descending order according to their priority
template<class ComparatorType>
void actor_group<ComparatorType>::schedule_task() {
    // heap_sort(_task_queue);
}

template<class ComparatorType>
void actor_group<ComparatorType>::run_and_dispose() noexcept {
    reset_timer();

    if (force_stopping()) {
        for(auto& task : _task_queue) {
            task->cancel();
        }
    }

    while (!_mailbox.empty()) {
        auto msg = deque_message();
        process_message(msg);
        advance_timer();
        if (need_yield()) { goto FINAL; }
    }

    while (!_task_queue.empty())  {
        if (if_new_task_enqueued) {
            std::make_heap(_task_queue.begin(), _task_queue.end(), _comparator);
            if_new_task_enqueued = false;
        }
        pop_heap(_task_queue.begin(), _task_queue.end(), _comparator);
        auto actor = std::move(_task_queue.back());
        _task_queue.pop_back();
        set_actor_quota(_unused_quota);
        actor->run_and_dispose();
        actor.release();
        if (need_yield()) { break; }
    }

FINAL:
    if (stopping() && !_num_actors_managed && _task_queue.empty() && _mailbox.empty()) {
        _exec_ctx->notify_child_stopped();
        ///temporarily used in gqs
        if (_cancel_src_sid != UINT32_MAX) {
            auto *response_msg = brane::make_response_message(
                    _cancel_src_sid, _cancel_pr_id, brane::message_type::RESPONSE);
            brane::actor_engine().send(response_msg);
        }
        delete this;
        return;
    }

    set_activatable();
    if (!_mailbox.empty() || !_task_queue.empty()) {
        activate();
    }
}

/**
 * Child actor (actor group) calls this function directly to notify its termination
 * @param child_addr: pointer to the address of child actor in the format of byte array
 */
template<class ComparatorType>
void actor_group<ComparatorType>::stop_child_actor(byte_t *child_addr, bool force) {
    void* child_actor_ptr = nullptr;
    auto search = _actor_tree.lookup(child_addr, &child_actor_ptr, GLocalActorAddrLength);
    if (search == -1) { return; }   // actor disabled
    assert(search  == 1);  // only actor existing in the tree calls this function
    _actor_tree.disable(child_addr, GLocalActorAddrLength);
    auto child = reinterpret_cast<actor_base*>(child_actor_ptr);
    child->stop(force);
}

/**
 * Child actor (actor group) calls this function to notify it has been terminated
 */
template<class ComparatorType>
void actor_group<ComparatorType>::notify_child_stopped() {
    --_num_actors_managed;
    _actor_activation_sem.signal(1);
}

template<class ComparatorType>
void actor_group<ComparatorType>::stop_all_children(cancel_func func) {
    // only non-disabled actor would apply this function.
    _actor_tree.iter_prefix_apply(_address, 0, func, this);
    _actor_tree.disable(_address, 0);
}

template<class ComparatorType>
void actor_group<ComparatorType>::stop(bool force) {
    if (stopping()) { return; }
    if (force) {
        _stop_status = actor_status::FORCE_STOPPING;
        if (_actor_activation_sem.waiters() != 0) { _actor_activation_sem.broken(); };
        // iterate and force stop all children in this actor group
        cancel_func func = [] (void* data, void* target) mutable {
            reinterpret_cast<actor_base *>(target)->stop(true);
        };
        stop_all_children(func);
        clean_mailbox();
    }
    else {
        _stop_status = actor_status::PEACE_STOPPING;
        // iterate and peace stop all children in this actor group
        cancel_func func = [] (void* data, void* target) mutable {
            reinterpret_cast<actor_base *>(target)->stop(false);
        };
        stop_all_children(func);
    }
    if (!_num_actors_managed && activatable()) {
        activate();
    }
}

void stateless_actor::stop(bool force) {
    if (stopping()) { return; }
    if (force) {
        _stop_status = actor_status::FORCE_STOPPING;
        clean_mailbox();
        clean_task_queue();
    }
    else {
        _stop_status = actor_status::PEACE_STOPPING;
    }
    if (activatable() /** not in the task queue */ && !_cur_concurrency /** no flying chain */) {
        activate();
    }
}


void stateless_actor::run_and_dispose() noexcept {
    reset_timer();

    if (force_stopping()) {
        for(auto& task : _task_queue) {
            task->cancel();
        }
    }

    while (!_task_queue.empty()) {
        auto task = std::move(_task_queue.back());
        _task_queue.pop_back();
        task->run_and_dispose();
        task.release();
        advance_timer();
        if (need_yield()) { goto FINAL; }
    }

    if (_cur_concurrency == _max_concurrency)  {  // reaches the concurrency limit
        set_activatable();
        return;
    }

    while (!_mailbox.empty()) {
        actor_message* msg = deque_message();
        switch (msg->hdr.m_type) {
        case message_type::USER: {
            auto result_f = do_work(msg);
            if (!result_f.available()) {
                ++_cur_concurrency;
                auto post_work_func = [this, msg](const seastar::future_state<stop_reaction> &state) {
                    --_cur_concurrency;
                    reclaim_actor_message(msg);
                    if (state.failed()) {
                        log_exception(
                                seastar::format("Stateless actor {} got exception with {} task chains flying in system",
                                       char_arr_to_str(_address, GLocalActorAddrLength), _cur_concurrency).c_str(),
                                state.get_exception());
                    }
                    else if (std::get<0>(state.get()) == stop_reaction::yes) {
						_exec_ctx->stop_child_actor(_address, true);
                    }
                };
                using continuationized_func = seastar::continuation<
                    std::function<void (seastar::future_state<stop_reaction>)>, stop_reaction>;
                seastar::internal::set_callback(result_f, 
                    std::make_unique<continuationized_func> (post_work_func, this));
                if (_cur_concurrency == _max_concurrency) { goto FINAL; }
                break;
            }
            // do_work() completes without blocking
            reclaim_actor_message(msg);
            if (result_f.failed()) {
                log_exception(
                        seastar::format("Stateless actor {} got exception with {} task chains flying in system",
                               char_arr_to_str(_address, GLocalActorAddrLength), _cur_concurrency).c_str(),
                        result_f.get_exception());
            }
            else if (result_f.get0() == stop_reaction::yes) {
				_exec_ctx->stop_child_actor(_address, true);
            }
            break;
        }
        case message_type::PEACE_STOP : {
            _exec_ctx->stop_child_actor(_address, false);
            reclaim_actor_message(msg);
            break;
        }
        case message_type::FORCE_STOP : {
            _exec_ctx->stop_child_actor(_address, true);
            reclaim_actor_message(msg);
            break;
        }
        default:
            assert(false);
            reclaim_actor_message(msg);
        }
        advance_timer();
        if (need_yield()) { break; }
    }

FINAL:
    if (stopping() && !_cur_concurrency && _mailbox.empty()) {
        _exec_ctx->notify_child_stopped();
        this->finalize();
        delete this;
        return;
    }

    set_activatable();
    if (!_task_queue.empty() || (!_mailbox.empty() && _cur_concurrency < _max_concurrency)) {
        activate();
    }
}

void stateless_actor::clean_task_queue() {
    assert(_mailbox.empty() && force_stopping());
    for(auto& task : _task_queue) {
        task->cancel();
    }
    while (!_task_queue.empty()) {
        auto task = std::move(_task_queue.back());
        _task_queue.pop_back();
        task->run_and_dispose();
        task.release();
    }
}

void stateful_actor::run_and_dispose() noexcept {
    reset_timer();

    if (force_stopping()) {
        for(auto& task : _task_queue) {
            task->cancel();
        }
    }

    while (!_task_queue.empty()) {
        auto task = std::move(_task_queue.back());
        _task_queue.pop_back();
        task->run_and_dispose();
        task.release();
        advance_timer();
        if (need_yield()) { goto FINAL; }
    }

    // Not re-entrant
    if (busy) {
        set_activatable();
        return;
    }

    while (!_mailbox.empty()) {
        actor_message* msg = deque_message();
        switch (msg->hdr.m_type) {
        case message_type::USER: {
            auto result_f = do_work(msg);
            // Async operation, result is not available immediately
            if (!result_f.available()) {
                busy = true;
                auto post_work_func = [this, msg](const seastar::future_state<stop_reaction> &state) {
                    busy = false;
                    reclaim_actor_message(msg);
                    if (state.failed()) {
                        log_exception(seastar::format("Stateful actor {} got exception ", char_arr_to_str(_address, GLocalActorAddrLength)).c_str(), state.get_exception());
                    }
                    else if (std::get<0>(state.get()) == stop_reaction::yes) {
						_exec_ctx->stop_child_actor(_address, true);
                    }
                };
                using continuationized_func = seastar::continuation<
                    std::function<void (seastar::future_state<stop_reaction>)>, stop_reaction>;
                seastar::internal::set_callback(result_f, 
                    std::make_unique<continuationized_func> (post_work_func, this));
                goto FINAL;
            }

            reclaim_actor_message(msg);
            if (result_f.failed()) {
                log_exception(seastar::format("Stateful actor {} got exception ", char_arr_to_str(_address, GLocalActorAddrLength)).c_str(), result_f.get_exception());
            }
            else if (result_f.get0() == stop_reaction::yes) {
				_exec_ctx->stop_child_actor(_address, true);
            }
            break;
        }
        case message_type::PEACE_STOP : {
            _exec_ctx->stop_child_actor(_address, false);
            reclaim_actor_message(msg);
            break;
        }
        case message_type::FORCE_STOP : {
            _exec_ctx->stop_child_actor(_address, true);
            reclaim_actor_message(msg);
            break;
        }
        default:
            assert(false);
            reclaim_actor_message(msg);
        }
        advance_timer();
        if (need_yield()) { break; }
    }

FINAL:
    if (stopping() && !busy && _mailbox.empty()) {
        _exec_ctx->notify_child_stopped();
        this->finalize();
        delete this;
        return;
    }

    set_activatable();
    if (!_task_queue.empty() || (!busy && !_mailbox.empty())) {
        activate();
    }
}

void stateful_actor::stop(bool force) {
    if (stopping()) { return; }
    if (force) {
        _stop_status = actor_status::FORCE_STOPPING;
        clean_mailbox();
        clean_task_queue();
    }
    else {
        _stop_status = actor_status::PEACE_STOPPING;
    }
    // if actor is not in task queue and there is no flying task
    if (activatable() && !busy) {
        activate();
    }
}

void stateful_actor::clean_task_queue() {
    assert(_mailbox.empty() && force_stopping());
    for(auto& task : _task_queue) {
        task->cancel();
    }
    while (!_task_queue.empty()) {
        auto task = std::move(_task_queue.back());
        _task_queue.pop_back();
        task->run_and_dispose();
        task.release();
    }
}

/// Register actor group in actor factory
registration::actor_registration<actor_group<fifo_cmp>> _actor_group_registeration(GActorGroupTypeId);
registration::actor_registration<actor_group<large_better>> _dfs_actor_group_registeration(GDFSActorGroupTypeId);
registration::actor_registration<actor_group<small_better>> _bfs_actor_group_registeration(GBFSActorGroupTypeId);

template class actor_group<fifo_cmp>;
template class actor_group<large_better>;
template class actor_group<small_better>;
} // namespace brane
