#include <brane/actor/actor_core.hh>
#include <brane/actor/actor_factory.hh>
#include <brane/actor/actor_implementation.hh>
#include <brane/core/barrier_type.hh>
#include <brane/core/coordinator.hh>
#include <brane/core/root_actor_group.hh>
#include <brane/core/local_channel.hh>
#include <brane/core/message_reclaimer.hh>
#include <brane/net/network_channel.hh>
#include <brane/util/unaligned_int.hh>
#include <brane/util/machine_info.hh>

#include "seastar/core/cacheline.hh"
#include "seastar/core/future.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/sleep.hh"

namespace brane {

root_actor_group::root_actor_group() : actor_base(nullptr) {}

root_actor_group::~root_actor_group() {
    assert(_num_actors_managed == 0);
}

void root_actor_group::stop_actor_system(bool force) {
    if (_stop_status != actor_status::RUNNING) { return; }
    _stop_status = force ? actor_status::FORCE_STOPPING
                         : actor_status::PEACE_STOPPING;
    if (force) {
        for (uint32_t id = 0; id < _response_pr_manager.size(); ++id) {
            if (_response_pr_manager.is_active(id)) {
                _response_pr_manager.set_exception(id,
                    std::make_exception_ptr(std::runtime_error("Force stop")));
                _response_pr_manager.remove_pr(id);
            }
        }
    }
    for (auto&& pairs : _actor_group_map) {
        if (pairs.second != nullptr) {
            pairs.second->stop(force);
            pairs.second = nullptr;
        }
    }
    if (_num_actors_managed == 0) {
        wait_others_and_exit();
    }
}

void root_actor_group::wait_others_and_exit() {
    coordinator::get().global_barrier(barrier_type::PEACE_STOPPED).then([] {
        if (local_shard_id() == 0) {
            seastar::engine().exit(0);
        }
    });
}

seastar::future<> root_actor_group::stopped_actor_enqueue(actor_message* msg) {
    delete msg;
    return seastar::make_ready_future<>();
}

seastar::future<> root_actor_group::receive(actor_message* msg) {
#if DEBUG
    const unsigned this_gsid = engine().cpu_id() + machine_info::sid_anchor();
    assert(this_gsid == msg->hdr.shard_id)
#endif
    auto &addr = msg->hdr.addr;
    /// destination is the root actor
    if (addr.length == _total_addr_length) {
        switch(msg->hdr.m_type) {
            case message_type::RESPONSE: {
                auto pr_id = msg->hdr.pr_id;
                if (_response_pr_manager.is_active(pr_id)) {
                    _response_pr_manager.set_value(pr_id, msg);
                } else {
                    delete msg;
                }
                break;
            }
            case message_type::PEACE_STOP: {
                stop_actor_system(false);
                reclaim_actor_message(msg);
                break;
            }
            case message_type::FORCE_STOP: {
                stop_actor_system(true);
                reclaim_actor_message(msg);
                break;
            }
            default:
                assert(false);
        }
        return seastar::make_ready_future<>();
    }

    /// destination is an actor
    auto child_id = load_unaligned_int_partial<uint64_t>(
                        addr.data + _total_addr_length,
                        GLocalActorAddrLength);
    auto search = _actor_group_map.find(child_id);
    if (search != _actor_group_map.end()) {
        /// actor is disabled, delete the message
        if (!search->second) {
            return stopped_actor_enqueue(msg);
        }

        actor_base *dest_actor = (addr.length == search->second->get_address_length())
                               ? search->second : search->second->get_actor_local(msg->hdr);

        if (dest_actor != nullptr) {
            if(msg->hdr.m_type == message_type::USER) {
                return dest_actor->enque_message(msg);
            }
            else {
                return dest_actor->enque_urgent_message(msg);
            }
        }
        else {
            /// actor is disabled, delete the message
            return stopped_actor_enqueue(msg);
        }
    }

    if (_stop_status != actor_status::RUNNING) {
        return stopped_actor_enqueue(msg);
    }

    /// actor not exists, create a new one
    uint16_t actor_type = load_unaligned_int<uint16_t>(addr.data + _total_addr_length);
    actor_base* child = actor_factory::get().create(actor_type, this, addr.data + _total_addr_length);

    auto child_priority = load_unaligned_int<uint32_t>(addr.data + _total_addr_length + GActorTypeInBytes);
    child->set_priority(child_priority);

    _actor_group_map[child_id] = child;
    ++_num_actors_managed;
    auto sem_f = _actor_activation_sem.wait(1);
    if (sem_f.available()) {
        child->schedule();
    }
    else {
        auto activate_actor_func = [child](const seastar::future_state<>&&) {
            child->schedule();
        };
        using continuationized_func = seastar::continuation<
            std::function<void(const seastar::future_state<>&&)>>;
        seastar::internal::set_callback(
            sem_f, std::make_unique<continuationized_func> (activate_actor_func, nullptr));
    }

    actor_base *dest_actor = (addr.length == child->get_address_length()) ?
                             child : child->get_actor_local(msg->hdr);

    if(msg->hdr.m_type == message_type::USER) {
        return dest_actor->enque_message(msg);
    }
    else {
        return dest_actor->enque_urgent_message(msg);
    }
}

void root_actor_group::exit(bool force) {
    auto m_type = force ? message_type::FORCE_STOP
                        : message_type::PEACE_STOP;
    for (uint32_t sid = 0; sid < global_shard_count(); ++sid) {
        address dst_addr{};
        memcpy(dst_addr.data, &sid, GShardIdInBytes);
        dst_addr.length = GShardIdInBytes;
        auto *msg = make_system_message(dst_addr, m_type);
        actor_engine().send(msg);
    }
}

void root_actor_group::cancel_query(uint32_t query_id, uint16_t query_actor_group_type) {
    auto m_type = message_type::FORCE_STOP;
    for (uint32_t sid = 0; sid < global_shard_count(); ++sid) {
        address dst_addr{};
        memcpy(dst_addr.data, &sid, GShardIdInBytes);
        memcpy(dst_addr.data + GShardIdInBytes, &query_actor_group_type, GActorTypeInBytes);
        memcpy(dst_addr.data + GShardIdInBytes + GActorTypeInBytes, &query_id, GActorIdInBytes);
        dst_addr.length = GShardIdInBytes + GLocalActorAddrLength;
        auto *msg = make_system_message(dst_addr, m_type);
        actor_engine().send(msg);
    }
}

seastar::future<> root_actor_group::cancel_query_request(uint32_t query_id, uint16_t query_actor_group_type) {
    return seastar::parallel_for_each(boost::irange(0u, global_shard_count()), [this, query_id, query_actor_group_type] (unsigned i) {
        auto m_type = message_type::FORCE_STOP;
        address dst_addr{};
        memcpy(dst_addr.data, &i, GShardIdInBytes);
        memcpy(dst_addr.data + GShardIdInBytes, &query_actor_group_type, GActorTypeInBytes);
        memcpy(dst_addr.data + GShardIdInBytes + GActorTypeInBytes, &query_id, GActorIdInBytes);
        dst_addr.length = GShardIdInBytes + GLocalActorAddrLength;

        auto global_sid = global_shard_id();
        auto pr_id = _response_pr_manager.acquire_pr();
        auto *msg = make_request_message(dst_addr, 0, global_sid, pr_id, m_type);
        return seastar::when_all(
                    actor_engine().send(msg),
                    actor_engine()._response_pr_manager.get_future(pr_id)).then(
                [pr_id] (std::tuple<seastar::future<>, seastar::future<actor_message*> > res) {
            std::get<0>(res).ignore_ready_future();
            std::get<1>(res).ignore_ready_future();
            actor_engine()._response_pr_manager.remove_pr(pr_id);
        });
    });
}

void root_actor_group::cancel_scope(const scope_builder& scope) {
    auto m_type = message_type::FORCE_STOP;
    for (uint32_t sid = 0; sid < global_shard_count(); ++sid) {
        address dst_addr = scope.addr;
        memcpy(dst_addr.data, &sid, GShardIdInBytes);
        auto *msg = make_system_message(dst_addr, m_type);
        actor_engine().send(msg);
    }
}

void root_actor_group::clear_all() {
    _actor_group_map.clear();
}

seastar::future<> root_actor_group::send(actor_message* msg) {
    static thread_local const unsigned this_gsid = global_shard_id();
    auto dest_shard_id = msg->hdr.addr.get_shard_id();
    if (dest_shard_id != this_gsid) {
        if (machine_info::is_local(dest_shard_id)) {
            return local_channel::send(msg);
        } else {
            return network_channel::send(msg);
        }
    }
    return receive(msg);
}

/// Only existing child actor group calls this function to terminate itself
void root_actor_group::stop_child_actor(byte_t *child_addr, bool force) {
    auto child_id = load_unaligned_int_partial<uint64_t>(child_addr, GLocalActorAddrLength);
    auto search = _actor_group_map.find(child_id);
    assert (search != _actor_group_map.end());
    if (search->second) {
        search->second->stop(force);
        _actor_group_map[child_id] = nullptr;
    }
}

void root_actor_group::notify_child_stopped() {
    if (--_num_actors_managed == 0 && _stop_status != actor_status::RUNNING) {
        wait_others_and_exit();
    }
//    _actor_group_map.clear();
    _actor_activation_sem.signal(1);
}

// [DAIL] hold root_actor_group.
struct rag_deleter {
    void operator()(root_actor_group* p) {
        p->~root_actor_group();
        free(p);
    }
};

thread_local std::unique_ptr<root_actor_group, rag_deleter> rag_holder;

void allocate_actor_engine() {
    assert(!rag_holder);

    void *buf;
    int r = posix_memalign(&buf, seastar::cache_line_size, sizeof(root_actor_group));
    assert(r == 0);
    local_actor_engine = reinterpret_cast<root_actor_group*>(buf);
    new (buf) root_actor_group();
    rag_holder.reset(local_actor_engine);
}

__thread root_actor_group* local_actor_engine{nullptr};

} // namespace brane
