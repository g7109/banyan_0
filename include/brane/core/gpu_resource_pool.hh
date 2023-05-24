/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <brane/core/shard-config.hh>
#include <brane/core/promise_manager.hh>
#include <brane/cuda_kernels/device.h>
#include <boost/lockfree/queue.hpp>
#include <functional>
#include <unordered_set>

#ifndef BRANE_GPU_RESOURCE_POOL
#include <seastar/core/print.hh>
#endif

namespace brane {

class gpu_resource_pool {
private:
    struct stream_unit {
        cuda_stream_handle stream;
        std::atomic<uint32_t> count;
        
        stream_unit(cuda_stream_handle stream, uint32_t init_count = 0)
            : stream(stream), count(init_count) {}
        stream_unit(stream_unit&& other) noexcept
            : stream(other.stream), count(other.count.load()) {}
        stream_unit& operator=(stream_unit&& other) noexcept {
            if(this != &other) {
                stream = other.stream;
                count.store(other.count.load());
            }
            return *this;
        }
    };
    static std::vector<stream_unit> _streams;
    static std::unique_ptr<boost::lockfree::queue<uint32_t> > _stream_resource_queue;
    static std::unique_ptr<pr_manager<> > _event_promise_manager;
    struct event_promise {
        cuda_event_handle event;
        uint32_t pr_id;
        event_promise(cuda_event_handle event, uint32_t pr_id)
            : event(event), pr_id(pr_id) {}

        bool operator==(const event_promise& ep) const { 
            return (this->pr_id == ep.pr_id); 
        } 
    };
    class event_promise_hash_func {
    public:  
        size_t operator()(const event_promise& ep) const { 
            return ep.pr_id; 
        } 
    };
    using event_promise_set = std::unordered_set<event_promise, event_promise_hash_func>;
    struct event_pr_set_deleter {
        void operator()(event_promise_set* eps) const;
    };
    static std::unique_ptr<event_promise_set[], event_pr_set_deleter> _event_pr_check_sets;
    static uint32_t _streams_num;
    static uint32_t _events_num_per_stream;
    static int64_t _sleep_duration_in_microseconds;
public:
    static void configure(const boost::program_options::variables_map& configs);
    static void stop();

#ifdef BRANE_GPU_RESOURCE_POOL
    template <typename... Args>
    static future<> submit_work(std::function<void(cuda_stream_handle, Args...)>&& gpu_func, Args... args) {
        return get_stream().then([func = std::move(gpu_func), args...] (uint32_t stream_id) {
            auto& su = _streams[stream_id];
            func(su.stream, args...);
            cuda_event_handle event = create_cuda_event();
            cuda_stream_event_record(su.stream, event);
            if(++(su.count) < _events_num_per_stream) {
                _stream_resource_queue->push(stream_id);
            }
            auto pr_id = _event_promise_manager->acquire_pr();
            _event_pr_check_sets[local_shard_id()].emplace(event, pr_id);
            return _event_promise_manager->get_future(pr_id).then([pr_id, stream_id, &su] {
                _event_promise_manager->remove_pr(pr_id);
                if((su.count)-- == _events_num_per_stream) {
                    _stream_resource_queue->push(stream_id);
                }
                return make_ready_future<>();
            });
        });
    }
#else
    template <typename... Args>
    static seastar::future<> submit_work(std::function<void(cuda_stream_handle, Args...)>&& gpu_func, Args... args) {
        fmt::print("GPU resource pool is disable now. "
                   "Try to add command line parameter \"--open-gpu-resource-pool\" "
                   "when configuring by configure.py.\n");
        abort();
    }
#endif

    static bool poll_queues();
    static bool pure_poll_queues();

private:
    static seastar::future<uint32_t> get_stream();
};

}  // namespace brane
