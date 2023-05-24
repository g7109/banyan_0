/**
 * DAIL in Alibaba Group
 *
 */

#include <seastar/core/sleep.hh>
#include <brane/core/gpu_resource_pool.hh>

namespace brane {

std::vector<gpu_resource_pool::stream_unit> gpu_resource_pool::_streams;
std::unique_ptr<boost::lockfree::queue<uint32_t> > gpu_resource_pool::_stream_resource_queue;
std::unique_ptr<pr_manager<> > gpu_resource_pool::_event_promise_manager;
std::unique_ptr<gpu_resource_pool::event_promise_set[], gpu_resource_pool::event_pr_set_deleter> gpu_resource_pool::_event_pr_check_sets;
uint32_t gpu_resource_pool::_streams_num = 4;
uint32_t gpu_resource_pool::_events_num_per_stream = 2;
int64_t gpu_resource_pool::_sleep_duration_in_microseconds = 500;

void gpu_resource_pool::event_pr_set_deleter::operator()(gpu_resource_pool::event_promise_set* eps) const {
    ::operator delete[](eps);
}

void gpu_resource_pool::configure(const boost::program_options::variables_map& configs) {
    // Configure variables
    if (configs.count("cuda-stream-resource-number")) {
        _streams_num = configs["cuda-stream-resource-number"].as<unsigned>();
    }
    if (configs.count("max-gpu-task-number-per-stream")) {
        _events_num_per_stream = configs["max-gpu-task-number-per-stream"].as<unsigned>();
    }
    // Create initial streams
    _streams.reserve(_streams_num);
    for (uint32_t i = 0; i < _streams_num; i++) {
        _streams.emplace_back(create_cuda_stream(), 0);
    }
    // Create stream resource queue
    _stream_resource_queue = std::make_unique<boost::lockfree::queue<uint32_t> >(_streams_num);
    for (uint32_t i = 0; i < _streams_num; i++) {
        _stream_resource_queue->push(i);
    }
    // Create event promise manager
    _event_promise_manager = std::make_unique<pr_manager<> >(_streams_num * _events_num_per_stream);
    // Create event promise sets
    _event_pr_check_sets = decltype(gpu_resource_pool::_event_pr_check_sets){
        reinterpret_cast<event_promise_set*>(operator new[] (sizeof(event_promise_set) * local_shard_count())),
        event_pr_set_deleter{}};
    for(unsigned i = 0; i < local_shard_count(); i++) {
        new (&gpu_resource_pool::_event_pr_check_sets[i]) event_promise_set();
    }
}

void gpu_resource_pool::stop() {
    for(unsigned i = 0; i < local_shard_count(); i++) {
        assert(_event_pr_check_sets[i].size() == 0);
    }
    gpu_resource_pool::_streams.clear();
}

bool gpu_resource_pool::poll_queues() {
    int count = 0;
    if (!_event_pr_check_sets[local_shard_id()].empty()) {
        for (auto it = _event_pr_check_sets[local_shard_id()].begin(); it != _event_pr_check_sets[local_shard_id()].end(); ) {
            if (cuda_check((*it).event) > 0) {
                _event_promise_manager->set_value((*it).pr_id);
                it = _event_pr_check_sets[local_shard_id()].erase(it);
                count++;
                continue;
            }
            it++;
        }
    }
    return count;
}

bool gpu_resource_pool::pure_poll_queues() {
    return _event_pr_check_sets[local_shard_id()].size();
}

seastar::future<uint32_t> gpu_resource_pool::get_stream() {
    uint32_t stream_id = 0;
    return seastar::now().then([&stream_id] {
        if (!_stream_resource_queue->pop(stream_id)) {
            return seastar::repeat([&stream_id] {
                    return seastar::sleep(std::chrono::microseconds(_sleep_duration_in_microseconds)).then([&stream_id] {
                        return _stream_resource_queue->pop(stream_id)? seastar::stop_iteration::yes : seastar::stop_iteration::no;
                    });
            });
        }
        return seastar::make_ready_future<>();
    }).then([&stream_id] {
        return seastar::make_ready_future<uint32_t>(stream_id);
    });
}

}  // namespace brane
