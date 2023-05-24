/**
 * DAIL in Alibaba Group
 *
 */

#include <memory>
#include <brane/core/thread_resource_pool.hh>

namespace brane {

work_thread::work_thread(unsigned worker_id)
    : _work_item()
    , _start_eventfd(0)
    , _worker([this, worker_id] { work(worker_id); }) {
}

work_thread::~work_thread() {
    _stopped.store(true, std::memory_order_relaxed);
    _start_eventfd.signal(1);
    _worker.join();
}

void work_thread::work(unsigned int worker_id) {
    const std::string thread_name = "resource_thread[" + std::to_string(worker_id) + "]";
    pthread_setname_np(pthread_self(), thread_name.c_str());
    sigset_t mask;
    sigfillset(&mask);
    auto r = ::pthread_sigmask(SIG_BLOCK, &mask, nullptr);
    seastar::throw_pthread_error(r);
    while(true) {
        uint64_t count;
        auto read_bytes = ::read(_start_eventfd.get_read_fd(), &count, sizeof(count));
        assert(read_bytes == sizeof(count));
        if (_stopped.load(std::memory_order_relaxed)) {
            break;
        }
        auto wi = _work_item.release();
        wi->process();
        thread_resource_pool::complete_work_item(wi);
        thread_resource_pool::return_worker(this);
        if (thread_resource_pool::_reactors_idle[_employer->cpu_id()].load(std::memory_order_seq_cst)) {
            uint64_t one = 1;
            ::write(_employer->_notify_eventfd.get(), &one, 8);
        }
    }
}

bool thread_resource_pool::_active = false;
unsigned thread_resource_pool::_queue_length = 32;
unsigned thread_resource_pool::_worker_num = 8;
int64_t thread_resource_pool::_sleep_duration_in_microseconds = 500;
std::unique_ptr<boost::lockfree::queue<work_thread*>> thread_resource_pool::_threads;
std::unique_ptr<thread_resource_pool::completed_queue[], thread_resource_pool::completed_queue_deleter> thread_resource_pool::_completed;
std::unique_ptr<std::atomic<bool>[], thread_resource_pool::atomic_flag_deleter> thread_resource_pool::_reactors_idle;

void thread_resource_pool::completed_queue_deleter::operator()(thread_resource_pool::completed_queue* q) const {
    ::operator delete[](q);
}

void thread_resource_pool::atomic_flag_deleter::operator()(std::atomic<bool> *flags) const {
    ::operator delete[](flags);
}

void thread_resource_pool::configure(const boost::program_options::variables_map& configs) {
    // Configure variables
    if (configs.count("open-thread-resource-pool")) {
        _active = configs["open-thread-resource-pool"].as<bool>();
    }
    if(!_active) return;
    if (configs.count("max-thread-resource-count")) {
        _queue_length = configs["max-thread-resource-count"].as<unsigned>();
    }
    if (configs.count("init-thread-resource-count")) {
        _worker_num = configs["init-thread-resource-count"].as<unsigned>();
    }
    assert(_worker_num <= _queue_length);
    if (configs.count("thread-resource-retry-interval-us")) {
        _sleep_duration_in_microseconds = configs["thread-resource-retry-interval-us"].as<int64_t>();
    }
    // Create completed queues
    _completed = decltype(thread_resource_pool::_completed){
        reinterpret_cast<completed_queue*>(operator new[] (sizeof(completed_queue{_queue_length}) * local_shard_count())),
        completed_queue_deleter{}};
    for(unsigned i = 0; i < local_shard_count(); i++) {
        new (&thread_resource_pool::_completed[i]) completed_queue(_queue_length);
    }
    // Create reactor idle flags
    _reactors_idle = decltype(thread_resource_pool::_reactors_idle){
        reinterpret_cast<std::atomic<bool>*>(operator new[] (sizeof(std::atomic<bool>) * local_shard_count())),
        atomic_flag_deleter{}};
    for(unsigned i = 0; i < local_shard_count(); i++) {
        new (&thread_resource_pool::_reactors_idle[i]) std::atomic<bool>(false);
    }
    // Create initial work threads
    _threads = std::make_unique<boost::lockfree::queue<work_thread*>>(_queue_length);
    for(unsigned i = 0; i < _worker_num; i++) {
        _threads->push(new work_thread(i));
    }
}

void thread_resource_pool::stop() {
    if(!_active) return;
    _threads->consume_all([&] (work_thread* wt) {
        delete wt;
    });
    for(unsigned i = 0; i < local_shard_count(); i++) {
        assert(_completed[i].empty());
        assert(!_reactors_idle[i].load(std::memory_order_seq_cst));
    }
}

bool thread_resource_pool::poll_queues() {
    auto nr = _completed[local_shard_id()].consume_all([&] (work_thread::work_item* wi) {
        wi->complete();
        delete wi;
    });
    return nr;
}

bool thread_resource_pool::pure_poll_queues() {
    return !_completed[local_shard_id()].empty();
}

void thread_resource_pool::enter_interrupt_mode() {
    _reactors_idle[local_shard_id()].store(true, std::memory_order_seq_cst);
}

void thread_resource_pool::exit_interrupt_mode() {
    _reactors_idle[local_shard_id()].store(false, std::memory_order_relaxed);
}

bool thread_resource_pool::active() {
    return _active;
}

void thread_resource_pool::complete_work_item(work_thread::work_item *wi) {
    assert(wi->_from_shard < local_shard_count());
    _completed[wi->_from_shard].push(wi);
}

void thread_resource_pool::return_worker(work_thread* wt) {
    _threads->push(wt);
}

} // namespace brane

