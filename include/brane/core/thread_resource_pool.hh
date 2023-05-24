/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/util/noncopyable_function.hh>
#include <brane/core/shard-config.hh>
#include <boost/lockfree/queue.hpp>

namespace brane {

class thread_resource_pool;

class work_thread {
    struct work_item {
        unsigned _from_shard;

        explicit work_item(unsigned from_shard): _from_shard(from_shard) {}
        virtual ~work_item() = default;
        virtual void process() = 0;
        virtual void complete() = 0;
    };
    template <typename T>
    struct work_item_returning : work_item {
        seastar::noncopyable_function<T ()> _func;
        seastar::promise<T> _promise;
        seastar::compat::optional<T> _result;

        work_item_returning(unsigned from_shard, seastar::noncopyable_function<T ()> func)
            : work_item(from_shard), _func(std::move(func)) {}
        void process() override { _result = this->_func(); }
        void complete() override { _promise.set_value(std::move(*_result)); }
        seastar::future<T> get_future() { return _promise.get_future(); }
    };
    std::unique_ptr<work_item> _work_item;
    seastar::writeable_eventfd _start_eventfd;
    seastar::posix_thread _worker;
    std::atomic<bool> _stopped = { false };
    seastar::reactor* _employer = nullptr;
public:
    ~work_thread();
private:
    explicit work_thread(unsigned worker_id);

    template <typename T>
    seastar::future<T> submit(seastar::reactor* employer, seastar::noncopyable_function<T ()> func) {
        _employer = employer;
        auto* wi = new work_item_returning<T>(_employer->cpu_id(), std::move(func));
        auto fut = wi->get_future();
        _work_item.reset(wi);
        _start_eventfd.signal(1);
        return fut;
    }

    void work(unsigned worker_id);

    friend thread_resource_pool;
};

class thread_resource_pool {
    static bool _active;
    static unsigned _queue_length;
    static unsigned _worker_num;
    static int64_t _sleep_duration_in_microseconds;
    static std::unique_ptr<boost::lockfree::queue<work_thread*>> _threads;
    using completed_queue = boost::lockfree::queue<work_thread::work_item*>;
    struct completed_queue_deleter {
        void operator()(completed_queue* q) const;
    };
    static std::unique_ptr<completed_queue[], completed_queue_deleter> _completed;
    struct atomic_flag_deleter {
        void operator()(std::atomic<bool>* flags) const;
    };
    static std::unique_ptr<std::atomic<bool>[], atomic_flag_deleter> _reactors_idle;

public:
    static void configure(const boost::program_options::variables_map& configs);
    static void stop();

    template <typename T, typename Func>
    static seastar::future<T> submit_work(Func func, const bool create_worker_if_fail_acquire = true) {
        if(!_active) {
            return seastar::make_exception_future<T>(std::make_exception_ptr(std::runtime_error(
                    "Thread resource pool is not open. "
                    "Try to add command line parameter \"--open-thread-resource-pool=true\".")));
        }
        work_thread* wt = nullptr;
        return seastar::now().then([&wt, create_worker_if_fail_acquire] {
            if(!_threads->pop(wt)) {
                if(create_worker_if_fail_acquire && _worker_num < _queue_length) {
                    wt = new work_thread(_worker_num++);
                    return seastar::make_ready_future<>();
                }
                return seastar::repeat([&wt] {
                    return seastar::sleep(std::chrono::microseconds(_sleep_duration_in_microseconds)).then([&wt] {
                        return _threads->pop(wt)? seastar::stop_iteration::yes : seastar::stop_iteration::no;
                    });
                });
            }
            return seastar::make_ready_future<>();
        }).then([&wt, &func] {
            return wt->submit<T>(seastar::local_engine, std::move(func));
        });
    }

    static bool poll_queues();
    static bool pure_poll_queues();

    static void enter_interrupt_mode();
    static void exit_interrupt_mode();

    static bool active();

private:
    static void complete_work_item(work_thread::work_item* wi);
    static void return_worker(work_thread* wt);

    friend work_thread;
};

} // namespace brane
