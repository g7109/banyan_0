#pragma once

#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/util/noncopyable_function.hh>
#include <boost/lockfree/spsc_queue.hpp>

namespace seastar {

class reactor;

} // namespace seastar

namespace brane {

class coord_work_queue {
    static constexpr size_t queue_length = 128;
    struct work_item;
    using lf_queue = boost::lockfree::spsc_queue<work_item*,
    boost::lockfree::capacity<queue_length>>;
    lf_queue _pending;
    lf_queue _completed;
    seastar::writeable_eventfd _start_eventfd;
    seastar::semaphore _queue_has_room = { queue_length };
    struct work_item {
        virtual ~work_item() = default;
        virtual void process() = 0;
        virtual void complete() = 0;
    };
    template <typename T>
    struct work_item_returning :  work_item {
        seastar::noncopyable_function<T ()> _func;
        seastar::promise<T> _promise;
        seastar::compat::optional<T> _result;
        work_item_returning(seastar::noncopyable_function<T ()> func) : _func(std::move(func)) {}
        virtual void process() override { _result = this->_func(); }
        virtual void complete() override { _promise.set_value(std::move(*_result)); }
        seastar::future<T> get_future() { return _promise.get_future(); }
    };
public:
    coord_work_queue();
    template <typename T>
    seastar::future<T> submit(seastar::noncopyable_function<T ()> func) {
        auto wi = std::make_unique<work_item_returning<T>>(std::move(func));
        auto fut = wi->get_future();
        submit_item(std::move(wi));
        return fut;
    }
private:
    unsigned complete();
    void submit_item(std::unique_ptr<coord_work_queue::work_item> wi);

    friend class coordinator_worker;
};

class coordinator_worker {
    seastar::reactor *_reactor;
    coord_work_queue inter_thread_wq;
    seastar::posix_thread _worker_thread;
    std::atomic<bool> _stopped = { false };
    std::atomic<bool> _main_thread_idle = { false };
public:
    explicit coordinator_worker(seastar::reactor *r);
    ~coordinator_worker();

    template <typename T, typename Func>
    seastar::future<T> submit(Func func) {
        return inter_thread_wq.submit<T>(std::move(func));
    }

    unsigned complete() {
        return inter_thread_wq.complete();
    }

    void enter_interrupt_mode() {
        _main_thread_idle.store(true, std::memory_order_seq_cst);
    }

    void exit_interrupt_mode() {
        _main_thread_idle.store(false, std::memory_order_relaxed);
    }
private:
    void work();
};

} // namespace brane
