#include <brane/core/coordinator_worker.hh>
#include "seastar/core/reactor.hh"

namespace brane {

coord_work_queue::coord_work_queue()
    : _pending()
    , _completed()
    , _start_eventfd(0) {
}

void coord_work_queue::submit_item(std::unique_ptr<coord_work_queue::work_item> item) {
    _queue_has_room.wait().then([this, item = std::move(item)] () mutable {
        _pending.push(item.release());
        _start_eventfd.signal(1);
    });
}

unsigned coord_work_queue::complete() {
    std::array<work_item*, queue_length> tmp_buf{nullptr};
    auto end = tmp_buf.data();
    auto nr = _completed.consume_all([&] (work_item* wi) {
        *end++ = wi;
    });
    for (auto p = tmp_buf.data(); p != end; ++p) {
        auto wi = *p;
        wi->complete();
        delete wi;
    }
    _queue_has_room.signal(nr);
    return nr;
}

coordinator_worker::coordinator_worker(seastar::reactor *r)
    : _reactor(r), _worker_thread([this] { work(); }) { }

void coordinator_worker::work() {
    pthread_setname_np(pthread_self(), "coordinator-worker");
    sigset_t mask;
    sigfillset(&mask);
    auto r = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
    seastar::throw_pthread_error(r);
    std::array<coord_work_queue::work_item*, coord_work_queue::queue_length> tmp_buf{nullptr};
    while (true) {
        uint64_t count;
        auto r = ::read(inter_thread_wq._start_eventfd.get_read_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
        if (_stopped.load(std::memory_order_relaxed)) {
            break;
        }
        auto end = tmp_buf.data();
        inter_thread_wq._pending.consume_all([&] (coord_work_queue::work_item* wi) {
            *end++ = wi;
        });
        for (auto p = tmp_buf.data(); p != end; ++p) {
            auto wi = *p;
            wi->process();
            inter_thread_wq._completed.push(wi);
        }
        if (_main_thread_idle.load(std::memory_order_seq_cst)) {
            uint64_t one = 1;
            ::write(_reactor->_notify_eventfd.get(), &one, 8);
        }
    }
}

coordinator_worker::~coordinator_worker() {
    _stopped.store(true, std::memory_order_relaxed);
    inter_thread_wq._start_eventfd.signal(1);
    _worker_thread.join();
}

} // namespace brane
