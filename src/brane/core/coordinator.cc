#include <brane/core/coordinator.hh>
#include <brane/core/shard-config.hh>

#include "seastar/core/print.hh"

namespace brane {

void coordinator::launch_worker(seastar::reactor *r) {
    worker_ = std::make_unique<coordinator_worker>(r);
}

seastar::future<> coordinator::global_barrier(unsigned barrier_id) {
    if (local_shard_id() != 0) {
        return seastar::smp::submit_to(0, [this, barrier_id] {
            get_sem(barrier_id, true)->signal();
        });
    } else {
        return get_sem(barrier_id, true)->wait(local_shard_count() - 1).then([barrier_id, this] {
            assert(worker_ && "coordinator worker is not ready.\n");
            return worker_->submit<int>([barrier_id, this] {
                if (impl_) {
                    return impl_->global_barrier(barrier_id);
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
                return 0;
            });
        }).then([barrier_id, this] (int) {
            global_sems_.erase(barrier_id);
        });
    }
}

seastar::future<> coordinator::local_barrier(unsigned barrier_id) {
    if (local_shard_id() != 0) {
        return seastar::smp::submit_to(0, [this, barrier_id] {
            get_sem(barrier_id, false)->signal();
        });
    } else {
        return get_sem(barrier_id, false)->wait(local_shard_count() - 1).then([barrier_id, this] {
            local_sems_.erase(barrier_id);
        });
    }
}

void coordinator::set_sync_size(unsigned sync_size) {
    if (impl_) {
        impl_->set_sync_size(sync_size);
    } else {
        fmt::print("Warning: unable to set sync size(coordinator::impl_ is nullptr).\n");
    }
}

} // namespace brane
