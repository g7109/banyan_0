#pragma once

#include <brane/core/coordinator.hh>
#include <seastar/core/reactor.hh>

namespace brane {

class coordinator_pollfn final : public seastar::reactor::pollfn {
public:
    coordinator_pollfn() = default;
    ~coordinator_pollfn() final = default;

    bool poll() final { return bool(coordinator::get().worker_->complete()); }
    bool pure_poll() final { return poll(); }

    bool try_enter_interrupt_mode() override {
        coordinator::get().worker_->enter_interrupt_mode();
        if (poll()) {
            // raced
            coordinator::get().worker_->exit_interrupt_mode();
            return false;
        }
        return true;
    }

    void exit_interrupt_mode() final {
        coordinator::get().worker_->exit_interrupt_mode();
    }
};

} // namespace brane
