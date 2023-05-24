#pragma once

#include <memory>
#include <cassert>

namespace brane {

struct lba_policy {
    lba_policy() = default;
    virtual ~lba_policy() {}
    virtual unsigned get_cpu(uint32_t addr, uint16_t port) = 0;
};

struct customized_lba {
    static inline
    unsigned get_cpu(uint32_t addr, uint16_t port) {
        return impl_->get_cpu(addr, port);
    }

    static inline
    void set_policy(std::unique_ptr<lba_policy>&& policy) {
        assert(!impl_ && "Error: policy is already setted.");
        impl_ = std::move(policy);
    }

private:
    static std::unique_ptr<lba_policy> impl_;
};

} // namespace brane
