/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <string>
#include <vector>
#include <brane/actor/actor_message.hh>
#include <brane/core/local_channel.hh>

namespace brane {

class actor_base;

typedef actor_base* (*ActorInstBuilder)(actor_base*, const byte_t*);

class actor_factory {
    std::vector<ActorInstBuilder> _actor_directory;
    actor_factory() = default;
public:
    static actor_factory& get();
    actor_base* create(uint16_t actor_tid, actor_base *exec_ctx, const byte_t *addr);
    void register_actor(uint16_t actor_tid, ActorInstBuilder func);
};

namespace registration {

template <typename T>
class actor_registration {
public:
    explicit actor_registration(uint16_t actor_tid) noexcept {
        static_assert(std::is_base_of<actor_base, T>::value, "T must be a derived class of actor_base.");
        actor_factory::get().register_actor(actor_tid, actor_builder_func);
    }
private:
    static actor_base* actor_builder_func(actor_base *exec_ctx, const byte_t *addr) {
        return new T{exec_ctx, addr};
    }
};

} // namespace registration

inline
actor_factory& actor_factory::get() {
    static actor_factory instance;
    return instance;
}

} // namespace brane
