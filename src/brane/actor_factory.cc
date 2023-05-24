/**
 * DAIL in Alibaba Group
 *
 */

#include <brane/actor/actor_factory.hh>
#include <brane/actor/actor_core.hh>

namespace brane {

actor_base* actor_factory::create(uint16_t actor_tid, actor_base *exec_ctx, const byte_t *addr) {
    auto build_func = _actor_directory[actor_tid];
    actor_base* actor_ptr = build_func(exec_ctx, addr);
    actor_ptr->initialize();
    return actor_ptr;
}

void actor_factory::register_actor(uint16_t actor_tid, const ActorInstBuilder func) {
	auto reserved_size = static_cast<size_t>(actor_tid + 1);
	if (reserved_size > _actor_directory.size()) {
		_actor_directory.resize(reserved_size);
	}
	_actor_directory[actor_tid] = std::move(func);
}

} // namespace brane
