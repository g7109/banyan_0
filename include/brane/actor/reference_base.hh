#pragma once

#include <brane/actor/actor_message.hh>
#include <brane/actor/actor_factory.hh>
#include <brane/util/machine_info.hh>

namespace brane {

class scope_builder;

class reference_base {
private:
    void make_address(address& src_addr, uint32_t actor_id) {
        memcpy(addr.data, src_addr.data, src_addr.length);
        addr.length = src_addr.length;

        auto offset = addr.data + addr.length;
        memcpy(offset, addr.data + GMaxAddrLength - GActorTypeInBytes, GActorTypeInBytes);
        offset += GActorTypeInBytes;
        memcpy(offset, &actor_id, GActorIdInBytes);
        addr.length += GLocalActorAddrLength;
    }

    friend scope_builder;

protected:
    address addr;
    actor_base* exec_ctx = nullptr;

    reference_base(uint16_t actor_type, actor_base *exec) {
        auto offset = addr.data + GMaxAddrLength - GActorTypeInBytes;
        memcpy(offset, &actor_type, GActorTypeInBytes);
        exec_ctx = exec;
    }

    actor_base* get_execution_context() {
        return exec_ctx;
    }
};

template <typename T>
struct scope {
private:
    uint32_t scope_id;
public:
    explicit scope<T>(uint32_t id) { scope_id = id; }

    inline
    uint16_t get_type() {
        return T::get_type_id();
    }

    inline
    uint32_t get_id() {
        return scope_id;
    }
};

class scope_builder {
    address addr;
    friend class actor_base;
    friend class root_actor_group;
public:
    explicit scope_builder(uint32_t shard_id) {
        memcpy(addr.data, &shard_id, GShardIdInBytes);
        addr.length = GShardIdInBytes;
    }

    template <typename... Scopes>
    explicit scope_builder(uint32_t shard_id, Scopes ... scopes) {
        assert(sizeof...(Scopes) < GMaxAddrLayer);
        memcpy(addr.data, &shard_id, GShardIdInBytes);
        addr.length = GShardIdInBytes;

        auto scope_list = std::forward_as_tuple(scopes...);
        foreach_init_scope(scope_list);
    }

    template <typename T>
    inline void enter_sub_scope(scope<T> sub) {
        assert(addr.length + GLocalActorAddrLength < GMaxAddrLength);
        uint16_t actor_group_type = sub.get_type();
        memcpy(addr.data + addr.length, &actor_group_type, GActorTypeInBytes);
        uint32_t actor_group_id = sub.get_id();
        memcpy(addr.data + addr.length + GActorTypeInBytes, &actor_group_id, GActorIdInBytes);
        addr.length += GLocalActorAddrLength;
    }

    inline void back_to_parent_scope() {
        assert(addr.length >= (GShardIdInBytes + GLocalActorAddrLength));
        addr.length -= GLocalActorAddrLength;
    }

    inline uint32_t get_current_scope_id() {
        assert(addr.length >= (GShardIdInBytes + GLocalActorAddrLength));
        return *reinterpret_cast<uint32_t*>(addr.data + addr.length - GActorIdInBytes);
    }

    inline uint32_t get_root_scope_id() {
        assert(addr.length >= (GShardIdInBytes + GLocalActorAddrLength));
        return *reinterpret_cast<uint32_t*>(addr.data + GShardIdInBytes + GActorTypeInBytes);
    }

    inline size_t scope_layer_number() const {
        return ((addr.length - GShardIdInBytes) / GLocalActorAddrLength);
    }

    inline void reset_shard(uint32_t shard_id) {
        memcpy(addr.data, &shard_id, GShardIdInBytes);
    }

    inline void copy_address(address& dst_addr) {
        memcpy(dst_addr.data, addr.data, addr.length);
        dst_addr.length = addr.length;
    }

    template <typename T>
    T build_ref(uint32_t actor_id, actor_base *exec = nullptr) {
        T reference(exec);
        reference.make_address(addr, actor_id);
        return reference;
    }

    template <typename T>
    T* new_ref(uint32_t actor_id, actor_base *exec = nullptr) {
        auto* p_ref = new T(exec);
        p_ref->make_address(addr, actor_id);
        return p_ref;
    }

private:
    template<size_t I = 0, typename... Scopes>
    inline std::enable_if_t<I == sizeof...(Scopes), void>
    foreach_init_scope(const std::tuple<Scopes...>& scopes, size_t layer = I) {}

    template<size_t I = 0, typename... Scopes>
    inline std::enable_if_t<I < sizeof...(Scopes), void>
    foreach_init_scope(const std::tuple<Scopes...>& scopes, size_t layer = I) {
        auto offset = addr.data + addr.length;
        uint16_t actor_group_type = std::get<I>(scopes).get_type();
        memcpy(offset, &actor_group_type, GActorTypeInBytes);
        offset += GActorTypeInBytes;
        uint32_t actor_group_id = std::get<I>(scopes).get_id();
        memcpy(offset, &actor_group_id, GActorIdInBytes);
        addr.length += GLocalActorAddrLength;
        foreach_init_scope<I + 1>(scopes, layer + 1);
    }
};

} // namespace brane
