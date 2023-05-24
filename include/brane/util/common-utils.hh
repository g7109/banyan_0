#pragma once

#include <cstdint>
#include <cstddef>

#define ANNOTATION(NAME) __attribute__((annotate(#NAME)))
#define ACTOR_ITFC_CTOR(NAME) NAME(brane::actor_base *exec = nullptr)
#define ACTOR_ITFC_DTOR(NAME) ~NAME() {}
#define ACTOR_IMPL_CTOR(NAME) NAME(brane::actor_base *exec_ctx, const brane::byte_t *addr)
#define ACTOR_IMPL_DTOR(NAME) ~NAME() override {}
#define ACTOR_DO_WORK() seastar::future<brane::stop_reaction> \
    do_work(brane::actor_message* msg) override

namespace brane {

using byte_t = uint8_t;

template<typename T, typename U>
constexpr size_t offset_of(U T::*member) {
    T* obj_ptr = nullptr;
    return (char*)&(obj_ptr->*member) - (char*)obj_ptr;
}

inline unsigned* as_u32_ptr(void* data) {
    return reinterpret_cast<uint32_t*>(data);
}

inline unsigned* to_u32_ptr(void* data) {
    return static_cast<uint32_t*>(data);
}

inline byte_t* as_byte_ptr(void* data) {
    return reinterpret_cast<byte_t*>(data);
}

inline byte_t* to_byte_ptr(void* data) {
    return static_cast<byte_t*>(data);
}

} // namespace brane
