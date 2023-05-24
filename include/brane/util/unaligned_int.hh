// reference link: https://github.com/aguinet/intmem/blob/master/intmem.h

#pragma once

#include <type_traits>
#include <cstring>

namespace brane {

template <typename T>
inline T load_unaligned_int(const void *ptr) {
    static_assert(std::is_integral<T>::value, "T must be an integer!");
    T ret;
    memcpy(&ret, ptr, sizeof(T));
    return ret;
}

template <typename T>
inline T load_unaligned_int_partial(const void *ptr, size_t len) {
    static_assert(std::is_integral<T>::value, "T must be an integer with a length >= len!");
    T ret = 0;
    memcpy(&ret, ptr, len);
    return ret;
}

template <typename T>
inline void store_unaligned_int(void *ptr, T const val) {
    static_assert(std::is_integral<T>::value, "T must be an integer!");
    memcpy(ptr, &val, sizeof(T));
}

} // namespace brane
