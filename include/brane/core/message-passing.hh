/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <seastar/core/alien.hh>

namespace brane {

template <typename Func>
inline
seastar::futurize_t<std::result_of_t<Func()>> submit_to(unsigned t, Func&& func) {
    return seastar::smp::submit_to(t, std::forward<Func>(func));
}

template<typename Func, typename T = seastar::alien::internal::return_type_t<Func>>
inline
std::future<T> alien_submit_to(unsigned shard, Func func) {
    return seastar::alien::submit_to(shard, std::move(func));
}

} // namespace brane
