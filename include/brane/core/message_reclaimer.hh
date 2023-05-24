#pragma once

#include <brane/actor/actor_message.hh>

namespace brane {

inline void reclaim_actor_message(actor_message *msg) {
    delete msg;
}

} // namespace brane
