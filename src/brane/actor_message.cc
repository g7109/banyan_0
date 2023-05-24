#include <brane/actor/actor_message.hh>
#include <sstream>

namespace brane {

std::string char_arr_to_str(const byte_t* addr, int len) {
    std::stringstream ss;
    for (int i = 0; i < len; ++i) {
        ss << std::hex << static_cast<unsigned>(addr[i]);
    }
    return ss.str();
}

} // namespace brane
