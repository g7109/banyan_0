/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <boost/range/irange.hpp>
#include <brane/util/common-utils.hh>
#include <brane/util/machine_info.hh>
#include <brane/util/unaligned_int.hh>
#include <cstring>
#include <string>

#include "seastar/core/temporary_buffer.hh"
#include "seastar/core/iostream.hh"

namespace brane {

const size_t GActorTypeInBytes = 2;
const size_t GShardIdInBytes = 4;
const size_t GActorIdInBytes = 4;
const size_t GMaxAddrLayer = 8;
const size_t GLocalActorAddrLength = GActorTypeInBytes + GActorIdInBytes; // 6 bytes
const size_t GMaxAddrLength = GShardIdInBytes + GMaxAddrLayer * GLocalActorAddrLength; // 52 bytes

enum message_type : uint8_t {
    USER = 0x01,
    RESPONSE = 0x02,
    FORCE_STOP = 0x04,
    PEACE_STOP = 0x08,
};

/***
 * Format: shard_id, actor_group_id_1, ..., actor_group_id_6, actor_id
 * shard_id: (4 bytes)
 * actor_group_id_x: class_type (2 bytes) + group_guid (4 bytes)
 * actor_id: class_type (2 bytes) + actor_guid (4 bytes)
 */
struct address {
    byte_t length = 0;
    byte_t data[GMaxAddrLength];
    address() = default;
    address(const address&) = default;

public:
    uint32_t get_shard_id() {
        return load_unaligned_int<uint32_t>(data);
    }
};

struct serializable_unit {
    uint32_t num_used = 0;
    seastar::temporary_buffer<char> bufs[6];
};

struct network_delegater;
struct actor_message_without_payload;
template <typename T> struct actor_message_with_payload;

struct actor_message {
    struct network_flag {};
    struct header {
        // actor address
        address addr;
        // behavior type id
        uint8_t behavior_tid;
        // fields used to address response
        uint32_t src_shard_id;
        uint32_t pr_id;

        message_type m_type;
        const bool from_network;
        // to make header a POD type, we explicitly define a default constructor
        header() = default;

        header(const address &addr, uint8_t behv_tid, uint32_t src_s_id,
               uint32_t pr_id, message_type m_type, bool from_network)
            : addr(addr), behavior_tid(behv_tid), src_shard_id(src_s_id),
            pr_id(pr_id), m_type(m_type), from_network(from_network) {}

        header(const header& x) = default;
    } hdr;

    actor_message(const actor_message& x) = default;
    explicit actor_message(const header& hdr) : hdr(hdr) {}
    virtual ~actor_message() = default;
    virtual seastar::future<> serialize(seastar::output_stream<char> &out) = 0;

private:
    // Local
    actor_message(const address &addr, uint8_t behv_tid, uint32_t src_s_id,
            uint32_t pr_id, message_type m_type)
        : hdr(addr, behv_tid, src_s_id, pr_id, m_type, false) {}

    // Remote
    actor_message(const address &addr, uint8_t behv_tid, uint32_t src_s_id,
            uint32_t pr_id, message_type m_type, network_flag)
        : hdr(addr, behv_tid, src_s_id, pr_id, m_type, true) {}
    // static_assert(std::is_pod<header>::value, "header type should be pod.");

    friend struct actor_message_without_payload;
    template <typename T>
    friend struct actor_message_with_payload;
};

struct actor_message_without_payload final : public actor_message {
    actor_message_without_payload(const actor_message_without_payload& ) = delete;
    actor_message_without_payload(actor_message_without_payload&& x) = delete;
    ~actor_message_without_payload() override = default;

    static actor_message_without_payload*
    make_actor_message(const address &addr, uint8_t behv_tid,
            uint32_t src_s_id, uint32_t pr_id, message_type m_type) {
        return new actor_message_without_payload {
            addr, behv_tid, src_s_id, pr_id, m_type
        };
    }

    seastar::future<> serialize(seastar::output_stream<char> &out) final {
        static const uint32_t nr_payload = 0;
        static constexpr uint32_t msg_len = sizeof(header) + 4;
        return out.write(reinterpret_cast<const char*>(&msg_len), 4).then([this, &out] {
            return out.write(reinterpret_cast<char*>(&hdr), sizeof(header));
        }).then([&out] {
            return out.write(reinterpret_cast<const char*>(&nr_payload), 4);
        });
    }

private:
    actor_message_without_payload(const address &addr, uint8_t behv_tid, uint32_t src_s_id,
            uint32_t pr_id, message_type m_type)
        : actor_message(addr, behv_tid, src_s_id, pr_id, m_type) {}

    // Can only be called by network_delegater.
    actor_message_without_payload(const address &addr, uint8_t behv_tid,
            uint32_t src_s_id, uint32_t pr_id, message_type m_type, network_flag)
        : actor_message(addr, behv_tid, src_s_id, pr_id, m_type, network_flag{}) {}

    friend struct network_delegater;
};

template <typename T>
struct actor_message_with_payload final : public actor_message {
    T data;
    actor_message_with_payload(const actor_message_with_payload&) = delete;
    actor_message_with_payload(actor_message_with_payload&&) = delete;
    ~actor_message_with_payload() override = default;

    static actor_message_with_payload*
    make_actor_message(const address &addr, uint8_t behv_tid, T&& data, uint32_t src_s_id,
            uint32_t pr_id, message_type m_type) {
        static_assert(std::is_rvalue_reference<T&&>::value, "T&& should be rvalue reference.");
        return new actor_message_with_payload {
            addr, behv_tid, std::forward<T>(data), src_s_id, pr_id, m_type
        };
    }

    seastar::future<> serialize(seastar::output_stream<char> &out) final {
        return seastar::do_with(serializable_unit(), [&out, this] (serializable_unit &su) {
            data.dump_to(su);
            uint32_t msg_len = sizeof(header) + 4 + su.num_used * 4;
            for (uint32_t i = 0; i < su.num_used; ++i) {
                msg_len += su.bufs[i].size();
            }

            return out.write(reinterpret_cast<const char*>(&msg_len), 4).then([this, &out] {
                return out.write(reinterpret_cast<char*>(&hdr), sizeof(header));
            }).then([&out, &su] {
                return out.write(reinterpret_cast<const char*>(&su.num_used), 4);
            }).then([&out, &su] {
                auto range = boost::irange(0u, su.num_used);
                return seastar::do_for_each(range, [&out, &su] (unsigned idx) {
                    auto size = static_cast<uint32_t>(su.bufs[idx].size());
                    return out.write(reinterpret_cast<char*>(&size), 4).then([&out, &su, idx] {
                        return out.write(su.bufs[idx].get(), su.bufs[idx].size());
//                            return out.write(std::move(su.bufs[idx]));
                    });
                });
            });
        });
    }

private:
    actor_message_with_payload(const address &addr, uint8_t behv_tid, T&& data, uint32_t src_s_id,
            uint32_t pr_id, message_type m_type)
        : actor_message(addr, behv_tid, src_s_id, pr_id, m_type), data(std::forward<T>(data)) {}
};

template <>
struct actor_message_with_payload<serializable_unit> final : public actor_message {
    serializable_unit data;
    actor_message_with_payload(const actor_message_with_payload &other) = delete;
    actor_message_with_payload(actor_message_with_payload&& x) = delete;
    ~actor_message_with_payload() override = default;

    seastar::future<> serialize(seastar::output_stream<char> &out) final {
        // deprecated & useless.
        return seastar::make_ready_future<>();
    }

private:
    actor_message_with_payload(const address &addr, uint8_t behv_tid, serializable_unit&& data,
            uint32_t src_s_id, uint32_t pr_id, message_type m_type)
        : actor_message(addr, behv_tid, src_s_id, pr_id, m_type, network_flag{}),
        data(std::move(data)) {}

    friend struct network_delegater;
};

inline actor_message_without_payload*
make_system_message(address &addr, message_type m_type) {
    return actor_message_without_payload::make_actor_message(addr, 0, 0, 0, m_type);
}

inline actor_message_without_payload*
make_request_message(const address &addr, uint8_t behv_tid, uint32_t src_s_id,
        uint32_t pr_id, message_type m_type) {
    return actor_message_without_payload::make_actor_message(addr, behv_tid, src_s_id, pr_id, m_type);
}

inline actor_message_without_payload*
make_one_way_request_message(const address &addr, uint8_t behv_tid, uint32_t src_s_id,
        message_type m_type) {
    return actor_message_without_payload::make_actor_message(addr, behv_tid, src_s_id, 0, m_type);
}

template <typename T, typename =
    typename std::enable_if<std::is_rvalue_reference<T&&>::value>::type>
inline actor_message_with_payload<T>*
make_request_message(const address &addr, uint8_t behv_tid, T&& data, uint32_t src_s_id,
        uint32_t pr_id, message_type m_type) {
    return actor_message_with_payload<T>::make_actor_message(addr, behv_tid,
            std::forward<T>(data), src_s_id, pr_id, m_type);
}

template <typename T, typename =
    typename std::enable_if<std::is_rvalue_reference<T&&>::value>::type>
inline actor_message_with_payload<T>*
make_one_way_request_message(const address &addr, uint8_t behv_tid, T&& data,
        uint32_t src_s_id, message_type m_type) {
    return actor_message_with_payload<T>::make_actor_message(addr, behv_tid,
            std::forward<T>(data), src_s_id, 0, m_type);
}

inline actor_message_without_payload*
make_response_message(uint32_t s_id, uint32_t pr_id, message_type m_type) {
    address addr{};
    addr.length = GShardIdInBytes;
    memcpy(addr.data, &s_id, GShardIdInBytes);
    return actor_message_without_payload::make_actor_message(addr, 0, 0, pr_id, m_type);
}

template <typename T, typename = 
	typename std::enable_if<std::is_rvalue_reference<T&&>::value>::type>
inline actor_message_with_payload<T>*
make_response_message(uint32_t s_id, T&& data, uint32_t pr_id, message_type m_type) {
    address addr{};
    addr.length = GShardIdInBytes;
    memcpy(addr.data, &s_id, GShardIdInBytes);
    return actor_message_with_payload<T>::make_actor_message(addr, 0, std::forward<T>(data),
            0, pr_id, m_type);
}

std::string char_arr_to_str(const byte_t *addr, int len);

} // namespace brane
