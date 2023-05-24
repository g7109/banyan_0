#pragma once

#include <brane/actor/actor_message.hh>
#include <brane/core/root_actor_group.hh>

namespace brane {

struct actor_client_helper {
    static inline
    void send_request_msg(const address &addr, uint8_t behv_tid, uint32_t src_s_id) {
        auto *msg = make_one_way_request_message(addr, behv_tid, src_s_id, message_type::USER);
        actor_engine().send(msg);
    }

    template <typename T, typename =
        typename std::enable_if<std::is_rvalue_reference<T&&>::value>::type>
    static inline
    void send_request_msg(const address &addr, uint8_t behv_tid, T&& data, uint32_t src_s_id) {
        auto *msg = make_one_way_request_message(addr, behv_tid, std::forward<T>(data),
                src_s_id, message_type::USER);
        actor_engine().send(msg);
    }

    template <typename retT, typename T, typename =
        typename std::enable_if<std::is_rvalue_reference<T&&>::value>::type>
    static inline
    seastar::future<retT> send_request_msg(const address &addr, uint8_t behv_tid, T&& data,
            uint32_t src_s_id, actor_base* exec_ctx) {
        auto pr_id = actor_engine()._response_pr_manager.acquire_pr();
        auto *msg = make_request_message(addr, behv_tid, std::forward<T>(data), src_s_id,
                pr_id, message_type::USER);
        return actor_engine().send(msg).then([pr_id, exec_ctx] {
            return actor_engine()._response_pr_manager.get_future(pr_id).then_wrapped(
                    [pr_id] (seastar::future<actor_message*> fut) {
                actor_engine()._response_pr_manager.remove_pr(pr_id);
                if (__builtin_expect(fut.failed(), false)) {
                    return seastar::make_exception_future<retT>(fut.get_exception());
                }
                auto a_msg = fut.get0();
                if (!a_msg->hdr.from_network) {
                    auto res_data = std::move(reinterpret_cast<
                        actor_message_with_payload<retT>*>(a_msg)->data);
                    delete a_msg;
                    return seastar::make_ready_future<retT>(std::move(res_data));
                }
                else {
                    auto *ori_msg = reinterpret_cast<
                        actor_message_with_payload<serializable_unit>*>(a_msg);
                    auto res_data = retT::load_from(std::move(ori_msg->data));
                    delete ori_msg;
                    return seastar::make_ready_future<retT>(std::move(res_data));
                }
            }, exec_ctx);
        });
    }

    template <typename retT>
    static inline
    seastar::future<retT> send_request_msg(const address &addr, uint8_t behv_tid,
            uint32_t src_s_id, actor_base* exec_ctx) {
        auto pr_id = actor_engine()._response_pr_manager.acquire_pr();
        auto *msg = make_request_message(addr, behv_tid, src_s_id, pr_id, message_type::USER);
        return actor_engine().send(msg).then([pr_id, exec_ctx] {
            return actor_engine()._response_pr_manager.get_future(pr_id).then_wrapped(
                    [pr_id] (seastar::future<actor_message*> fut) {
                actor_engine()._response_pr_manager.remove_pr(pr_id);
                if (__builtin_expect(fut.failed(), false)) {
                    return seastar::make_exception_future<retT>(fut.get_exception());
                }
                auto a_msg = fut.get0();
                if (!a_msg->hdr.from_network) {
                    auto res_data = std::move(reinterpret_cast<
                        actor_message_with_payload<retT>*>(a_msg)->data);
                    delete a_msg;
                    return seastar::make_ready_future<retT>(std::move(res_data));
                }
                else {
                    auto *ori_msg = reinterpret_cast<
                        actor_message_with_payload<serializable_unit>*>(a_msg);
                    auto res_data = retT::load_from(std::move(ori_msg->data));
                    delete ori_msg;
                    return seastar::make_ready_future<retT>(std::move(res_data));
                }
            }, exec_ctx);
        });
    }
};

} // namespace brane
