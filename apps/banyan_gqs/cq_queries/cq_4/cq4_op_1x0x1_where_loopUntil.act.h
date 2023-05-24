//
// Created by everlighting on 2021/4/15.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"

class i_cq4_op_1x0x0_where_loopOut;
class i_cq4_op_1x1_whereBranchSync;

class ANNOTATION(actor:reference) i_cq4_op_1x0x1_where_loopUntil : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq4_op_1x0x1_where_loopUntil);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq4_op_1x0x1_where_loopUntil);
};

class ANNOTATION(actor:implement) cq4_op_1x0x1_where_loopUntil : public brane::stateless_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq4_op_1x0x1_where_loopUntil);
    // Destructor
    ACTOR_IMPL_DTOR(cq4_op_1x0x1_where_loopUntil);
    // Do work
    ACTOR_DO_WORK();

public:
    thread_local static std::unordered_set<int64_t> _successful_vertices;
    thread_local static std::unordered_set<int64_t> _failed_vertices;

private:
    const unsigned _max_loop_times = 4;
    unsigned _cur_loop_times = 0;

    std::unordered_set<int64_t> _expect_universities{};
    const std::string _workAt_label = "workAt";

    std::unordered_set<int64_t> _vertices_have_seen{};

    using next_loop_t = i_cq4_op_1x0x0_where_loopOut;
    next_loop_t* _next_loop_ref = nullptr;
    bool _next_loop_flag = false;

    using sync_t = i_cq4_op_1x1_whereBranchSync;
    sync_t* _sync_ref = nullptr;
    bool _have_sent = false;
    bool _sync_flag = false;
};
