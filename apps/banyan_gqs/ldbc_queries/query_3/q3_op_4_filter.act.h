//
// Created by everlighting on 2020/11/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "q3_op_5_in.act.h"
#include "common/downstream_handlers.hh"

class i_q3_op_8_group_and_order;

class ANNOTATION(actor:reference) i_q3_op_4_filter : public brane::reference_base {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q3_op_4_filter);
    // Destructor
    ACTOR_ITFC_DTOR(i_q3_op_4_filter);
};

class ANNOTATION(actor:implement) q3_op_4_filter : public brane::stateful_actor {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q3_op_4_filter);
    // Destructor
    ACTOR_IMPL_DTOR(q3_op_4_filter);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_q3_op_5_in, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q3_op_8_group_and_order, true, q3_result_node_Batch>;
    sync_t* _sync_hdl = nullptr;

    int64_t _name_propId = storage::berkeleydb_helper::getPropertyID("name");
    std::string _country_x;
    std::string _country_y;

    std::unordered_set<int64_t> _x_or_y{};
    std::unordered_set<int64_t> _neither_x_or_y{};
};
