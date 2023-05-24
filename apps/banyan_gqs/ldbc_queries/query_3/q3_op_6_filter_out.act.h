//
// Created by everlighting on 2020/11/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "q3_op_7_compare_count.act.h"
#include "common/downstream_handlers.hh"

class i_q3_op_8_group_and_order;

class ANNOTATION(actor:reference) i_q3_op_6_filter_out : public brane::reference_base {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q3_op_6_filter_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q3_op_6_filter_out);
};

class ANNOTATION(actor:implement) q3_op_6_filter_out : public brane::stateful_actor {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q3_op_6_filter_out);
    // Destructor
    ACTOR_IMPL_DTOR(q3_op_6_filter_out);
    // Do work
    ACTOR_DO_WORK();

private:
    const std::string _out_label = "isLocatedIn";
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_q3_op_7_compare_count, vertex_intProp_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q3_op_8_group_and_order, true, q3_result_node_Batch>;
    sync_t* _sync_hdl = nullptr;

    int64_t _creationDate_propId = storage::berkeleydb_helper::getPropertyID("creationDate");
    int64_t _start_date;
    int64_t _end_date;
};
