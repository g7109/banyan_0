//
// Created by everlighting on 2020/11/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q4_op_4_filter_propGet;
class i_q4_op_5_globalSync_order;

class ANNOTATION(actor:reference) i_q4_op_3_parallel_filter_out : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q4_op_3_parallel_filter_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q4_op_3_parallel_filter_out);
};

class ANNOTATION(actor:implement) q4_op_3_parallel_filter_out : public brane::stateful_actor {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q4_op_3_parallel_filter_out);
    // Destructor
    ACTOR_IMPL_DTOR(q4_op_3_parallel_filter_out);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using side_ds_group_t = downstream_group_handler<i_q4_op_4_filter_propGet, VertexBatch>;
    side_ds_group_t* _side_ds_group_hdl = nullptr;
    using ds_group_t = downstream_group_handler<i_q4_op_4_filter_propGet, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q4_op_5_globalSync_order, true>;
    sync_t* _sync_hdl = nullptr;

    class side_prop_predictor: public storage::base_predicate {
    public:
        long prop_name_id = storage::berkeleydb_helper::getPropertyID("creationDate");
        LongPropCheck<std::less<>> property_check;

        bool operator()(Dbc *cursor, Dbt &key, Dbt &value) override {
            return checkVertexPredicate(cursor, key, value, &prop_name_id, property_check);
        }
    } _side_prop_checker;

    class prop_predictor: public storage::base_predicate {
    public:
        long prop_name_id = storage::berkeleydb_helper::getPropertyID("creationDate");
        LongPropInsideIntervalCheck property_check;

        bool operator()(Dbc *cursor, Dbt &key, Dbt &value) override {
            return checkVertexPredicate(cursor, key, value, &prop_name_id, property_check);
        }
    } _prop_checker;

    std::string _filter_label = "Post";

    const std::string _out_label = "hasTag";
};

