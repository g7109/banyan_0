//
// Created by everlighting on 2020/10/9.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q11_op_5_propGet;
class i_q11_op_6_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q11_op_4_filter_dedup : public brane::reference_base {
public:
    void process(vertex_organisationId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_4_filter_dedup);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_4_filter_dedup);
};

class ANNOTATION(actor:implement) q11_op_4_filter_dedup : public brane::stateful_actor {
public:
    void process(vertex_organisationId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_4_filter_dedup);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_4_filter_dedup);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_group_t = downstream_group_handler<i_q11_op_5_propGet, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q11_op_6_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    class prop_predictor: public storage::base_predicate {
    public:
        long prop_name_id = storage::berkeleydb_helper::getPropertyID("name");
        StringPropCheck<true> property_check;

        bool operator()(Dbc *cursor, Dbt &key, Dbt &value) override {
            return checkVertexPredicate(cursor, key, value, &prop_name_id, property_check);
        }
    } _prop_checker;
    std::string _filter_label;

    std::unordered_set<int64_t> _successful_orgs{};
};

