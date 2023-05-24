#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q1_op_3_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q1_op_2_loopLeave_dedup_filter : public brane::reference_base {
public:
    void process(vertex_pathLen_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q1_op_2_loopLeave_dedup_filter);
    // Destructor
    ACTOR_ITFC_DTOR(i_q1_op_2_loopLeave_dedup_filter);
};

class ANNOTATION(actor:implement) q1_op_2_loopLeave_dedup_filter : public brane::stateful_actor {
public:
    void process(vertex_pathLen_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q1_op_2_loopLeave_dedup_filter);
    // Destructor
    ACTOR_IMPL_DTOR(q1_op_2_loopLeave_dedup_filter);
    // Do work
    ACTOR_DO_WORK();

private:
    std::unordered_set<int64_t> _vertices_have_seen{};

    class prop_predictor: public storage::base_predicate {
    public:
        long firstName_type_id = storage::berkeleydb_helper::getPropertyID("firstName");
        StringPropCheck<true> property_check;

        bool operator()(Dbc *cursor, Dbt &key, Dbt &value) override {
            return checkVertexPredicate(cursor, key, value, &firstName_type_id, property_check);
        }
    };
    prop_predictor _prop_checker;
    const long _lastName_type_id = storage::berkeleydb_helper::getPropertyID("lastName");

    using ds_and_sync_t = downstream_handler<i_q1_op_3_globalSync_order_limit, true, vertex_pathLen_StrProp_Batch>;
    ds_and_sync_t* _ds_sync_hdl = nullptr;
};


