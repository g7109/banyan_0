//
// Created by everlighting on 2020/10/9.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q11_op_3_relationStore_dedup_out;

class ANNOTATION(actor:reference) i_q11_op_2_dedup_out : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_2_dedup_out);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_2_dedup_out);
};

class ANNOTATION(actor:implement) q11_op_2_dedup_out : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_2_dedup_out);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_2_dedup_out);
    // Do work
    ACTOR_DO_WORK();

    static int64_t _work_from_year;

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_group_t = downstream_group_handler<i_q11_op_3_relationStore_dedup_out, work_relation_Batch>;
    ds_group_t* _ds_group_hdl = nullptr;

    std::unordered_set<int64_t> _person_have_seen{};

    const std::string _out_label = "workAt";
    class out_edge_prop_predictor: public storage::base_predicate {
    public:
        long prop_name_id = storage::berkeleydb_helper::getPropertyID("workFrom");
        LongPropCheck<std::less<>> property_check;

        out_edge_prop_predictor()
            : property_check(LongPropCheck<std::less<>>::get_from(_work_from_year)) {}
        bool operator()(const Dbt &value) override {
            return checkEdgePredicate(value, &prop_name_id, property_check);
        }
    };
    const long _edge_prop_type_id = storage::berkeleydb_helper::getPropertyID("workFrom");
};