#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/props_predictor.hh"
#include "common/downstream_handlers.hh"
#include <unordered_set>

class i_q2_op_4_order_and_limit_display;

class ANNOTATION(actor:reference) i_q2_op_3_vertex_filter : public brane::reference_base {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q2_op_3_vertex_filter);
    // Destructor
    ACTOR_ITFC_DTOR(i_q2_op_3_vertex_filter);
};

class ANNOTATION(actor:implement) q2_op_3_vertex_filter : public brane::stateful_actor {
public:
    void process(VertexBatch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q2_op_3_vertex_filter);
    // Destructor
    ACTOR_IMPL_DTOR(q2_op_3_vertex_filter);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _query_id;
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using sync_t = downstream_handler<i_q2_op_4_order_and_limit_display, true, vertex_intProp_Batch>;
    sync_t* _sync_hdl = nullptr;

    std::string _vertex_label = "Post";
    std::string prop_name = "creationDate";
    const long _prop_type_id = storage::berkeleydb_helper::getPropertyID(prop_name);

    class q2_op_3_predicate: public storage::base_predicate {
    public:
        long prop_name_id = storage::berkeleydb_helper::getPropertyID("creationDate");
        LongPropCheck<std::less_equal<>> property_check;

        bool operator()(Dbc *cursor, Dbt &key, Dbt &value) override {
            return checkVertexPredicate(cursor, key, value, &prop_name_id, property_check);
        }
    };
    q2_op_3_predicate _prop_checker;
};


