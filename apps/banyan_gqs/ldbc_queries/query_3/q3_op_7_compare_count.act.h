//
// Created by everlighting on 2020/11/13.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/configs.hh"
#include "common/data_unit_type.hh"
#include "common/props_predictor.hh"
#include "common/downstream_handlers.hh"

class i_q3_op_8_group_and_order;

class ANNOTATION(actor:reference) i_q3_op_7_compare_count : public brane::reference_base {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q3_op_7_compare_count);
    // Destructor
    ACTOR_ITFC_DTOR(i_q3_op_7_compare_count);
};

class ANNOTATION(actor:implement) q3_op_7_compare_count : public brane::stateful_actor {
public:
    void process(vertex_intProp_Batch&& vertices);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q3_op_7_compare_count);
    // Destructor
    ACTOR_IMPL_DTOR(q3_op_7_compare_count);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    std::string _country_x;
    std::string _country_y;
    const long _prop_type_id = storage::berkeleydb_helper::getPropertyID("name");

    std::unordered_set<int64_t> _failed_locations{};
    std::unordered_map<int64_t, std::pair<unsigned, unsigned> > _x_y_locations{};
    
    using sync_t = downstream_handler<i_q3_op_8_group_and_order, true, q3_result_node_Batch>;
    sync_t* _sync_hdl = nullptr;

};

