//
// Created by everlighting on 2020/9/28.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"
#include "common/props_predictor.hh"

class i_q5_op_4_dedup_out;
class i_q5_op_6_group_localOrder;
class i_q5_op_7_globalSync_order;

class ANNOTATION(actor:reference) i_q5_op_3_dedup_friendStore_in : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void record_msgCreators_in_forum(vertex_forumId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q5_op_3_dedup_friendStore_in);
    // Destructor
    ACTOR_ITFC_DTOR(i_q5_op_3_dedup_friendStore_in);
};

class ANNOTATION(actor:implement) q5_op_3_dedup_friendStore_in : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void record_msgCreators_in_forum(vertex_forumId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q5_op_3_dedup_friendStore_in);
    // Destructor
    ACTOR_IMPL_DTOR(q5_op_3_dedup_friendStore_in);
    // Do work
    ACTOR_DO_WORK();

    static int64_t _min_date;

private:
    void filter_count();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;
    bool _at_stage2 = false;

    using ds_group_t = downstream_group_handler<i_q5_op_4_dedup_out, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
    using stage2_ds_group_t = downstream_group_handler<i_q5_op_6_group_localOrder, vertex_count_Batch>;
    const unsigned _stage2_ds_op_id = 6;
    stage2_ds_group_t* _stage2_ds_group_hdl = nullptr;
    using sync_t = downstream_handler<i_q5_op_7_globalSync_order, true>;
    sync_t* _sync_hdl = nullptr;

    std::unordered_set<int64_t> _friend_store{};

    class in_edge_prop_predictor: public storage::base_predicate {
    public:
        long prop_name_id = storage::berkeleydb_helper::getPropertyID("joinDate");
        LongPropCheck<std::greater<>> property_check;

        in_edge_prop_predictor()
            : property_check(LongPropCheck<std::greater<>>::get_from(q5_op_3_dedup_friendStore_in::_min_date)) {}

        bool operator()(const Dbt &value) override {
            return checkEdgePredicate(value, &prop_name_id, property_check);
        }
    };
    const std::string _in_label = "hasMember";

    std::vector<std::pair<int64_t, int64_t> > _relations{};
};

