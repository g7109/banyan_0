#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"
#include "q6_op_5_globalSync_group_order.act.h"

class i_q6_op_3_filter_relationStore_out;

class ANNOTATION(actor:reference) i_q6_op_4_dedup_propGet : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void record_tag_counts(vertex_count_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q6_op_4_dedup_propGet);
    // Destructor
    ACTOR_ITFC_DTOR(i_q6_op_4_dedup_propGet);
};

class ANNOTATION(actor:implement) q6_op_4_dedup_propGet : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void record_tag_counts(vertex_count_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q6_op_4_dedup_propGet);
    // Destructor
    ACTOR_IMPL_DTOR(q6_op_4_dedup_propGet);
    // Do work
    ACTOR_DO_WORK();

private:
    void local_order();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _at_stage2 = false;

    using ds_group_t = downstream_group_handler<i_q6_op_3_filter_relationStore_out, VertexBatch>;
    unsigned _ds_op_id = 3;
    ds_group_t* _ds_group_hdl = nullptr;

    using sync_t = downstream_handler<i_q6_op_5_globalSync_group_order, true, count_name_Batch>;
    sync_t* _sync_hdl = nullptr;

    const int64_t _name_propId = storage::berkeleydb_helper::getPropertyID("name");
    std::string _filter_tag_name = "";

    std::unordered_set<int64_t> _tags_have_seen{};

    std::unordered_map<int64_t, unsigned> _tag_counts{};
    std::unordered_map<int64_t, std::string> _tag_names{};

    const unsigned _expect_num = 10;
    std::priority_queue<node, std::vector<node>, cmp> _order_heap;
};