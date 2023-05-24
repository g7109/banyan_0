#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/downstream_handlers.hh"
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "cq3x_op_6_globalSync.act.h"

class ANNOTATION(actor:reference) i_cq3x_op_5_cmp_dedup : public brane::reference_base {
public:
    void process(vertex_intProp_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3x_op_5_cmp_dedup);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3x_op_5_cmp_dedup);
};

class ANNOTATION(actor:implement) cq3x_op_5_cmp_dedup : public brane::stateful_actor {
public:
    void process(vertex_intProp_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3x_op_5_cmp_dedup);
    // Destructor
    ACTOR_IMPL_DTOR(cq3x_op_5_cmp_dedup);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    int64_t _name_propId = storage::berkeleydb_helper::getPropertyID("name");
    std::unordered_set<uint64_t> _persons;
    const std::string _tag_type_name = "Country";

    std::priority_queue<int64_t, std::vector<int64_t>, std::less<int64_t> > _order_heap;
    const unsigned _expect_num = 10;

    using ds_t = downstream_handler<i_cq3x_op_6_globalSync, true, VertexBatch>;
    ds_t* _ds_hdl = nullptr;
};
