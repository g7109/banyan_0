#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"
#include "q9_op_4_globalSync_order_limit.act.h"

class ANNOTATION(actor:reference) i_q9_op_3_filter_localOrder : public brane::reference_base {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q9_op_3_filter_localOrder);
    // Destructor
    ACTOR_ITFC_DTOR(i_q9_op_3_filter_localOrder);
};

class ANNOTATION(actor:implement) q9_op_3_filter_localOrder : public brane::stateful_actor {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q9_op_3_filter_localOrder);
    // Destructor
    ACTOR_IMPL_DTOR(q9_op_3_filter_localOrder);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward_order_results();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    using ds_and_sync_t = downstream_handler<i_q9_op_4_globalSync_order_limit, true>;
    ds_and_sync_t* _ds_sync_hdl = nullptr;

    const int64_t _creationDate_propId = storage::berkeleydb_helper::getPropertyID("creationDate");
    int64_t _filter_creationDate;
    std::less<> _compare;

    std::priority_queue<heap_node, std::vector<heap_node>, cmp> _order_heap;
    const unsigned _expect_num = 20;
};