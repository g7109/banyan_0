#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/configs.hh"
#include "common/downstream_handlers.hh"

class ANNOTATION(actor:reference) i_cq1_op_3_globalSync_limit : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq1_op_3_globalSync_limit);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq1_op_3_globalSync_limit);
};

class ANNOTATION(actor:implement) cq1_op_3_globalSync_limit : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq1_op_3_globalSync_limit);
    // Destructor
    ACTOR_IMPL_DTOR(cq1_op_3_globalSync_limit);
    // Do work
    ACTOR_DO_WORK();

private:
    void finish_query();
    void log_results();
    void cq_end_action();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    bool _cancelled = false;

    std::unordered_set<int64_t> _friends{};
    unsigned _limit_num;
};
