//
// Created by everlighting on 2021/4/13.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "berkeley_db/storage/bdb_graph_handler.hh"

class i_cq3_op_3x4_whereBranchSync;

class ANNOTATION(actor:reference) i_cq3_op_3x3_whereFilter : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq3_op_3x3_whereFilter);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq3_op_3x3_whereFilter);
};

class ANNOTATION(actor:implement) cq3_op_3x3_whereFilter : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(PathEos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq3_op_3x3_whereFilter);
    // Destructor
    ACTOR_IMPL_DTOR(cq3_op_3x3_whereFilter);
    // Do work
    ACTOR_DO_WORK();

private:
    using sync_t =  i_cq3_op_3x4_whereBranchSync;
    const unsigned _sync_op_id = 4;
    sync_t* _sync_ref = nullptr;

    const int64_t _name_propId = storage::berkeleydb_helper::getPropertyID("name");
    std::string _expect_type_name = "Country";
    bool _found = false;
};
