//
// Created by everlighting on 2021/4/19.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "common/downstream_handlers.hh"
#include "berkeley_db/storage/bdb_graph_handler.hh"

class i_cq6x_op_1x5_loopUntil;

class ANNOTATION(actor:reference) i_cq6x_op_1x4_loopFilter : public brane::reference_base {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_cq6x_op_1x4_loopFilter);
    // Destructor
    ACTOR_ITFC_DTOR(i_cq6x_op_1x4_loopFilter);
};

class ANNOTATION(actor:implement) cq6x_op_1x4_loopFilter : public brane::stateless_actor {
public:
    void process(vertex_friendId_Batch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(cq6x_op_1x4_loopFilter);
    // Destructor
    ACTOR_IMPL_DTOR(cq6x_op_1x4_loopFilter);
    // Do work
    ACTOR_DO_WORK();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();

    int64_t _name_propId = storage::berkeleydb_helper::getPropertyID("name");
    const std::string _tag_type_name = "Country";

    std::unordered_set<uint64_t> _persons;

    using ds_group_t = downstream_group_handler<i_cq6x_op_1x5_loopUntil, VertexBatch>;
    ds_group_t* _ds_group_hdl = nullptr;
};
