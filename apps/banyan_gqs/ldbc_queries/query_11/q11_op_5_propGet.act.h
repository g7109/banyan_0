//
// Created by everlighting on 2020/10/10.
//
#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_q11_op_3_relationStore_dedup_out;
class i_q11_op_6_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q11_op_5_propGet : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q11_op_5_propGet);
    // Destructor
    ACTOR_ITFC_DTOR(i_q11_op_5_propGet);
};

class ANNOTATION(actor:implement) q11_op_5_propGet : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;

    // Constructor.
    ACTOR_IMPL_CTOR(q11_op_5_propGet);
    // Destructor
    ACTOR_IMPL_DTOR(q11_op_5_propGet);
    // Do work
    ACTOR_DO_WORK();

private:
    void forward_organisation_names();

private:
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = brane::global_shard_count();
    bool _notify_ds = false;

    using ds_t = downstream_handler<i_q11_op_3_relationStore_dedup_out, false>;
    const unsigned _ds_op_id = 3;
    ds_t* _ds_hdl = nullptr;
    using sync_t = downstream_handler<i_q11_op_6_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const long _prop_type_id = storage::berkeleydb_helper::getPropertyID("name");

    std::unordered_set<int64_t> _orgs_have_seen{};
    std::unordered_map<int64_t, std::string> _org_names;
};
