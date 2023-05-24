#pragma once

#include <unordered_set>
#include <brane/util/common-utils.hh>
#include <brane/actor/reference_base.hh>
#include <brane/actor/actor_implementation.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"
#include "common/downstream_handlers.hh"

class i_q7_op_3_globalSync_order_limit;

class ANNOTATION(actor:reference) i_q7_op_3_propGet : public brane::reference_base {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    // Constructor.
    ACTOR_ITFC_CTOR(i_q7_op_3_propGet);
    // Destructor
    ACTOR_ITFC_DTOR(i_q7_op_3_propGet);
};

class ANNOTATION(actor:implement) q7_op_3_propGet : public brane::stateful_actor {
public:
    void process(VertexBatch&& input);
    void receive_eos(Eos&& eos);

    void initialize() override;
    void finalize() override;
    // Constructor.
    ACTOR_IMPL_CTOR(q7_op_3_propGet);
    // Destructor
    ACTOR_IMPL_DTOR(q7_op_3_propGet);
    // Do work
    ACTOR_DO_WORK();

private:
    using sync_t = downstream_handler<i_q7_op_3_globalSync_order_limit, true>;
    sync_t* _sync_hdl = nullptr;

    const int64_t _post_labelId = storage::berkeleydb_helper::getLabelID("Post");
    const int64_t _comment_labelId = storage::berkeleydb_helper::getLabelID("Comment");

    const int64_t _creationDate_propId = storage::berkeleydb_helper::getPropertyID("creationDate");
    const int64_t _content_propId = storage::berkeleydb_helper::getPropertyID("content");
    const int64_t _imageFile_propId = storage::berkeleydb_helper::getPropertyID("imageFile");
};