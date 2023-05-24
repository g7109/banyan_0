//
// Created by everlighting on 2021/4/8.
//

#include <brane/actor/actor_implementation.hh>
#include "common/schedule_helpers.hh"
#include "common/configs.hh"

uint16_t get_query_actor_group_typeId() {
    if (cq_query_policy == DFS) {
        return brane::GDFSActorGroupTypeId;
    } else if (cq_query_policy == BFS) {
        return brane::GBFSActorGroupTypeId;
    }
    return brane::GActorGroupTypeId;
}

brane::scope_builder get_query_scope_builder(unsigned shard_id, unsigned query_id) {
    brane::scope_builder builder(shard_id);
    if (cq_query_policy == FIFO) {
        builder.enter_sub_scope(brane::scope<brane::actor_group<brane::fifo_cmp> >(query_id));
    } else if (cq_query_policy == DFS) {
        builder.enter_sub_scope(brane::scope<brane::actor_group<brane::large_better> >(query_id));
    } else if (cq_query_policy == BFS) {
        builder.enter_sub_scope(brane::scope<brane::actor_group<brane::small_better> >(query_id));
    } else {
        abort();
    }
    return builder;
}

void enter_loop_scope(brane::scope_builder* current_scope, unsigned loop_scope_id) {
    if (cq_loop_policy == FIFO) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::fifo_cmp> >(loop_scope_id));
    } else if (cq_loop_policy == DFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::large_better> >(loop_scope_id));
    } else if (cq_loop_policy == BFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::small_better> >(loop_scope_id));
    } else {
        abort();
    }
}

void enter_loop_instance_scope(brane::scope_builder* current_scope, unsigned loop_instance_id) {
    if (cq_loop_instance_policy == FIFO) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::fifo_cmp> >(loop_instance_id));
    } else if (cq_loop_instance_policy == DFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::large_better> >(loop_instance_id));
    } else if (cq_loop_instance_policy == BFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::small_better> >(loop_instance_id));
    } else {
        abort();
    }
}

void enter_where_scope(brane::scope_builder* current_scope, unsigned where_scope_id) {
    if (cq_where_policy == FIFO) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::fifo_cmp> >(where_scope_id));
    } else if (cq_where_policy == DFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::large_better> >(where_scope_id));
    } else if (cq_where_policy == BFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::small_better> >(where_scope_id));
    } else {
        abort();
    }
}

void enter_where_branch_scope(brane::scope_builder* current_scope, unsigned where_branch_id) {
    if (cq_where_branch_policy == FIFO) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::fifo_cmp> >(where_branch_id));
    } else if (cq_where_branch_policy == DFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::large_better> >(where_branch_id));
    } else if (cq_where_branch_policy == BFS) {
        current_scope->enter_sub_scope(brane::scope<brane::actor_group<brane::small_better> >(where_branch_id));
    } else {
        abort();
    }
}