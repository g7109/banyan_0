//
// Created by everlighting on 2021/4/8.
//
#pragma once

#include <brane/actor/reference_base.hh>

uint16_t get_query_actor_group_typeId();
brane::scope_builder get_query_scope_builder(unsigned shard_id, unsigned query_id);

void enter_loop_scope(brane::scope_builder* current_scope, unsigned loop_scope_id);
void enter_loop_instance_scope(brane::scope_builder* current_scope, unsigned loop_instance_id);

void enter_where_scope(brane::scope_builder* current_scope, unsigned where_scope_id);
void enter_where_branch_scope(brane::scope_builder* current_scope, unsigned where_branch_id);