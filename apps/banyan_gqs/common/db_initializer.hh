//
// Created by everlighting on 2020/11/3.
//
#pragma once

#include <chrono>
#include <brane/core/shard-config.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include "berkeley_db/storage/bdb_graph_handler.hh"

inline seastar::future<> init_graph_store() {
    storage::berkeleydb_helper::configure_maps();
    return seastar::parallel_for_each(boost::irange<unsigned>(0u, brane::local_shard_count()),
            [] (unsigned local_shard_id) {
        return seastar::smp::submit_to(local_shard_id, [] {
            storage::bdb_graph_handler::init_graph();
        });
    }).then([] {
        return seastar::sleep(std::chrono::seconds(1));
    });
}
