# message ("CMake source dir is: ${CMAKE_SOURCE_DIR}")
# message ("CMake current source dir is: ${CMAKE_CURRENT_SOURCE_DIR}")

add_definitions(-DBANYAN_GQS_REPO="${CMAKE_CURRENT_SOURCE_DIR}")

set (ACTOR_CUSTOM_SYS_LIB_PATH ${CMAKE_SOURCE_DIR}/include)
set (USER_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/apps/banyan_gqs)
set (ACTOR_MODULD_DIR .)
include (${CMAKE_SOURCE_DIR}/actor-codegen-tool/ActorAutoGen.cmake)

generate_fake_autogen_sources (${CMAKE_CURRENT_SOURCE_DIR} )
file (GLOB AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} generated/*.cc)

include_directories(.)

set (berkeley_db_storage_files
        berkeley_db/storage/bdb_graph_handler.hh
        berkeley_db/storage/bdb_graph_store.hh
        berkeley_db/storage/bdb_graph_utils.hh
        berkeley_db/storage/bdb_graph_handler.cc
        berkeley_db/storage/bdb_graph_utils.cc
        berkeley_db/storage/bdb_graph_property_predicate.hh)

set (query_basic_files
        common/configs.hh
        common/configs.cc
        common/data_unit_type.hh
        common/db_initializer.hh
        common/downstream_handlers.hh
        common/logs.hh
        common/logs.cc
        common/path_eos.hh
        common/props_predictor.hh
        common/query_helpers.hh
        common/query_helpers.cc)

macro(RegisterLdbcQuery query_id)
    file (GLOB Q${query_id}_SRC_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            ldbc_queries/query_${query_id}/*.act.h
            ldbc_queries/query_${query_id}/*.act.cc)
    file (GLOB Q${query_id}_AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} generated/q${query_id}_*.cc)
    seastar_add_app (run_ic_${query_id}
            SOURCES
            ldbc_benchmark_runner/run_query_${query_id}.cc
            ldbc_queries/query_${query_id}/q${query_id}_executor.hh
            ${query_basic_files}
            ${berkeley_db_storage_files}
            ${Q${query_id}_SRC_FILES}
            ${Q${query_id}_AUTOGEN_SOURCES})
    target_link_libraries (app_run_ic_${query_id} PRIVATE db_cxx)
    add_dependencies (app_run_ic_${query_id} actor-autogen)
endmacro()
RegisterLdbcQuery(1)
RegisterLdbcQuery(2)
RegisterLdbcQuery(3)
RegisterLdbcQuery(4)
RegisterLdbcQuery(5)
RegisterLdbcQuery(6)
RegisterLdbcQuery(7)
RegisterLdbcQuery(8)
RegisterLdbcQuery(9)
RegisterLdbcQuery(10)
RegisterLdbcQuery(11)
RegisterLdbcQuery(12)

macro(RegisterE1Runner)
    file (GLOB E1_SRC_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            ldbc_queries/query_6/*.act.h
            ldbc_queries/query_6/*.act.cc)
    file (GLOB E1_AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            generated/q6_*.cc)
    seastar_add_app (e1_runner
            SOURCES
            evaluation_runner/e1_concurrent_scalability.cc
            ${query_basic_files}
            ${berkeley_db_storage_files}
            ${E1_SRC_FILES}
            ${E1_AUTOGEN_SOURCES})
    target_link_libraries (app_e1_runner PRIVATE db_cxx)
    add_dependencies (app_e1_runner actor-autogen)
endmacro()
RegisterE1Runner()

macro(RegisterE3Runner)
    file (GLOB E3_SRC_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            ldbc_queries/query_1/*.act.h
            ldbc_queries/query_1/*.act.cc
            ldbc_queries/query_9/*.act.h
            ldbc_queries/query_9/*.act.cc)
    file (GLOB E3_AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            generated/q1_*.cc
            generated/q9_*.cc)
    seastar_add_app (e3_runner
            SOURCES
            evaluation_runner/e3_concurrent_performance_isolation.cc
            ${query_basic_files}
            ${berkeley_db_storage_files}
            ${E3_SRC_FILES}
            ${E3_AUTOGEN_SOURCES})
    target_link_libraries (app_e3_runner PRIVATE db_cxx)
    add_dependencies (app_e3_runner actor-autogen)
endmacro()
RegisterE3Runner()

macro(RegisterE4Runner)
    file (GLOB E4_SRC_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            ldbc_queries/query_1/*.act.h
            ldbc_queries/query_1/*.act.cc
            ldbc_queries/query_10/*.act.h
            ldbc_queries/query_10/*.act.cc
            ldbc_queries/query_9/*.act.h
            ldbc_queries/query_9/*.act.cc)
    file (GLOB E4_AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            generated/q1_*.cc
            generated/q10_*.cc
            generated/q9_*.cc)
    seastar_add_app (e4_runner
            SOURCES
            evaluation_runner/e4_background_workload.cc
            ${query_basic_files}
            ${berkeley_db_storage_files}
            ${E4_SRC_FILES}
            ${E4_AUTOGEN_SOURCES})
    target_link_libraries (app_e4_runner PRIVATE db_cxx)
    add_dependencies (app_e4_runner actor-autogen)
endmacro()
RegisterE4Runner()

macro(RegisterE5Runner)
    file (GLOB E5_SRC_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            ldbc_queries/query_6/*.act.h
            ldbc_queries/query_6/*.act.cc)
    file (GLOB E5_AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            generated/q6_*.cc)
    seastar_add_app (e5_runner
            SOURCES
            evaluation_runner/e5_load_balancing.cc
            ${query_basic_files}
            ${berkeley_db_storage_files}
            ${E5_SRC_FILES}
            ${E5_AUTOGEN_SOURCES})
    target_link_libraries (app_e5_runner PRIVATE db_cxx)
    add_dependencies (app_e5_runner actor-autogen)
endmacro()
RegisterE5Runner()

macro(RegisterCQ query_id)
    file (GLOB CQ_${query_id}_SRC_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
            cq_queries/cq_${query_id}/*.act.h
            cq_queries/cq_${query_id}/*.act.cc)
    file (GLOB CQ_${query_id}_AUTOGEN_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} generated/cq${query_id}_*.cc)
    seastar_add_app (run_cq_${query_id}
            SOURCES
            cq_runner/run_cq_${query_id}.cc
            cq_queries/cq_${query_id}/cq${query_id}_executor.hh
            common/schedule_helpers.cc
            common/schedule_helpers.hh
            ${query_basic_files}
            ${berkeley_db_storage_files}
            ${CQ_${query_id}_SRC_FILES}
            ${CQ_${query_id}_AUTOGEN_SOURCES})
    target_link_libraries (app_run_cq_${query_id} PRIVATE db_cxx)
    add_dependencies (app_run_cq_${query_id} actor-autogen)
endmacro()
RegisterCQ(1)
RegisterCQ(2)
RegisterCQ(3)
RegisterCQ(3x)
RegisterCQ(4)
RegisterCQ(4x)
RegisterCQ(5)
RegisterCQ(5x)
RegisterCQ(6)
RegisterCQ(6x)