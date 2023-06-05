# set (LIBCLANG_PATH "/usr/lib" CACHE STRING "libclang path in system.")
# set (ACTOR_MODULD_DIR "" CACHE STRING "your actor module dir relative to your cxx project.")
# set (USER_INCLUDE_DIR "" CACHE STRING "your additional user-define include dir for codegen.")

message ("Cmake libclang path: ${LIBCLANG_PATH}")

# Input argument is your actor module directory
function (generate_fake_autogen_sources)
	set (GEN_FOLDER generated)
	file (REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/${GEN_FOLDER})
	file (MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${GEN_FOLDER})

	set (ACTOR_DIRS ${ARGV0})
	file (GLOB_RECURSE ACTOR_ACT_H RELATIVE ${ACTOR_DIRS} *.act.h)
	message ("Autogen header is ${ACTOR_ACT_H}")
	foreach (HEADER ${ACTOR_ACT_H})
		get_filename_component (BASENAME ${HEADER} NAME_WE)
		execute_process (COMMAND ${CMAKE_COMMAND} -E
			touch ${GEN_FOLDER}/${BASENAME}.act.autogen.cc
			WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
	endforeach()
endfunction()

set (OPTIONAL_SYSPATH)
if (DEFINED ACTOR_CUSTOM_SYS_LIB_PATH)
    set (OPTIONAL_SYSPATH ${ACTOR_CUSTOM_SYS_LIB_PATH})
endif()

add_custom_target (actor-autogen)
add_custom_command (TARGET actor-autogen POST_BUILD
    COMMAND python3 actor_codegen.py --libclang-dir=${LIBCLANG_PATH}
    --project-dir=${CMAKE_CURRENT_SOURCE_DIR}
    --actor-mod-dir=${ACTOR_MODULE_DIR}
    --sysactor-include=${OPTIONAL_SYSPATH}
    --user-include=${USER_INCLUDE_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR})
