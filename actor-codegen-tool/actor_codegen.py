#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys
import os
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWUSR
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

import shutil
import argparse
import clang.cindex
from clang.cindex import CursorKind
from clang.cindex import Config

from multiprocessing import Pool, cpu_count

class ActorRegister:
    def __init__(self, actor_type_id):
        self.actor_id = actor_type_id

    def register(self, actor_name, to_reflect):
        _, _, ns_list = to_reflect[0]
        actor_class_name = actor_name
        if len(ns_list) > 0:
            namespace = "::".join([ns for ns in ns_list])
            actor_class_name = namespace + "::" + actor_name

        stmt = "\n" + \
               "namespace auto_registeration {{" \
               "\n\nbrane::registration::actor_registration<{cls_name}> _{name}_auto_registeration({id});\n\n" \
               "}} // namespace auto_registeration\n".format(cls_name=actor_class_name, name=actor_name, id=self.actor_id)
        return self.actor_id, stmt

class ActorBehaviors:
    def __init__(self):
        self.behv_set = set()
    
    def build(self, methods):
        for method in methods:
            self.behv_set.add(method.spelling)

    def find(self, method_name):
        if method_name in self.behv_set:
            self.behv_set.remove(method_name)
            return True
        return False

    def is_find_all(self):
        if len(self.behv_set) == 0:
            return True
        return False

def check_return_type(node):
    for child in node.get_children():
        print(child.kind)
    print("After check_return_type")
    return None

def check_actor_attribute(node):
    for child in node.get_children():
        if child.kind == CursorKind.ANNOTATE_ATTR and \
                child.spelling.startswith('actor:'):
            return child.spelling[len('actor:'):]
    return None

def traverse(node, to_reflect, filepath, ns_list):
    ''' Traverse the AST tree.
    '''
    if node.kind in [CursorKind.CLASS_DECL, CursorKind.CLASS_TEMPLATE,
                     CursorKind.STRUCT_DECL]:
        attr = check_actor_attribute(node)
        if attr and node.location.file.name == filepath:
            # print("find actor node.spelling : {}".format(node.spelling))
            to_reflect.append((attr, node, tuple(ns_list)))

    if node.kind in [CursorKind.TRANSLATION_UNIT, CursorKind.NAMESPACE]:
        if node.kind == CursorKind.NAMESPACE:
            ns_list.append(node.spelling)
            # print("append node.spelling : {}".format(node.spelling))
        for child in node.get_children():
            traverse(child, to_reflect, filepath, ns_list)
        
        if node.kind == CursorKind.NAMESPACE:
            # print("pop node.spelling : {}".format(node.spelling))
            ns_list.pop()
            # print("After pop ns_list: {}".format(ns_list))

def traverse_reference_methods(reference):
    ctor_def = ""
    methods = []
    for child in reference.get_children():
        if child.kind == CursorKind.CXX_METHOD:
            # print(child.kind, ":", child.spelling, ":", child.result_type.spelling)
            # check_return_type(child)
            methods.append(child)
        elif child.kind == CursorKind.CONSTRUCTOR:
            ctor_def = "{class_name}::{class_name}(brane::actor_base *exec)\n\t\t" \
                       ": brane::reference_base(k_actor_type_id, exec) {{}}\n\n".format(class_name=child.spelling)
    return methods, ctor_def

def traverse_implement_methods(implements):
    methods = []
    base_class = ""
    current_class = ""
    for child in implements.get_children():
        # print(child.kind, ":", child.spelling)
        if child.kind == CursorKind.CXX_METHOD and child.spelling != "do_work":
            methods.append(child)
        elif child.kind == CursorKind.CONSTRUCTOR:
            current_class = child.spelling
        elif child.kind == CursorKind.CXX_BASE_SPECIFIER:
            base_class = child.get_definition().spelling
    ctor_def = "{class_name}::{class_name}(brane::actor_base *exec_ctx, \n\t " \
               "const brane::byte_t *addr) \n\t" \
               ": {base_class_name}(exec_ctx, addr) {{}}\n\n".format(
        class_name=current_class,
        base_class_name=base_class)
    return methods, ctor_def

def check_and_extract_type_from_futureT(result_name):
    left = 0
    left_found = False
    right = 0
    PairMatch = 0
    for i in range(len(result_name)):
        if result_name[i] == "<":
            PairMatch += 1
            if not left_found:
                left_found = True
                left = i
        elif result_name[i] == ">":
            right = i
            PairMatch -= 1
    if PairMatch != 0:
        raise RuntimeError("Bracket pairs don't match in {}\n", result_name)
    elif left < 6:
        raise RuntimeError("Wrong result name: {}.\n", result_name)
    elif result_name[left-6 : left] != "future":
        raise RuntimeError("Wrong result name: {}, not a future<> type!\n", result_name)
    return result_name[left+1 : right]

def generate_reference_def(node, actor_behvs):
    reference_def = ""
    methods, ctor_def = traverse_reference_methods(node)
    enum_type = "enum : uint8_t {{ \n\t{cval} \n}};\n\n".format(
        cval=",\n\t".join(["k_{m}".format(m=m.spelling) for m in methods]))
    actor_behvs.build(methods)
    definitions = []

    for mthd in methods:
        assert(len(mthd.type.argument_types()) < 2)
        optional_args = ""
        optional_arg_type = ""
        optional_data = ""
        if len(mthd.type.argument_types()) > 0:
            optional_args = "{}data".format(mthd.type.argument_types()[0].spelling)
            optional_arg_type = ", {}".format(mthd.type.argument_types()[0].spelling[:-3])
            optional_data = ", std::forward<{}>(data)".format(mthd.type.argument_types()[0].spelling[:-3])

        sendmsg_func = ""
        if mthd.result_type.spelling == "void":
            sendmsg_func = "auto src_shard_id = brane::global_shard_id();\n\t" \
                           "brane::actor_client_helper::send_request_msg(\n\t\t\t" \
                           "addr, k_{behv}{opt_data}, src_shard_id);".format(
                behv=mthd.spelling,
                opt_data=optional_data)
        else:
            result_type = check_and_extract_type_from_futureT(mthd.result_type.spelling)
            sendmsg_func = "auto exec_ctx = get_execution_context();\n\t" \
                           "auto src_shard_id = brane::global_shard_id();\n\t" \
                           "return brane::actor_client_helper::send_request_msg<{result_type}{input_arg_type}>(\n\t\t\t" \
                           "addr, k_{behv}{opt_data}, src_shard_id, exec_ctx);".format(
                result_type=result_type,
                input_arg_type=optional_arg_type,
                behv=mthd.spelling,
                opt_data=optional_data)

        definitions.append("{ret_type} {class_name}::{func_name}({arg_type}) {{ \n\t{smsg}\n}}\n".format(
            ret_type=mthd.result_type.spelling,
            class_name=node.spelling,
            arg_type=optional_args,
            smsg=sendmsg_func,
            func_name=mthd.spelling))
    reference_def = enum_type + ctor_def + "\n".join([d for d in definitions]) + "\n"
    return reference_def

def generate_implementation_def(node, actor_behvs):
    # TODO: parse
    methods, ctor = traverse_implement_methods(node)
    behvs = ""
    for method in methods:
        if not actor_behvs.find(method.spelling):
            continue

        if method.result_type.spelling == "void":
            if(len(method.type.argument_types()) != 0):
                behvs += "\t\tcase k_{behv_name}: {{\n\t\t\tif (!msg->hdr.from_network) {{\n\t\t\t\t" \
                         "{behv_name}(\n\t\t\t\t\t" \
                         "std::move(reinterpret_cast<\n\t\t\t\t\t" \
                         "brane::actor_message_with_payload<{input_type}>*>(msg)->data));\n\t\t\t" \
                         "}} else {{\n\t\t\t\t" \
                         "auto *ori_msg = reinterpret_cast<brane::actor_message_with_payload<brane::serializable_unit>*>(msg);\n\t\t\t\t" \
                         "{behv_name}({input_type}::load_from(std::move(ori_msg->data)));\n\t\t\t"\
                         "}}\n\t\t\t"\
                         "return seastar::make_ready_future<brane::stop_reaction>(brane::stop_reaction::no);\n\t\t"\
                         "}} \n".format(
                    behv_name=method.spelling,
                    input_type=method.type.argument_types()[0].spelling[:-3])
            else:
                behvs += "\t\tcase k_{behv_name}: {{\n\t\t\t{behv_name}();\n\t\t\t" \
                         "return seastar::make_ready_future<brane::stop_reaction>(brane::stop_reaction::no);\n\t\t" \
                         "}} \n".format(
                    behv_name=method.spelling)
        else:
            apply_action = "return {behv}".format(behv=method.spelling)
            optional_args = ""
            result_type = check_and_extract_type_from_futureT(method.result_type.spelling)

            if (len(method.type.argument_types()) != 0):
                apply_action = "static auto apply_{behv} = [] (brane::actor_message *a_msg, {class_name} *self) {{\n\t\t\t\t" \
                               "if (!a_msg->hdr.from_network) {{\n\t\t\t\t\t" \
                               "return self->{behv}(std::move(reinterpret_cast<brane::actor_message_with_payload<{input_type}>*>(a_msg)->data));\n\t\t\t\t" \
                               "}} else {{\n\t\t\t\t\t" \
                               "auto *ori_msg = reinterpret_cast<brane::actor_message_with_payload<brane::serializable_unit>*>(a_msg);\n\t\t\t\t\t" \
                               "return self->{behv}({input_type}::load_from(std::move(ori_msg->data)));\n\t\t\t\t" \
                               "}}\n\t\t\t" \
                               "}};\n\t\t\t" \
                               "return apply_{behv}".format(
                    behv=method.spelling,
                    input_type=method.type.argument_types()[0].spelling[:-3],
                    class_name=node.spelling)
                optional_args = "msg, this"

            behvs += "\t\tcase k_{behv_name}: {{\n\t\t\t{action}({opt_arg}).then([msg] ({result_type} result) {{\n\t\t\t\t" \
                     "auto *response_msg = brane::make_response_message(msg->hdr.src_shard_id, std::move(result), msg->hdr.pr_id, brane::message_type::RESPONSE);\n\t\t\t\t" \
                     "return brane::actor_engine().send(response_msg);\n\t\t\t" \
                     "}}, get_execution_context()).then([] {{\n\t\t\t\t" \
                     "return seastar::make_ready_future<brane::stop_reaction>(brane::stop_reaction::no);\n\t\t\t" \
                     "}}, get_execution_context()); \n\t\t" \
                     "}} \n".format(
                behv_name=method.spelling,
                action=apply_action,
                opt_arg=optional_args,
                result_type=result_type)

    if not actor_behvs.is_find_all():
        raise RuntimeError("Actor behaviors in actor reference can't match with implementation")

    behvs += "\t\tdefault : {\n\t\t\treturn seastar::make_ready_future<brane::stop_reaction>(brane::stop_reaction::yes); \n\t\t}\n"

    routing_logic = "\tswitch (msg->hdr.behavior_tid) {{\n{behvs}\t}}".format(behvs=behvs)
    implement_def = "seastar::future<brane::stop_reaction> {class_name}::do_work(brane::actor_message* msg) {{\n{routing_logic}\n}}\n".format(class_name=node.spelling, routing_logic=routing_logic)

    return ctor + implement_def

def verify_correctness(to_reflect):
    implement_name = ""
    num_reference = 0
    num_implement = 0
    for kind, node, _ in to_reflect:
        if kind == "reference":
            num_reference += 1
        elif kind == "implement":
            num_implement += 1
            implement_name = node.spelling

    ret = num_reference == 1 and num_implement == 1 and len(to_reflect) == 2
    return ret, implement_name

def write_defs_with_namespace(fp, def_stmts, ns_list):
    if len(ns_list) > 0:
        ns_starting = "\n".join(["namespace {ns} {{".format(ns=nsn) for nsn in ns_list])
        fp.write(ns_starting)
        fp.write("\n\n")
    fp.write(def_stmts)
    if len(ns_list) > 0:
        ns_ending = "\n".join(["}} // namespace {ns}".format(ns=nsn) for nsn in ns_list])
        fp.write(ns_ending)
        fp.write("\n\n")

class ProcessArgument:
    def __init__(self, project_dir, file_pair, includes, factory):
        self.project_dir = project_dir
        self.file_pair = file_pair
        self.includes = includes
        self.factory = factory

def process_one(arg):
    hh_filepath = os.path.join(arg.project_dir, arg.file_pair[0])
    cc_filepath = os.path.join(arg.project_dir, arg.file_pair[1])
    to_reflect = []
    ns_list = []
    index = clang.cindex.Index.create()
    unit = index.parse(hh_filepath, ['-nostdinc++', '-x', 'c++', '-std=gnu++14'] + arg.includes)
    traverse(unit.cursor, to_reflect, hh_filepath, ns_list)
    ret, actor_name = verify_correctness(to_reflect)
    if not ret:
        error_message = "Error: actor reference and implementation class must be 1-to-1 in actor header file"
        raise ValueError("Incorrect number of actor reference(implement) in {}. {}\n".format(
            hh_filepath, error_message))

    for kind, node, ns_list in to_reflect:
        print("kind: {}, node: {}, ns_list: {}".format(kind, node.spelling, ns_list))

    actor_type_id, auto_register_stmt = arg.factory.register(actor_name, to_reflect)
    print('Generating for %s ...' % arg.file_pair[0])
    if os.path.isfile(cc_filepath):
        os.chmod(cc_filepath, S_IWUSR|S_IREAD)

    with open(cc_filepath, 'w') as fp:
        fp.write('#include "%s"\n' % arg.file_pair[0])
        fp.write('#include <brane/actor/actor_factory.hh>\n')
        fp.write('#include <brane/actor/actor_client_helper.hh>\n\n')
        fp.write('#include <seastar/core/manual_clock.hh>\n\n')
        fp.write('using namespace std::chrono_literals;\n\n')
        fp.write("enum : uint16_t {{ k_actor_type_id = {aid} }};\n\n".format(aid=actor_type_id))
        
        ref_node = ""
        ref_ns_list = ""
        impl_node = ""
        impl_ns_list = ""
        for kind, node, ns_list in to_reflect:
            if kind == "reference":
                ref_node = node
                ref_ns_list = ns_list
            elif kind == "implement":
                impl_node = node
                impl_ns_list = ns_list
        actor_behvs = ActorBehaviors()
        # Reference
        ref_def = generate_reference_def(ref_node, actor_behvs)
        write_defs_with_namespace(fp, ref_def, ref_ns_list)
        # Implementation
        impl_def = generate_implementation_def(impl_node, actor_behvs)
        write_defs_with_namespace(fp, impl_def, impl_ns_list)

        fp.write(auto_register_stmt)
    os.chmod(cc_filepath, S_IREAD|S_IRGRP|S_IROTH)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Actor codegen tool.')
    parser.add_argument('--libclang-dir', action="store", dest="libclang_path",
            help="Libclang path on your system")
    parser.add_argument('--project-dir', action="store", dest="project_dir",
            help="Your CXX project root dir.")
    parser.add_argument('--actor-mod-dir', action="store", dest="actor_dir",
            help="Your actor module dir relative to project-dir.")
    parser.add_argument('--sysactor-include', action="store", dest="sysactor_include",
            help="Path of seastar-actor's include/ folder to search for headers")
    parser.add_argument('--sys-include', action="store", dest="sys_include", default="",
                        help="Path of other system folder to search for headers.")
    parser.add_argument('--user-include', action="store", dest="user_include", default="",
            help="Path of user-defined folder to search for headers.")
    args = parser.parse_args()

    libclang_exist = False
    for file in os.listdir(args.libclang_path):
        if file == "libclang.so":
            libclang_exist = True
    if not libclang_exist:
        raise RuntimeError("The libclang.so(.dylib) doesn't exist under {}".format(
            args.libclang_path))
    Config.set_library_path(args.libclang_path)

    includes = []
    includes.append('-isystem')
    includes.append(args.sysactor_include)

    if len(args.sys_include) > 0:
        for path in args.sys_include.split(';'):
            includes.append('-isystem')
            includes.append(path)
    if len(args.user_include) > 0:
        for path in args.user_include.split(';'):
            includes.append('-I%s' % path)

    autogen_dir = os.path.join(args.project_dir, args.actor_dir, "generated")
    if os.path.exists(autogen_dir):
        shutil.rmtree(autogen_dir)
    os.mkdir(autogen_dir)

    actor_hcc_pairs=[]
    actor_root_dir = os.path.join(args.project_dir, args.actor_dir)
    for a_dir, _, _ in os.walk(actor_root_dir):
        for f in os.listdir(a_dir):
            if f.endswith(".act.h"):
                actname = os.path.basename(f).split(".")[0]
                rel_act_h = os.path.relpath(os.path.join(a_dir, f), args.project_dir)
                rel_autogen_cc = os.path.join(args.actor_dir, "generated", actname+".act.autogen.cc")
                actor_hcc_pairs.append((rel_act_h, rel_autogen_cc))

    def getKey(item):
        return item[0]
    actor_hcc_pairs = sorted(actor_hcc_pairs, key=getKey)

    process_args = []
    actor_type_id = 1
    for pair in actor_hcc_pairs:
        process_args.append(ProcessArgument(args.project_dir, pair, includes, ActorRegister(actor_type_id)))
        actor_type_id += 1
    pool = Pool(min(cpu_count(), len(actor_hcc_pairs)))
    pool.map(process_one, process_args)
    pool.close()
    pool.join()
