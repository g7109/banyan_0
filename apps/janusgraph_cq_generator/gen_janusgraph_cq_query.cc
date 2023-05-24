//
// Created by everlighting on 2021/4/21.
//

#include <string>
#include <fstream>
#include "seastar/core/print.hh"

const std::string in_folder = "apps/banyan_gqs/cq_interactive_params/ldbc1/";
const std::string out_folder = "janus_cq_queries/";
const unsigned param_num = 10;

const std::string cq[7] ={
        ""
        ,
        "g.V({}).repeat(__.out('knows')).times(5).dedup().next(100)"
        ,
        "g.V({}).sideEffect(out('workAt').store('companies')).barrier()."
        "repeat(__.out('knows')).times(5)."
        "emit(__.out('workAt').where(within('companies')).count().is(gt(0)))."
        "dedup().next(20)"
        ,
        "g.V({}).out('knows').union(identity(), out('knows')).dedup()."
        "where(__.in('hasCreator').out('hasTag').dedup().out('hasType').values('name').filter{{it.get().contains('Country')}})."
        "order().by().next(10)"
        ,
        "g.V({}).sideEffect(out('workAt').store('companies')).barrier().out('knows')."
        "where(__.repeat(__.out('knows')).times(4)."
        "emit(__.out('workAt').where(within('companies')).count().is(gt(0)))."
        "dedup().count().is(gt(1)))."
        "next(10)"
        ,
        "g.V({}).sideEffect(out('workAt').store('companies')).barrier()."
        "repeat(__.out('knows')).times(5)."
        "emit(__.out('workAt').where(within('companies')).count().is(gt(0))).dedup()."
        "where(__.in('hasCreator').out('hasTag').dedup().out('hasType').values('name').filter{{it.get().contains('Country')}})."
        "next(5)"
        ,
        "g.V({}).repeat(__.out('knows')."
        "where(__.in('hasCreator').out('hasTag').dedup().out('hasType').values('name')."
        "filter{{it.get().contains('Country')}}))."
        "times(5).dedup().next(10)"
};

std::string format_one(unsigned cq_id, int64_t start_v) {
    return seastar::format(cq[cq_id].c_str(), start_v * 256);
}

int main() {
    std::ifstream input;
    std::ofstream output;
    std::string line;
    int64_t start_person;
    for (unsigned i = 1; i <= 6; i++) {
        input.open(in_folder + "cq_" + std::to_string(i) + "_param.txt");
        output.open(out_folder + "cq_" + std::to_string(i) + ".groovy");

        output << "graph = JanusGraphFactory.open('conf/bdb-ldbc1.properties')\n";
        output << "g = graph.traversal()\n";

        getline(input, line);
        for (unsigned j = 0; j < param_num; j++) {
            input >> start_person;
            output << "clock(10) { " + format_one(i, start_person) + " }\n";
        }

        input.close();
        output.close();
    }
}