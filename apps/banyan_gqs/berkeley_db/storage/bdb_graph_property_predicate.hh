/**
 * DAIL in Alibaba Group
 * BerkeleyDB APIs
 */
#pragma once

#include <string>
#include <iostream>
#include <tuple>
#include <cstring>
#include <unordered_map>
#include <db_cxx.h>
#include <cassert>
#include <seastar/core/print.hh>

#include "berkeley_db/storage/bdb_graph_utils.hh"

namespace storage {

class base_predicate {
public:
    virtual ~base_predicate() = default;

    virtual bool operator () (const Dbt &value) {
       return true;
    }

    virtual bool operator () (Dbc *cursor, Dbt &key, Dbt &value) {
        return true;
    }

    template <typename PropertyCheck>
    static bool checkSingleVertexProperty(Dbt &key, Dbt &value, long typeID, PropertyCheck &check) {
        int position = 0;
        void *key_data = (char*) key.get_data() + 8;
        long relationType[2];
        berkeleydb_helper::readPositiveWithPrefix(key_data, 3, position, relationType);
        long type = berkeleydb_helper::addPadding(((unsigned long) relationType[0]) >> 1, 6L, 5L);
        if (type != typeID) {
            return false;
        }
        position = 0;
        return check((char *)value.get_data(), position);
    }

    template <typename... PropertyChecks>
    bool checkVertexPredicate(Dbc *cursor, Dbt &key, Dbt &value, long *typeIDs, PropertyChecks &... checks) {
        int parsedNum = 0;
        int index = 0;
        int ret = cursor->get(&key, &value, DB_NEXT);
        const std::size_t size = sizeof...(PropertyChecks);

        auto func = [&cursor, &ret, &key, &value, typeIDs, &parsedNum, &index](auto &check) {
            assert (index < size);
            long type = *(typeIDs + index++);
            while(!ret) {
                if (key.get_size() != 10) {
                    break;
                }
                if (checkSingleVertexProperty(key, value, type, check)) {
                    parsedNum++;
                    break;
                }
                ret = cursor->get(&key, &value, DB_NEXT);
            }
        };
        std::initializer_list<int>{(func(checks), 0) ...};
        return parsedNum == size;
    }

    template <typename PropertyCheck>
    static bool checkSingleEdgeProperty(char *value_data, int &position, long typeID, PropertyCheck &check) {
        if (typeID != berkeleydb_helper::readInlineRelationType(value_data, position)) {
            return false;
        }
        return check(value_data, position);
    }

    template <typename... PropertyChecks>
    bool checkEdgePredicate(const Dbt &value, const long *typeIDs, PropertyChecks &... checks) {
        int index = 0;
        int position = 0;
        int passedNum = 0;
        int value_size = value.get_size();
        char* value_data = (char*)value.get_data();
        const std::size_t size = sizeof...(PropertyChecks);

        auto func = [value_data, value_size, typeIDs, &index, &position, &passedNum](auto &check) {
            assert (index < size);
            long type = *(typeIDs + index++);
            while (position < value_size) {
                if (checkSingleEdgeProperty(value_data, position, type, check)) {
                    passedNum++;
                    break;
                }
            }
        };
        std::initializer_list<int>{(func(checks), 0) ...};
        return passedNum == size;
    }
};

}// namespace storage
