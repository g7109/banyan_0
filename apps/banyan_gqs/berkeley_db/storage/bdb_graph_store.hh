/**
 * DAIL in Alibaba Group
 * BerkeleyDB APIs
 */
#pragma once

#include <string>
#include <stdio.h>
#include <tuple>
#include <cstring>
#include <db_cxx.h>
#include <db.h>
#include <unordered_map>
#include <chrono>
#include <functional>
#include <seastar/core/print.hh>

#include "berkeley_db/storage/bdb_graph_utils.hh"
#include "berkeley_db/storage/bdb_graph_property_predicate.hh"

namespace storage {

static const int  VERTEX_KEY_LENGTH = 9;
static const int  VERTEX_KEY_STOP_CHAR = 36;
static const int  VERTEX_ID_OFFSET = 8;

struct edge_record {
    int64_t src_id;
    int64_t dst_id;
};

static bool checkEdgePropertyType(Dbt &value, int &position, long typeID) {
    auto value_data = (char*)value.get_data();
    if (typeID != berkeleydb_helper::readInlineRelationType(value_data, position)) {
        return false;
    }
    else {
        return true;
    }
}

static bool checkVertexPropertyType(Dbt &key, long typeID) {
    int position = 0;
    void *key_data = (char*) key.get_data() + 8;
    long relationType[2];
    berkeleydb_helper::readPositiveWithPrefix(key_data, 3, position, relationType);
    long type = berkeleydb_helper::addPadding(((unsigned long) relationType[0]) >> 1, 6L, 5L);
    if (type != typeID) {
        return false;
    }
    else {
        return true;
    }
}

template <typename Pred = base_predicate>
class scan_vertex_iterator {
    Dbc *cursor;
    Dbt key;
    Dbt value;
    long label_id;
    int ret;
    Pred predicate;

public:
    scan_vertex_iterator() = default;

    scan_vertex_iterator(Db &_db, const std::string &label) {
        label_id = berkeleydb_helper::getLabelID(label);
        _db.cursor(nullptr, &cursor, 0);
        ret = cursor->get(&key, &value, DB_FIRST);
    }

    ~scan_vertex_iterator() {
        cursor->close();
    }

    bool next(int64_t &vertex_id) {
        bool passed;
        while (!ret) {
            if (key.get_size() == VERTEX_KEY_LENGTH && *((char *) key.get_data() + VERTEX_KEY_LENGTH - 1) == VERTEX_KEY_STOP_CHAR) {
                auto vertex_label_id = berkeleydb_helper::parseVertexLabel(key, value);
                if (vertex_label_id == label_id) {
                    passed = predicate(cursor, key, value);
                    if (passed == false) {
                        ret = cursor->get(&key, &value, DB_NEXT);
                        continue;
                    }
                    auto id  = berkeleydb_helper::parseVertexID(key) >> VERTEX_ID_OFFSET;
                    ret = cursor->get(&key, &value, DB_NEXT);
                    vertex_id = id;
                    return true;
                }
            }
            ret = cursor->get(&key, &value, DB_NEXT);
        }
        return false;
    }

    friend class berkeleydb_store;
};

template <typename Pred = base_predicate>
class vertex_iterator {
    Dbc *cursor;
    Dbt key;
    Dbt value;
    char* right_bound;
    int32_t right_size;
    int64_t src_id;
    bool direction;
    int ret;
    Pred predicate;

public:
    vertex_iterator() = default;

    // direction: true -> out; false -> in
    vertex_iterator(Db &_db, int64_t src_id, const std::string &label, bool direction)
                    : src_id(src_id), direction(direction) {
        long vertexId = berkeleydb_helper::toVertexId(src_id);
        char *key_data_left_bound = nullptr;
        char *key_data_right_bound = nullptr;
        int left, right;
        berkeleydb_helper::getEdgePrefix(vertexId, label, direction,
                                         &key_data_left_bound, &key_data_right_bound, left, right);
        right_bound = key_data_right_bound;
        right_size = right;
        _db.cursor(nullptr, &cursor, 0);
        key = Dbt(key_data_left_bound, left);
        ret = cursor->get(&key, &value, DB_SET_RANGE);
        delete[] key_data_left_bound;
    }

    ~vertex_iterator() {
        delete[] right_bound;
        cursor->close();
    }

    bool next(int64_t &vertex_id) {
        while (!ret) {
            if (std::memcmp(key.get_data(), right_bound, right_size) >= 0) {
                break;
            }
            int64_t vertex_src_id;
            int64_t vertex_dest_id;
            long edge_label_id;
            bool result = berkeleydb_helper::parseEdge<Pred>(key, value, predicate,
                    vertex_src_id, vertex_dest_id, edge_label_id);
            if (!result) {
                ret = cursor->get(&key, &value, DB_NEXT);
                continue;
            }
            vertex_src_id >>= VERTEX_ID_OFFSET;
            vertex_dest_id >>= VERTEX_ID_OFFSET;
            if( (direction && src_id == vertex_dest_id) || (!direction && src_id == vertex_src_id)) {
                ret = cursor->get(&key, &value, DB_NEXT);
                continue;
            }
            vertex_id = src_id ==  vertex_src_id ? vertex_dest_id : vertex_src_id;

            ret = cursor->get(&key, &value, DB_NEXT);
            return true;
        }
        return false;
    }
    friend class berkeleydb_store;
};

template <typename Pred = base_predicate>
class vertex_iterator_with_property {
    Dbc *cursor;
    Dbt key;
    Dbt value;
    char* right_bound;
    int32_t right_size;
    int64_t src_id;
    bool direction;
    int position, ret;
    Pred predicate;
    bool found_vertex;

public:
    vertex_iterator_with_property() = default;

    // direction: true -> out; false -> in
    vertex_iterator_with_property(Db &_db, int64_t src_id, const std::string &label, bool direction)
            : src_id(src_id), direction(direction) {
        long vertexId = berkeleydb_helper::toVertexId(src_id);
        char *key_data_left_bound = nullptr;
        char *key_data_right_bound = nullptr;
        int left, right;
        berkeleydb_helper::getEdgePrefix(vertexId, label, direction,
                                         &key_data_left_bound, &key_data_right_bound, left, right);
        right_bound = key_data_right_bound;
        right_size = right;
        _db.cursor(nullptr, &cursor, 0);
        key = Dbt(key_data_left_bound, left);
        ret = cursor->get(&key, &value, DB_SET_RANGE);
        delete[] key_data_left_bound;
        found_vertex = false;
        position = -1;
    }

    ~vertex_iterator_with_property() {
        delete[] right_bound;
        cursor->close();
    }

    bool next(int64_t &vertex_id) {
        if (found_vertex) {
            ret = cursor->get(&key, &value, DB_NEXT);
        }
        while (!ret) {
            if (std::memcmp(key.get_data(), right_bound, right_size) >= 0) {
                break;
            }
            int64_t vertex_src_id;
            int64_t vertex_dest_id;
            long edge_label_id;
            bool result = berkeleydb_helper::parseEdge<Pred>(key, value, predicate,
                                                             vertex_src_id, vertex_dest_id, edge_label_id);
            if (!result) {
                ret = cursor->get(&key, &value, DB_NEXT);
                continue;
            }
            vertex_src_id >>= VERTEX_ID_OFFSET;
            vertex_dest_id >>= VERTEX_ID_OFFSET;
            if( (direction && src_id == vertex_dest_id) || (!direction && src_id == vertex_src_id)) {
                ret = cursor->get(&key, &value, DB_NEXT);
                continue;
            }
            vertex_id = src_id ==  vertex_src_id ? vertex_dest_id : vertex_src_id;
            found_vertex = true;
            position = 0;
            // ret = cursor->get(&key, &value, DB_NEXT);
            return true;
        }
        return false;
    }

    std::tuple<char*, int> next_property(long type_id) {
        bool found = false;
        int value_size = value.get_size();
        while (position != -1 && position < value_size) {
            if (checkEdgePropertyType(value, position, type_id)) {
                found = true;
                break;
            }
        }
        if (found) {
            return std::tuple<char*, int>((char*)value.get_data(), position);
        }
        else {
            return std::tuple<char*, int>(nullptr, 0);
        }
    }

    friend class berkeleydb_store;
};

template <typename Pred = base_predicate>
class scan_edge_iterator {
    Dbc *cursor;
    Dbt key;
    Dbt value;
    long label_id;
    int ret;
    Pred predicate;

public:
    scan_edge_iterator() = default;

    scan_edge_iterator(Db &_db, const std::string &label) {
        label_id = berkeleydb_helper::getLabelID(label);
        _db.cursor(nullptr, &cursor, 0);
        ret = cursor->get(&key, &value, DB_FIRST);
    }

    ~scan_edge_iterator() {
        cursor->close();
    }

    bool next(edge_record &ret_edge) {
        while (!ret) {
            int64_t vertex_src_id;
            int64_t vertex_dest_id;
            long edge_label_id;
            bool result = berkeleydb_helper::parseEdge<Pred>(key, value, predicate,
                    vertex_src_id, vertex_dest_id, edge_label_id);
            if (result && label_id == edge_label_id) {
                ret = cursor->get(&key, &value, DB_NEXT);
                ret_edge.src_id = vertex_src_id >> VERTEX_ID_OFFSET;
                ret_edge.dst_id = vertex_dest_id >> VERTEX_ID_OFFSET;
                return true;
            }
            ret = cursor->get(&key, &value, DB_NEXT);
        }
        return false;
    }

    friend class berkeleydb_store;
};

class vertex_property_iterator {
    Dbc *cursor;
    Dbt key;
    Dbt value;
    int ret;

public:
    vertex_property_iterator() = default;

    vertex_property_iterator(Db &_db, int64_t src_id) {
        _db.cursor(nullptr, &cursor, 0);
        uint8_t key_data[VERTEX_KEY_LENGTH];
        berkeleydb_helper::convertVertexIdToPrefix(src_id, key_data);
        key_data[VERTEX_KEY_LENGTH - 1] = (uint8_t) VERTEX_KEY_STOP_CHAR;
        Dbt temp_key(key_data, VERTEX_KEY_LENGTH);
        Dbt temp_value;
        ret = cursor->get(&temp_key, &temp_value, DB_SET_RANGE);
        ret = cursor->get(&key, &value, DB_NEXT);
    }

    ~vertex_property_iterator() {
        cursor->close();
    }

    std::tuple<char*, int> next(long type_id) {
        bool found = false;
        while (!ret) {
            if (key.get_size() != 10) {
                break;
            }
            if (checkVertexPropertyType(key, type_id)) {
                found = true;
                break;
            }
            ret = cursor->get(&key, &value, DB_NEXT);
        }
        if (found) {
            return std::tuple<char*, int>((char *)value.get_data(), 0);
        }
        else {
            return std::tuple<char*, int>(nullptr, 0);
        }
    }

    friend class berkeleydb_store;
};

class edge_property_iterator {
    Dbc *cursor;
    Dbt key;
    Dbt value;
    int position;
    base_predicate predicate;

public:
    edge_property_iterator() = default;

    edge_property_iterator(Db &_db, int64_t local_vid, int64_t other_vid, const std::string &label, bool direction) {
        position = -1;

        long vertexId = berkeleydb_helper::toVertexId(local_vid);
        char *key_data_left_bound = nullptr;
        char *key_data_right_bound = nullptr;
        int left, right;
        berkeleydb_helper::getEdgePrefix(vertexId, label, direction,
                &key_data_left_bound, &key_data_right_bound, left, right);

        _db.cursor(nullptr, &cursor, 0);
        key = Dbt(key_data_left_bound, left);
        int ret = cursor->get(&key, &value, DB_SET_RANGE);

        while (!ret) {
            if (std::memcmp(key.get_data(), key_data_right_bound, right) >= 0) {
                break;
            }
            int64_t vertex_src_id;
            int64_t vertex_dest_id;
            long edge_label_id;
            bool result = berkeleydb_helper::parseEdge<base_predicate>(key, value, predicate,
                    vertex_src_id, vertex_dest_id, edge_label_id);
            vertex_src_id >>= VERTEX_ID_OFFSET;
            vertex_dest_id >>= VERTEX_ID_OFFSET;
            if(result && ((direction && other_vid == vertex_dest_id) || (!direction && other_vid == vertex_src_id))) {
                position = 0;
                break;
            }
            ret = cursor->get(&key, &value, DB_NEXT);
        }
        delete[] key_data_left_bound;
        delete[] key_data_right_bound;
    }

    ~edge_property_iterator() {
        cursor->close();
    }

    std::tuple<char*, int> next(long type_id) {
        bool found = false;
        int value_size = value.get_size();

        while (position != -1 && position < value_size) {
            if (checkEdgePropertyType(value, position, type_id)) {
                found = true;
                break;
            }
        }
        if (found) {
            return std::tuple<char*, int>((char*)value.get_data(), position);
        }
        else {
            return std::tuple<char*, int>(nullptr, 0);
        }
    }

    friend class berkeleydb_store;
};

class berkeleydb_store {
private:
    Db _db;

    explicit berkeleydb_store(const std::string& path) : _db(nullptr, 0) {
        _db.set_cachesize(16, 0, 0);
        _db.set_alloc(malloc, realloc, free);
        _db.open(nullptr, (path).c_str(), nullptr, DB_BTREE, DB_CREATE, 0);
    }

public:
    static berkeleydb_store* open(const std::string& path) {
        return new berkeleydb_store{path};
    }

    ~berkeleydb_store() {
        try {
            _db.close(0);
        }
        catch (std::bad_cast &e) {
            std::cout << "Berkeley DB close() failed: " << e.what() << std::endl;
        }
    }

    void warmup_cache() {
        Dbc *cursor;
        _db.cursor(nullptr, &cursor, 0);
        Dbt key;
        Dbt value;
        int ret = cursor->get(&key, &value, DB_FIRST);
        while(!ret) {
            ret = cursor->get(&key, &value, DB_NEXT);
        }
        cursor->close();
    }

    template <typename Pred = base_predicate>
    bool get_vertex(int64_t id, Pred &predicate, std::string &vertex_label) {
        Dbc *cursor;
        _db.cursor(nullptr, &cursor, 0);
        uint8_t key_data[VERTEX_KEY_LENGTH];
        berkeleydb_helper::convertVertexIdToPrefix(id, key_data);
        key_data[VERTEX_KEY_LENGTH - 1] = (uint8_t) VERTEX_KEY_STOP_CHAR;
        Dbt key(key_data, VERTEX_KEY_LENGTH);
        Dbt value;
        cursor->get(&key, &value, DB_SET_RANGE);

        if(vertex_label != "") {
            long vertex_label_id = berkeleydb_helper::parseVertexLabel(key, value);
            if(berkeleydb_helper::getLabelID(vertex_label) != vertex_label_id) {
                cursor->close();
                return false;
            }
        }
        bool ret = predicate(cursor, key, value);
        cursor->close();
        return ret;
    }

    long get_vertex_label_id(int64_t id) {
        Dbc *cursor;
        _db.cursor(nullptr, &cursor, 0);
        uint8_t key_data[VERTEX_KEY_LENGTH];
        berkeleydb_helper::convertVertexIdToPrefix(id, key_data);
        key_data[VERTEX_KEY_LENGTH - 1] = (uint8_t) VERTEX_KEY_STOP_CHAR;
        Dbt key(key_data, VERTEX_KEY_LENGTH);
        Dbt value;
        cursor->get(&key, &value, DB_SET_RANGE);
        auto ret = berkeleydb_helper::parseVertexLabel(key, value);
        cursor->close();
        return ret;
    }

    template <typename Pred = base_predicate>
    vertex_iterator<Pred> get_out_v(int64_t src_id, const std::string &label) {
         return vertex_iterator<Pred>(_db, src_id, label, true);
    }

    template <typename Pred = base_predicate>
    vertex_iterator<Pred> get_in_v(int64_t src_id, const std::string &label) {
        return vertex_iterator<Pred>(_db, src_id, label, false);
    }

    template <typename Pred = base_predicate>
    vertex_iterator_with_property<Pred> get_out_v_with_property(int64_t src_id, const std::string &label) {
        return vertex_iterator_with_property<Pred>(_db, src_id, label, true);
    }

    template <typename Pred = base_predicate>
    vertex_iterator_with_property<Pred> get_in_v_with_property(int64_t src_id, const std::string &label) {
        return vertex_iterator_with_property<Pred>(_db, src_id, label, false);
    }

    template <typename Pred = base_predicate>
    scan_vertex_iterator<Pred> scan_vertex(const std::string &label) {
        return scan_vertex_iterator<Pred>(_db, label);
    }

    template <typename Pred = base_predicate>
    scan_edge_iterator<Pred> scan_edge(const std::string &label) {
        return scan_edge_iterator<Pred>(_db, label);
    }

    vertex_property_iterator get_vertex_properties(int64_t src_id) {
        return vertex_property_iterator(_db, src_id);
    }

    edge_property_iterator get_in_edge_properties(int64_t other_vid, int64_t local_vid, const std::string &label) {
        return edge_property_iterator(_db, local_vid, other_vid, label, false);
    }

    edge_property_iterator get_out_edge_properties(int64_t local_vid, int64_t other_vid, const std::string &label) {
        return edge_property_iterator(_db, local_vid, other_vid, label, true);
    }
};

} //namespace storage
