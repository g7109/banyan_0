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
#include <berkeley/db_cxx.h>
#include <cassert>
#include <seastar/core/print.hh>

namespace storage {

class berkeleydb_helper {
    static const int  PREFIX_BIT_LEN = 3;
    static const int  PARTITION_BITS = 5;
    static const int  USERVERTEX_PADDING_BITWIDTH = 3;
    static const long PARTITION_ID_BOUND = (1L << (PARTITION_BITS));
    static const long PARTITION_OFFSET = 64L - PARTITION_BITS;
    static const long PADDING_OFFSET = 3;
    static const long PADDING_SUFFIX = 0;
    static const char BIT_MASK = 127;
    static const char STOP_MASK = -128;
    static const long COMPRESSOR_BIT_LEN = 3;
    static const int MAX_NUM_COMPRESSORS = (1 << COMPRESSOR_BIT_LEN);
    static const long COMPRESSOR_BIT_MASK = MAX_NUM_COMPRESSORS-1;

    static std::unordered_map<long, std::string> label_map;
    static std::unordered_map<std::string, long> reversed_label_map;
    static std::unordered_map<std::string, long> reversed_property_map;

    static std::unordered_map<long, std::string> init_label_map();
    static std::unordered_map<std::string, long> init_reversed_label_map();
    static std::unordered_map<std::string, long> init_reversed_property_map();

    static bool isSystemSchema(long id, long offset, long suffix);
    static int  numberOfLeadingZeros(long num);
    static void getPrefixed(int prefix, char *arr);
    static int  getDirectionID(int relationType, int direction);
    static long readUnsigned(const char *byte_ptr, int &position);
    static long readUnsignedBackward(char *in, int &position);
    static void writeUnsigned(char *out, int offset, long value, int &pos);
    static long getLong(char *array);
    static long constructId(long count, long partition, std::tuple<long, long> type);
    static char* getPrefixedString(long num, int length);
    static void getPrefixNoLabel(long vertexId, long keyId, char **byteArray_left_ptr, char **byteArray_right_ptr,
                                 int &left, int &right);
    static void getPrefixWithLabel(long keyId, const std::string &label, int direction,
                                   char **byteArray_left_ptr, char **byteArray_right_ptr, int &left, int &right);

public:
    static long toVertexId(long id);
    static long addPadding(long count, long offset, long suffix);
    static void readPositiveWithPrefix(void *in, int prefix_bit_len, int &position, long *relationType);
    static long readInlineRelationType(char *arr, int &position);
    static void convertVertexIdToPrefix(long vertexId, uint8_t *byteArray);
    static long parseVertexLabel(const Dbt &key, const Dbt &value);
    static long parseVertexID(const Dbt &key);
    static void getEdgePrefix(long vertexId, const std::string &label, bool is_out,
                              char **byteArray_left_ptr, char **byteArray_right_ptr, int &left, int &right);
    static std::string bytesToString(char *byte_ptr, int size);
    static bool compareString(char *byte_ptr, int &position, const std::string &value);
    static std::string readString(char *byte_ptr, int &position);
    static long readLong(char *byte_ptr, int &position);

    static void configure_maps();

    static long getPropertyID(const std::string &property_name) {
        auto property = reversed_property_map.find(property_name);
        assert (property != reversed_property_map.end());
        return property->second;
    }

    static long getLabelID(const std::string &label_name) {
        auto label = reversed_label_map.find(label_name);
        assert (label != reversed_label_map.end());
        return label->second;
    }

    template <typename Pred>
    static bool parseEdge(const Dbt& key, const Dbt& value, Pred &predicate, long &src, long &dst, long &label_id) {
        void *key_data = key.get_data();
        int key_size = key.get_size();

        // 1. Read RelationType & Vertex ID
        long vid = getLong((char *) key_data);
        int position = 0;
        if (!isSystemSchema(vid, 2L, 1L)) {
            std::tuple<long, long> offset_suffix = std::make_tuple(3L, 0L);
            long partition = 0;
            long count = ((ulong) vid >> 3L) & ((1L << (PARTITION_OFFSET - 3L)) - 1);
            vid = constructId(count, partition, offset_suffix);
        }
        else {
            return false;
        }

        // 2. Reading edge label and neighbour vertex id
        key_data = (unsigned char *) key_data + 8;
        long relationType[2];
        readPositiveWithPrefix(key_data, 3, position, relationType);
        int direction_id = getDirectionID((int) relationType[1] & 1, (int) (relationType[0] & 1));
        if (direction_id != 2 && direction_id != 3 ) {
            return false;
        }
        long type_id = addPadding(((unsigned long) relationType[0]) >> 1, 6L, 21L); ;

        auto found = label_map.find(type_id);
        if (found != label_map.end()) {
            long otherVertexId;
            position = key_size - 8;
            readUnsignedBackward((char *) key_data, position);
            otherVertexId = readUnsignedBackward((char *) key_data, position);

            auto edge_label = label_map.find(type_id);
            if (edge_label != label_map.end()) {
                if (predicate(value)) {
                    if (direction_id == 2) {
                        src = vid;
                        dst = otherVertexId;
                    } else {
                        src = otherVertexId;
                        dst = vid;
                    }
                    label_id = type_id;
                    return true;
                }

            }
        }
        return false;
    }
};

}