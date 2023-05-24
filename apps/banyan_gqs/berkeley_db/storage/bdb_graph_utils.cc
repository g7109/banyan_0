/**
 * DAIL in Alibaba Group
 * BerkeleyDB APIs
 */
#include <string>
#include <iostream>
#include <fstream>
#include <tuple>
#include <cstring>
#include <unordered_map>
#include <berkeley/db_cxx.h>
#include <cassert>
#include "berkeley_db/storage/bdb_graph_utils.hh"
#include "common/configs.hh"
#include <seastar/core/byteorder.hh>

namespace storage {

std::unordered_map<long, std::string> berkeleydb_helper::init_label_map() {
    std::unordered_map<long, std::string> i_label_map{};
    return i_label_map;
}

std::unordered_map<std::string, long> berkeleydb_helper::init_reversed_label_map() {
    std::unordered_map<std::string, long> i_reversed_label_map{};
    return i_reversed_label_map;
}

std::unordered_map<std::string, long> berkeleydb_helper::init_reversed_property_map() {
    std::unordered_map<std::string, long> i_reversed_property_map{};
    return i_reversed_property_map;
}

std::vector<std::string> split(const std::string& s, const char* delim = " ") {
    std::vector<std::string> sv;
    char* buffer = new char[s.size() + 1];
    buffer[s.size()] = '\0';
    std::copy(s.begin(), s.end(), buffer);
    char* p = std::strtok(buffer, delim);
    do {
        sv.emplace_back(p);
    } while ((p = std::strtok(nullptr, delim)));
    delete[] buffer;
    return sv;
}

void berkeleydb_helper::configure_maps() {
    std::string schema_folder = std::string(banyan_gqs_dir) + "/berkeley_db/db_schema/";
    std::string path_vertex_label_type = schema_folder + "typeIDToVertexLabel.csv";
    std::string path_edge_label_type = schema_folder + "typeIDToEdgeLabel.csv";
    std::string path_prop_name_type = schema_folder + "typeIDToPropertyName.csv";
    std::string path_remaining_prop_name_type = schema_folder + "typeIDToPropertyNameRemaining.csv";

    std::string line;

    std::ifstream f_vertex_label_type(path_vertex_label_type);
    while ( getline(f_vertex_label_type, line) )
    {
        auto values = split(line, ",");
        auto label_type_id = std::stol(values[0]);
        auto label_name = values[1];
        label_map[label_type_id] = label_name;
        reversed_label_map[label_name] = label_type_id;
    }
    f_vertex_label_type.close();

    std::ifstream f_edge_label_type(path_edge_label_type);
    while ( getline(f_edge_label_type, line) )
    {
        auto values = split(line, ",");
        auto label_type_id = std::stol(values[0]);
        auto label_name = values[1];
        label_map[label_type_id] = label_name;
        reversed_label_map[label_name] = label_type_id;
    }
    f_edge_label_type.close();

    std::ifstream f_prop_name_type(path_prop_name_type);
    while( getline(f_prop_name_type, line) ) {
        auto values = split(line, ",");
        auto prop_name_type_id = std::stol(values[0]);
        auto prop_name = values[1];
        reversed_property_map[prop_name] = prop_name_type_id;
    }
    f_prop_name_type.close();

    std::ifstream f_remaining_prop_name_type(path_remaining_prop_name_type);
    while( getline(f_remaining_prop_name_type, line) ) {
        auto values = split(line, ",");
        auto prop_name_type_id = std::stol(values[0]);
        auto prop_name = values[1];
        reversed_property_map[prop_name] = prop_name_type_id;
    }
    f_remaining_prop_name_type.close();
}

std::unordered_map<long, std::string> berkeleydb_helper::label_map = berkeleydb_helper::init_label_map();
std::unordered_map<std::string, long> berkeleydb_helper::reversed_label_map = berkeleydb_helper::init_reversed_label_map();
std::unordered_map<std::string, long> berkeleydb_helper::reversed_property_map = berkeleydb_helper::init_reversed_property_map();

std::string berkeleydb_helper::bytesToString(char* byte_ptr, int size){
    if(byte_ptr == nullptr) return "";
    std::string ret = "[";
    for (int i = 0; i< size; i++){
        ret += std::to_string((int)byte_ptr[i]) + ", ";
    }
    ret += "]";
    return ret;
}

long berkeleydb_helper::addPadding(long count, long offset, long suffix) {
    return (count << offset) | suffix;
}

bool berkeleydb_helper::isSystemSchema(long id, long offset, long suffix) {
    return (id & ((1L << offset) - 1)) == suffix;
}

int berkeleydb_helper::numberOfLeadingZeros(long num) {
    if (num == 0)
        return 64;
    int n = 1;
    int x = (int) ((ulong) num >> 32);
    if (x == 0) {
        n += 32;
        x = (int) num;
    }
    if ((uint) x >> 16 == 0) {
        n += 16;
        x <<= 16;
    }
    if ((uint) x >> 24 == 0) {
        n += 8;
        x <<= 8;
    }
    if ((uint) x >> 28 == 0) {
        n += 4;
        x <<= 4;
    }
    if ((uint) x >> 30 == 0) {
        n += 2;
        x <<= 2;
    }
    n -= (uint) x >> 31;
    return n;
}

void berkeleydb_helper::getPrefixed(int prefix, char *arr) {
    arr[0] = (char) (prefix << (sizeof(char) * 8 - PREFIX_BIT_LEN));
}

int berkeleydb_helper::getDirectionID(int relationType, int direction) {
    return (relationType << 1) + direction;
}

long berkeleydb_helper::toVertexId(long id) {
    return id << (PARTITION_BITS + USERVERTEX_PADDING_BITWIDTH);
}

long berkeleydb_helper::readUnsigned(const char *byte_ptr, int &position) {
    long value = 0;
    char b;
    do {
        b = byte_ptr[position];
        position += sizeof(char);
        value = value << 7 | (b & BIT_MASK);
    } while (b >= 0);
    return value;
}

long berkeleydb_helper::readUnsignedBackward(char* in, int& position) {
    int numBytes = 0;
    long value = 0;
    long b;
    while (true) {
        position--;
        b = in[position];
        if (b < 0) { //First byte
            value = value | ((b & 0x0F) << (7 * numBytes));
            break;
        }
        value = value | (b << (7 * numBytes));
        numBytes++;
    }
    return value;
}

void berkeleydb_helper::writeUnsigned(char *out, int offset, long value, int &pos) {
    while (offset > 0) {
        offset -= 7;
        char b = (char) (((ulong) value >> offset) & BIT_MASK);
        if (offset == 0) {
            b = b | STOP_MASK;
        }
        out[pos++] = b;
    }
}

void berkeleydb_helper::readPositiveWithPrefix(void *in, int prefix_bit_len, int &position, long* relationType) {
    int first = *(unsigned char *) in;
    position += sizeof(unsigned char);
    int delta_len = 8 - prefix_bit_len;
    long prefix = first >> delta_len;
    long value = first & ((1 << (delta_len - 1)) - 1);
    if ((((unsigned int) first >> (delta_len - 1)) & 1) == 1) {
        int deltaPos = position;
        long remainder = readUnsigned((char *) in, position);
        deltaPos = position - deltaPos;
        value = (value << (deltaPos * 7)) + remainder;
    }
    relationType[0] = value;
    relationType[1] = prefix;
}

void berkeleydb_helper::convertVertexIdToPrefix(long vertexId, uint8_t *byteArray) {
    vertexId = vertexId << 8;
    long partition = (vertexId >> USERVERTEX_PADDING_BITWIDTH) & (PARTITION_ID_BOUND - 1);
    long count = vertexId >> (PARTITION_BITS + USERVERTEX_PADDING_BITWIDTH);
    long keyId = (partition << PARTITION_OFFSET) | (count << PADDING_OFFSET) | PADDING_SUFFIX;
    // convert from long  to char array
    seastar::write_be<long>((char*)byteArray, keyId);
}

long berkeleydb_helper::getLong(char *array) {
    long ret = seastar::read_be<long>(array);
    return ret;
}

long berkeleydb_helper::constructId(long count, long partition, std::tuple<long, long> type) {
    long id = (count << PARTITION_BITS) + partition;
    return id << std::get<0>(type) | std::get<1>(type);
}

char* berkeleydb_helper::getPrefixedString(long keyId, int length) {
    char *byteArray_left = new char[length];
    seastar::write_be<long>(byteArray_left, keyId);
    return byteArray_left;
}

void berkeleydb_helper::getPrefixNoLabel(long vertexId, long keyId, char **byteArray_left_ptr,
        char **byteArray_right_ptr, int &left, int &right) {
    char *byteArray_left = getPrefixedString(keyId, 9);
    getPrefixed(3, &byteArray_left[8]);
    *byteArray_left_ptr = byteArray_left;

    long nextVertexId = vertexId + 256L;
    long partition = ((ulong) nextVertexId >> USERVERTEX_PADDING_BITWIDTH) & (PARTITION_ID_BOUND - 1);
    long count = ((ulong) nextVertexId) >> (PARTITION_BITS + USERVERTEX_PADDING_BITWIDTH);
    keyId = (partition << PARTITION_OFFSET) | (count << PADDING_OFFSET) | PADDING_SUFFIX;

    char *byteArray_right = getPrefixedString(keyId, 8);
    *byteArray_right_ptr = byteArray_right;
    left = 9;
    right = 8;
}

void berkeleydb_helper::getPrefixWithLabel(long keyId, const std::string &label, int direction,
                                           char **byteArray_left_ptr, char **byteArray_right_ptr, int &left, int &right) {
    int DEFAULT_COLUMN_CAPACITY = 60;
    int position = 0;
    char *byteArray_left = getPrefixedString(keyId, 8 + DEFAULT_COLUMN_CAPACITY);
    position += 8;
    auto pair = reversed_label_map.find(label);
    assert (pair != reversed_label_map.end());
    long type_id = pair->second;
    long strippedId = (((ulong) type_id >> 6) << 1) + (direction & 1);
    bool isSystemRelationType = isSystemSchema(type_id, 6, 53) || isSystemSchema(type_id, 6, 37);
    int dir_id_prefix = ((isSystemRelationType ? 0 : 1) << 1) + ((uint) direction >> 1);
    int deltaLen = 8 - PREFIX_BIT_LEN;
    char first = (char) (dir_id_prefix << deltaLen);
    int valueLen = (strippedId == 0) ? 1 : 64 - numberOfLeadingZeros(strippedId);
    int mod = valueLen % 7;

    if (mod <= (deltaLen - 1)) {
        int offset = (valueLen - mod);
        first = (char) (first | ((ulong) strippedId >> offset));
        strippedId = strippedId & ((1L << offset) - 1);
        valueLen -= mod;
    } else {
        valueLen += (7 - mod);
    }

    if (valueLen > 0) {
        first = (char) (first | (1 << (deltaLen - 1)));
    }
    byteArray_left[position++] = first;
    if (valueLen > 0) {
        writeUnsigned(byteArray_left, valueLen, strippedId, position);
    }

    char *byteArray_right = getPrefixedString(keyId, position);
    int pos_right = 8;
    bool carry = true;
    for (int i = position - 1; i >= 8; i--) {
        char b = byteArray_left[i];
        if (carry) {
            b++;
            if (b != 0) carry = false;
        }
        byteArray_right[i] = b;
        pos_right++;
    }
    *byteArray_left_ptr = byteArray_left;
    *byteArray_right_ptr = byteArray_right;
    left = position;
    right = pos_right;
}

void berkeleydb_helper::getEdgePrefix(long vertexId, const std::string &label, bool is_out, char **byteArray_left_ptr,
                                      char **byteArray_right_ptr, int &left, int &right) {

    int direction = is_out ? 2 : 3;
    long partition = ((ulong) vertexId >> USERVERTEX_PADDING_BITWIDTH) & (PARTITION_ID_BOUND - 1);
    long count = ((ulong) vertexId) >> (PARTITION_BITS + USERVERTEX_PADDING_BITWIDTH);
    long keyId = (partition << PARTITION_OFFSET) | (count << PADDING_OFFSET) | PADDING_SUFFIX;

    if (label == "") {
        getPrefixNoLabel(vertexId, keyId, byteArray_left_ptr, byteArray_right_ptr, left, right);
    } else {
        getPrefixWithLabel(keyId, label, direction, byteArray_left_ptr, byteArray_right_ptr, left, right);
    }
}

long berkeleydb_helper::parseVertexLabel(const Dbt& key, const Dbt& value) {
    void* value_data = value.get_data();
    int key_size = key.get_size();
    int position = 1;
    position -= (key_size - 8);
    long otherVertexId = readUnsigned((char *) value_data, position);
    assert (label_map.find(otherVertexId) != label_map.end());
    return otherVertexId;
}

long berkeleydb_helper::parseVertexID(const Dbt& key) {
    long vid = getLong((char *) key.get_data());
    std::tuple<long, long> offset_suffix = std::make_tuple(3L, 0L);
    long partition = 0;
    long count = ((ulong) vid >> 3L) & ((1L << (PARTITION_OFFSET - 3L)) - 1);
    return constructId(count, partition, offset_suffix);
}

long berkeleydb_helper::readInlineRelationType(char* arr, int& position) {
    return (readUnsigned(arr, position)<< 4L) | 5L;
}

bool berkeleydb_helper::compareString(char *byte_ptr, int &position, const std::string &value) {
    long length = readUnsigned(byte_ptr, position);
    if(length == 0) return false;
    long compressionId = length & COMPRESSOR_BIT_MASK;
    length = (ulong)length >> COMPRESSOR_BIT_LEN;
    assert (compressionId == 0);
    length = (ulong)length >> 1;
    int char_length = (int)value.length();
    int index = 0;
    // ASCII encoding
    if ((length & 1) == 0) {
        if (length == 1) {
            return char_length == 0;
        }
        bool end;
        while(true){
            int c = 0xFF & byte_ptr[position++];
            if(value.at(index++) != (char)(c & 0x7F)) {
                return false;
            }
            end = (c & 0x80) > 0;
            if(end || index == char_length) break;
        }
        return end && index == char_length;
    }
        // UTF encoding
    else {
        if (length != char_length) {
            return false;
        }
        char c;
        for (int i = 0; i < length; i++) {
            int b = byte_ptr[position ++] & 0xFF;
            switch (b >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    c = (char)b;
                    break;
                case 12:
                case 13:
                    c = (char)((b & 0x1F) << 6 | ((byte_ptr[position++]) & 0x3F));
                    break;
                case 14:
                    c = (b & 0x0F) << 12;
                    c |= (byte_ptr[position++] & 0x3F) << 6;
                    c |= byte_ptr[position++] & 0x3F;
                    break;
            }
            if (value.at(index++) != c) {
                return false;
            }
        }
        return true;
    }
}

std::string berkeleydb_helper::readString(char *byte_ptr, int &position) {
    long length = readUnsigned(byte_ptr, position);
    if (length == 0) { return ""; }
    long compressionId = length & COMPRESSOR_BIT_MASK;
    length = (ulong)length >> COMPRESSOR_BIT_LEN;
    assert (compressionId == 0);
    length = (ulong)length >> 1;
    // ASCII encoding
    std::string str;
    if ((length & 1) == 0) {
        if (length == 1) {
            return "";
        }
        str.reserve(64);
        while(true){
            int c = 0xFF & byte_ptr[position++];
            str.push_back((char)(c & 0x7F));
            if((c & 0x80) > 0) break;
        }
    }
    // UTF encoding
    else {
        str.reserve(length);
        for (int i = 0; i < length; i++) {
            int b = byte_ptr[position ++] & 0xFF;
            switch (b >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    str.push_back(b);
                    break;
                case 12:
                case 13:
                    str.push_back((char)((b & 0x1F) << 6 | ((byte_ptr[position++]) & 0x3F)));
                    break;
                case 14:
                    str.push_back((char)((b & 0x0F) << 12) |
                                    (char)((byte_ptr[position] & 0x3F) << 6) |
                                    (char)(byte_ptr[position + 1] & 0x3F));
                    position += 2;
                    break;
            }
        }
    }
    return str;
}

long berkeleydb_helper::readLong(char* byte_ptr, int &position) {
    long value = getLong(&byte_ptr[++position]) + LONG_MIN;
    position += 8;
    return value;
}

}