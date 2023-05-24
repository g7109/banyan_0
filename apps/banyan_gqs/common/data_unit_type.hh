//
// Created by everlighting on 2020/8/4.
//

#pragma once

#include <brane/util/common-utils.hh>
#include <brane/query/column_batch.hh>
#include "common/configs.hh"
#include "common/path_eos.hh"

// For basic data types
template <typename T>
struct TrivialStruct {
    T val;
    explicit TrivialStruct(T value) : val(value) {}

    TrivialStruct(TrivialStruct&& other) noexcept
        : val(std::move(other.val)) {}

    inline void dump_to(brane::serializable_unit &su) {
        su.num_used = 1;
        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(T));
        memcpy(su.bufs[0].get_write(), &val, sizeof(T));
    }

    inline static TrivialStruct<T> load_from(brane::serializable_unit&& su) {
        auto new_value = *reinterpret_cast<const T*>(su.bufs[0].get());
        return TrivialStruct<T>{new_value};
    }
};

using Eos = TrivialStruct<bool>;
using StartVertex = TrivialStruct<int64_t>;

using VertexBatch = brane::cb::column_batch<int64_t>;
using LabelBatch = brane::cb::column_batch<brane::cb::string>;
using StringBatch = brane::cb::column_batch<brane::cb::string>;
using PathBatch = brane::cb::column_batch<brane::cb::path>;
using PathLenBatch = brane::cb::column_batch<uint8_t>;
using TagNodeBatch = brane::cb::column_batch<int64_t>;
using IntPropBatch = brane::cb::column_batch<int64_t>;
using uIntBatch = brane::cb::column_batch<unsigned>;

struct vertex_pathLen {
    int64_t v_id;
    uint8_t path_len;

    vertex_pathLen(int64_t v_id, uint8_t path_len)
        : v_id(v_id), path_len(path_len) {}
    ~vertex_pathLen() = default;
};
using vertex_pathLen_Batch = brane::cb::column_batch<vertex_pathLen>;

struct vertex_intProp {
    int64_t v_id;
    int64_t prop;

    vertex_intProp(int64_t v, int64_t p) : v_id(v), prop(p) {};
    ~vertex_intProp() = default;
};
using vertex_intProp_Batch = brane::cb::column_batch<vertex_intProp>;

struct q3_result_node {
    int64_t v_id;
    unsigned x_count;
    unsigned y_count;

    q3_result_node(int64_t vertex_id, unsigned x_count, unsigned y_count)
        : v_id(vertex_id), x_count(x_count), y_count(y_count) {}
    q3_result_node(const q3_result_node&) = default;
    q3_result_node(q3_result_node&& n) noexcept
        : v_id(n.v_id), x_count(n.x_count), y_count(n.y_count) {}

    q3_result_node& operator=(const q3_result_node& n) = delete;
    q3_result_node& operator=(q3_result_node&& n) noexcept {
        if(this != &n) {
            v_id = n.v_id;
            x_count = n.x_count;
            y_count = n.y_count;
        }
        return *this;
    }
};
using q3_result_node_Batch = brane::cb::column_batch<q3_result_node>;

struct vertex_pathLen_StrProp_Batch {
    VertexBatch v_ids;
    PathLenBatch path_lens;
    StringBatch props;

    explicit vertex_pathLen_StrProp_Batch(const uint32_t capacity = batch_size)
        : v_ids(capacity), path_lens(capacity), props(capacity, capacity * 32) {}
    vertex_pathLen_StrProp_Batch(VertexBatch&& vertex_batch, PathLenBatch&& path_len_batch, StringBatch&& string_batch)
        : v_ids(std::move(vertex_batch)), path_lens(std::move(path_len_batch)), props(std::move(string_batch)) {}
    vertex_pathLen_StrProp_Batch(const vertex_pathLen_StrProp_Batch&) = delete;
    vertex_pathLen_StrProp_Batch(vertex_pathLen_StrProp_Batch&& other) noexcept
        : v_ids(std::move(other.v_ids)), path_lens(std::move(other.path_lens)), props(std::move(other.props)) {}
    ~vertex_pathLen_StrProp_Batch() = default;

    inline unsigned size() const { return v_ids.size(); }
    inline unsigned capacity() const { return v_ids.capacity(); }

    inline void emplace_back(int64_t v_id, uint8_t path_len, std::string& prop_str) {
        v_ids.push_back(v_id);
        path_lens.push_back(path_len);
        props.push_back(prop_str);
    }

    inline void reset(const uint32_t capacity = batch_size) {
        v_ids.reset(capacity);
        path_lens.reset(capacity);
        props.reset(capacity, capacity * 32);
    }

    inline void dump_to(brane::serializable_unit &su) {
        su.num_used = 4;
        v_ids.dump_to(su.bufs[0]);
        path_lens.dump_to(su.bufs[1]);
        props.dump_to(su.bufs[2], su.bufs[3]);
    }

    static inline vertex_pathLen_StrProp_Batch load_from(brane::serializable_unit &&su) {
        return vertex_pathLen_StrProp_Batch{
            VertexBatch{std::move(su.bufs[0])},
            PathLenBatch{std::move(su.bufs[1])},
            StringBatch{std::move(su.bufs[2]), std::move(su.bufs[3])}};
    }
};

struct vertex_friendId {
    int64_t v_id;
    int64_t friend_id;

    vertex_friendId(int64_t v_id, int64_t friend_id)
        : v_id(v_id), friend_id(friend_id) {}
    ~vertex_friendId() = default;
};
using vertex_friendId_Batch = brane::cb::column_batch<vertex_friendId>;

struct vertex_tagId {
    int64_t v_id;
    int64_t tag_id;

    vertex_tagId(int64_t v_id, int64_t tag_id) : v_id(v_id), tag_id(tag_id) {}
    ~vertex_tagId() = default;
};
using vertex_tagId_Batch = brane::cb::column_batch<vertex_tagId>;

struct vertex_forumId {
    int64_t v_id;
    int64_t forum_id;

    vertex_forumId(int64_t v_id, int64_t forum_id) : v_id(v_id), forum_id(forum_id) {}
    ~vertex_forumId() = default;
};
using vertex_forumId_Batch = brane::cb::column_batch<vertex_forumId>;

struct vertex_string_Batch {
    VertexBatch v_ids;
    StringBatch strs;

    explicit vertex_string_Batch(const uint32_t capacity = batch_size)
        : v_ids(capacity), strs(capacity, capacity * 32) {}
    vertex_string_Batch(VertexBatch&& vertex_batch, StringBatch&& string_batch)
        : v_ids(std::move(vertex_batch)), strs(std::move(string_batch)) {}
    vertex_string_Batch(const vertex_string_Batch&) = delete;
    vertex_string_Batch(vertex_string_Batch&& other) noexcept
        : v_ids(std::move(other.v_ids)), strs(std::move(other.strs)) {}
    ~vertex_string_Batch() = default;

    inline unsigned size() const { return v_ids.size(); }
    inline unsigned capacity() const { return v_ids.capacity(); }

    inline void emplace_back(int64_t v_id, std::string& prop_str) {
        v_ids.push_back(v_id);
        strs.push_back(prop_str);
    }

    inline void reset(const uint32_t capacity = batch_size) {
        v_ids.reset(capacity);
        strs.reset(capacity, capacity * 32);
    }

    inline void dump_to(brane::serializable_unit &su) {
        su.num_used = 3;
        v_ids.dump_to(su.bufs[0]);
        strs.dump_to(su.bufs[1], su.bufs[2]);
    }

    static inline vertex_string_Batch load_from(brane::serializable_unit &&su) {
        return vertex_string_Batch{
            VertexBatch{std::move(su.bufs[0])},
            StringBatch{std::move(su.bufs[1]), std::move(su.bufs[2])}};
    }
};

struct vertex_IntProp_StrProp_Batch {
    VertexBatch v_ids;
    IntPropBatch int_props;
    StringBatch str_props;

    explicit vertex_IntProp_StrProp_Batch(const uint32_t capacity = batch_size)
            : v_ids(capacity), int_props(capacity), str_props(capacity, capacity * 64) {}
    vertex_IntProp_StrProp_Batch(VertexBatch&& vertex_batch, IntPropBatch&& int_batch, StringBatch&& string_batch)
            : v_ids(std::move(vertex_batch)), int_props(std::move(int_batch)), str_props(std::move(string_batch)) {}
    vertex_IntProp_StrProp_Batch(const vertex_IntProp_StrProp_Batch&) = delete;
    vertex_IntProp_StrProp_Batch(vertex_IntProp_StrProp_Batch&& other) noexcept
            : v_ids(std::move(other.v_ids)), int_props(std::move(other.int_props)), str_props(std::move(other.str_props)) {}
    ~vertex_IntProp_StrProp_Batch() = default;

    inline unsigned size() const { return v_ids.size(); }
    inline unsigned capacity() const { return v_ids.capacity(); }

    inline void emplace_back(int64_t v_id, int64_t int_prop, std::string& str_prop) {
        v_ids.push_back(v_id);
        int_props.push_back(int_prop);
        str_props.push_back(str_prop);
    }

    inline void reset(const uint32_t capacity = batch_size) {
        v_ids.reset(capacity);
        int_props.reset(capacity);
        str_props.reset(capacity, capacity * 64);
    }

    inline void dump_to(brane::serializable_unit &su) {
        su.num_used = 4;
        v_ids.dump_to(su.bufs[0]);
        int_props.dump_to(su.bufs[1]);
        str_props.dump_to(su.bufs[2], su.bufs[3]);
    }

    static inline vertex_IntProp_StrProp_Batch load_from(brane::serializable_unit &&su) {
        return vertex_IntProp_StrProp_Batch{
                VertexBatch{std::move(su.bufs[0])},
                IntPropBatch{std::move(su.bufs[1])},
                StringBatch{std::move(su.bufs[2]), std::move(su.bufs[3])}};
    }
};

struct work_relation {
    int64_t person_id;
    int64_t work_from_year;
    int64_t organisation_id;

    work_relation(int64_t person_id, int64_t work_from_year, int64_t organisation_id)
        : person_id(person_id), work_from_year(work_from_year), organisation_id(organisation_id) {};
    ~work_relation() = default;
};
using work_relation_Batch = brane::cb::column_batch<work_relation>;

struct vertex_organisationId {
    int64_t v_id;
    int64_t organisation_id;

    vertex_organisationId(int64_t v_id, int64_t organisation_id)
        :v_id(v_id), organisation_id(organisation_id) {}
    ~vertex_organisationId() = default;
};
using vertex_organisationId_Batch = brane::cb::column_batch<vertex_organisationId>;

struct create_relation {
    int64_t msg_id;
    int64_t creator;
    int64_t creation_date;

    create_relation(int64_t msg_id, int64_t creator, int64_t creation_date)
        : msg_id(msg_id), creator(creator), creation_date(creation_date) {};
    ~create_relation() = default;
};
using create_relation_Batch = brane::cb::column_batch<create_relation>;

struct like_relation {
    int64_t msg_id;
    int64_t liker;
    int64_t like_date;

    like_relation(int64_t msg_id, int64_t liker, int64_t like_date)
            : msg_id(msg_id), liker(liker), like_date(like_date) {};
    like_relation(const like_relation&) = default;
    like_relation(like_relation&&) = default;
    ~like_relation() = default;

    like_relation& operator=(const like_relation&) = default;
    like_relation& operator=(like_relation&&) = default;
};
using like_relation_Batch = brane::cb::column_batch<like_relation>;

struct vertex_count {
    int64_t v_id;
    unsigned count;

    vertex_count(int64_t v, int64_t cnt) : v_id(v), count(cnt) {};
    vertex_count(const vertex_count&) = default;
    vertex_count(vertex_count&&) noexcept = default;
    ~vertex_count() = default;

    vertex_count& operator=(const vertex_count&) = default;
    vertex_count& operator=(vertex_count&&) noexcept = default;
};
using vertex_count_Batch = brane::cb::column_batch<vertex_count>;

struct count_name_Batch {
    uIntBatch counts;
    StringBatch names;

    explicit count_name_Batch(const uint32_t capacity = batch_size)
        : counts(capacity), names(capacity, capacity * 16) {}
    count_name_Batch(uIntBatch&& counts, StringBatch&& names)
        : counts(std::move(counts)), names(std::move(names)) {}
    count_name_Batch(const count_name_Batch&) = delete;
    count_name_Batch(count_name_Batch&& other) noexcept
        : counts(std::move(other.counts)), names(std::move(other.names)) {}
    ~count_name_Batch() = default;

    inline unsigned size() const { return counts.size(); }
    inline unsigned capacity() const { return counts.capacity(); }

    inline void emplace_back(unsigned count, std::string& name) {
        counts.push_back(count);
        names.push_back(name);
    }

    inline void reset(const uint32_t capacity = batch_size) {
        counts.reset(capacity);
        names.reset(capacity, capacity * 16);
    }

    inline void dump_to(brane::serializable_unit &su) {
        su.num_used = 3;
        counts.dump_to(su.bufs[0]);
        names.dump_to(su.bufs[1], su.bufs[2]);
    }

    static inline count_name_Batch load_from(brane::serializable_unit &&su) {
        return count_name_Batch{
            uIntBatch{std::move(su.bufs[0])},
            StringBatch{std::move(su.bufs[1]), std::move(su.bufs[2])}};
    }
};

struct person_replyOf_msg {
    int64_t person_id;
    int64_t comment_id;
    int64_t msg_id;

    person_replyOf_msg(int64_t person_id, int64_t comment_id, int64_t msg_id)
        : person_id(person_id), comment_id(comment_id), msg_id(msg_id) {};
    ~person_replyOf_msg() = default;
};
using person_replyOf_msg_Batch = brane::cb::column_batch<person_replyOf_msg>;

struct person_comment_tag {
    int64_t person_id;
    int64_t comment_id;
    int64_t tag_id;

    person_comment_tag(int64_t person_id, int64_t comment_id, int64_t tag_id)
        : person_id(person_id), comment_id(comment_id), tag_id(tag_id) {};
    ~person_comment_tag() = default;
};
using person_comment_tag_Batch = brane::cb::column_batch<person_comment_tag>;

// ------
//
//struct EdgeBatch {
//    VertexBatch src;
//    LabelBatch label;
//    VertexBatch dst;
//
//    explicit EdgeBatch(const uint32_t capacity = batch_size)
//        : src(capacity)
//        , label(capacity, capacity * 16)
//        , dst(capacity) {}
//    EdgeBatch(VertexBatch&& src_batch, LabelBatch&& label_batch, VertexBatch&& dst_batch)
//        : src(std::move(src_batch)), label(std::move(label_batch)), dst(std::move(dst_batch)) {}
//    ~EdgeBatch() = default;
//
//    EdgeBatch(const EdgeBatch&) = delete;
//    EdgeBatch(EdgeBatch&& other) noexcept
//        : src(std::move(other.src)), label(std::move(other.label)), dst(std::move(other.dst)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        src.reset(capacity);
//        label.reset(capacity, capacity * 16);
//        dst.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 4;
//        src.dump_to(su.bufs[0]);
//        label.dump_to(su.bufs[1], su.bufs[2]);
//        dst.dump_to(su.bufs[3]);
//    }
//
//    static EdgeBatch load_from(brane::serializable_unit &&su) {
//        return EdgeBatch{VertexBatch{std::move(su.bufs[0])},
//                         LabelBatch{std::move(su.bufs[1]), std::move(su.bufs[2])},
//                         VertexBatch{std::move(su.bufs[3])}};
//    }
//};
//
//struct VtxWithPthBatch {
//    VertexBatch ids;
//    PathBatch paths;
//
//    explicit VtxWithPthBatch(const uint32_t capacity = batch_size)
//        : ids(capacity)
//        , paths(capacity, capacity * sizeof(int64_t)) {}
//    VtxWithPthBatch(VertexBatch&& vertex_batch, PathBatch&& path_batch)
//        : ids(std::move(vertex_batch)), paths(std::move(path_batch)) {}
//    ~VtxWithPthBatch() = default;
//
//    VtxWithPthBatch(const VtxWithPthBatch&) = delete;
//    VtxWithPthBatch(VtxWithPthBatch&& other) noexcept
//        : ids(std::move(other.ids)), paths(std::move(other.paths)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        paths.reset(capacity, capacity * sizeof(int64_t));
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 3;
//        ids.dump_to(su.bufs[0]);
//        paths.dump_to(su.bufs[1], su.bufs[2]);
//    }
//
//    static VtxWithPthBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithPthBatch{VertexBatch{std::move(su.bufs[0])},
//                               PathBatch{std::move(su.bufs[1]), std::move(su.bufs[2])}};
//    }
//};
//
//struct VtxWithPathLenBatch {
//    VertexBatch ids;
//    PathLenBatch path_lens;
//
//    explicit VtxWithPathLenBatch(const uint32_t capacity = batch_size)
//        : ids(capacity), path_lens(capacity) {}
//    VtxWithPathLenBatch(VertexBatch&& vertex_batch, PathLenBatch&& path_len_batch)
//        : ids(std::move(vertex_batch)), path_lens(std::move(path_len_batch)) {}
//    ~VtxWithPathLenBatch() = default;
//
//    VtxWithPathLenBatch(const VtxWithPathLenBatch&) = delete;
//    VtxWithPathLenBatch(VtxWithPathLenBatch&& other) noexcept
//        : ids(std::move(other.ids)), path_lens(std::move(other.path_lens)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        path_lens.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        ids.dump_to(su.bufs[0]);
//        path_lens.dump_to(su.bufs[1]);
//    }
//
//    static VtxWithPathLenBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithPathLenBatch{VertexBatch{std::move(su.bufs[0])},
//                                   PathLenBatch{std::move(su.bufs[1])}};
//    }
//};
//
//struct VtxWithStrPropBatch {
//    VertexBatch ids;
//    StringBatch props;
//
//    explicit VtxWithStrPropBatch(const uint32_t capacity = batch_size)
//        : ids(capacity), props(capacity, capacity * 256) {}
//    VtxWithStrPropBatch(VertexBatch&& vertex_batch, StringBatch&& string_batch)
//        : ids(std::move(vertex_batch)), props(std::move(string_batch)) {}
//    ~VtxWithStrPropBatch() = default;
//
//    VtxWithStrPropBatch(const VtxWithStrPropBatch&) = delete;
//    VtxWithStrPropBatch(VtxWithStrPropBatch&& other) noexcept
//        : ids(std::move(other.ids)), props(std::move(other.props)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        props.reset(capacity, capacity * 256);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 3;
//        ids.dump_to(su.bufs[0]);
//        props.dump_to(su.bufs[1], su.bufs[2]);
//    }
//
//    static VtxWithStrPropBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithStrPropBatch{VertexBatch{std::move(su.bufs[0])},
//                                   StringBatch{std::move(su.bufs[1]), std::move(su.bufs[2])}};
//    }
//};
//
//struct VtxWithNbrDualStrPropBatch {
//    VertexBatch ids;
//    VertexBatch nbr_ids;
//    StringBatch props_1;
//    StringBatch props_2;
//
//    explicit VtxWithNbrDualStrPropBatch(const uint32_t capacity = batch_size)
//            : ids(capacity), nbr_ids(capacity), props_1(capacity, capacity * 32), props_2(capacity, capacity * 32) {}
//    VtxWithNbrDualStrPropBatch(VertexBatch&& vertex_batch, VertexBatch&& nbr_vertex_batch,
//            StringBatch&& string_batch_1, StringBatch&& string_batch_2)
//            : ids(std::move(vertex_batch)), nbr_ids(std::move(nbr_vertex_batch)),
//            props_1(std::move(string_batch_1)), props_2(std::move(string_batch_2)) {}
//    ~VtxWithNbrDualStrPropBatch() = default;
//
//    VtxWithNbrDualStrPropBatch(const VtxWithNbrDualStrPropBatch&) = delete;
//    VtxWithNbrDualStrPropBatch(VtxWithNbrDualStrPropBatch&& other) noexcept
//            : ids(std::move(other.ids)), nbr_ids(std::move(other.nbr_ids)),
//            props_1(std::move(other.props_1)), props_2(std::move(other.props_2)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        nbr_ids.reset(capacity);
//        props_1.reset(capacity, capacity * 32);
//        props_2.reset(capacity, capacity * 32);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 6;
//        ids.dump_to(su.bufs[0]);
//        nbr_ids.dump_to(su.bufs[1]);
//        props_1.dump_to(su.bufs[2], su.bufs[3]);
//        props_1.dump_to(su.bufs[4], su.bufs[5]);
//    }
//
//    static VtxWithNbrDualStrPropBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithNbrDualStrPropBatch{VertexBatch{std::move(su.bufs[0])}, VertexBatch{std::move(su.bufs[1])},
//                                       StringBatch{std::move(su.bufs[2]), std::move(su.bufs[3])},
//                                       StringBatch{std::move(su.bufs[4]), std::move(su.bufs[5])}};
//    }
//};
//
//struct VtxWithMsgCreatorBatch {
//    VertexBatch ids;
//    StringBatch contents;
//    VertexBatch dates;
//    VertexBatch creator_ids;
//
//    explicit VtxWithMsgCreatorBatch(const uint32_t capacity = batch_size)
//            : ids(capacity), contents(capacity, capacity * 256), dates(capacity), creator_ids(capacity) {}
//    VtxWithMsgCreatorBatch(VertexBatch&& vertex_batch_1, StringBatch&& string_batch_1, VertexBatch&& vertex_batch_2,
//            VertexBatch&& vertex_batch_3)
//            : ids(std::move(vertex_batch_1)), contents(std::move(string_batch_1)), dates(std::move(vertex_batch_2)),
//            creator_ids(std::move(vertex_batch_3)) {}
//    ~VtxWithMsgCreatorBatch() = default;
//
//    VtxWithMsgCreatorBatch(const VtxWithMsgCreatorBatch&) = delete;
//    VtxWithMsgCreatorBatch(VtxWithMsgCreatorBatch&& other) noexcept
//            : ids(std::move(other.ids)), contents(std::move(other.contents)), dates(std::move(other.dates)),
//            creator_ids(std::move(other.creator_ids)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        contents.reset(capacity, capacity * 256),
//        dates.reset(capacity),
//        creator_ids.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 5;
//        ids.dump_to(su.bufs[0]);
//        contents.dump_to(su.bufs[1], su.bufs[2]);
//        dates.dump_to(su.bufs[3]);
//        creator_ids.dump_to(su.bufs[4]);
//    }
//
//    static VtxWithMsgCreatorBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithMsgCreatorBatch{VertexBatch{std::move(su.bufs[0])},
//                                      StringBatch{std::move(su.bufs[1]), std::move(su.bufs[2])},
//                                      VertexBatch{std::move(su.bufs[3])},
//                                      VertexBatch{std::move(su.bufs[4])}};
//    }
//};
//
//struct VtxWithIntPropBatch {
//    VertexBatch ids;
//    VertexBatch props;
//
//    explicit VtxWithIntPropBatch(const uint32_t capacity = batch_size)
//            : ids(capacity), props(capacity) {}
//    VtxWithIntPropBatch(VertexBatch&& vertex_batch, VertexBatch&& prop_batch)
//            : ids(std::move(vertex_batch)), props(std::move(prop_batch)) {}
//    ~VtxWithIntPropBatch() = default;
//
//    VtxWithIntPropBatch(const VtxWithIntPropBatch&) = delete;
//    VtxWithIntPropBatch(VtxWithIntPropBatch&& other) noexcept
//            : ids(std::move(other.ids)), props(std::move(other.props)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        props.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        ids.dump_to(su.bufs[0]);
//        props.dump_to(su.bufs[1]);
//    }
//
//    static VtxWithIntPropBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithIntPropBatch{VertexBatch{std::move(su.bufs[0])},
//                                   VertexBatch{std::move(su.bufs[1])}};
//    }
//};
//
//struct VtxWithIntAndStrPropBatch {
//    VertexBatch ids;
//    IntPropBatch int_props;
//    StringBatch str_props;
//
//    explicit VtxWithIntAndStrPropBatch(const uint32_t capacity = batch_size)
//        : ids(capacity), int_props(capacity), str_props(capacity, capacity * 32) {}
//    VtxWithIntAndStrPropBatch(VertexBatch&& vertex_batch, IntPropBatch&& int_prop_batch, StringBatch&& str_prop_batch)
//        : ids(std::move(vertex_batch)), int_props(std::move(int_prop_batch)), str_props(std::move(str_prop_batch)) {}
//    ~VtxWithIntAndStrPropBatch() = default;
//
//    VtxWithIntAndStrPropBatch(const VtxWithIntAndStrPropBatch&) = delete;
//    VtxWithIntAndStrPropBatch(VtxWithIntAndStrPropBatch&& other) noexcept
//        : ids(std::move(other.ids)), int_props(std::move(other.int_props)), str_props(std::move(other.str_props)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        int_props.reset(capacity);
//        str_props.reset(capacity, capacity * 32);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 4;
//        ids.dump_to(su.bufs[0]);
//        int_props.dump_to(su.bufs[1]);
//        str_props.dump_to(su.bufs[2], su.bufs[3]);
//    }
//
//    static VtxWithIntAndStrPropBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithIntAndStrPropBatch{VertexBatch{std::move(su.bufs[0])},
//                                   IntPropBatch{std::move(su.bufs[1])},
//                                   StringBatch{std::move(su.bufs[2]), std::move(su.bufs[3])}};
//    }
//};
//
//struct VtxWithTagNodeBatch {
//    VertexBatch ids;
//    TagNodeBatch tag_nodes;
//
//    explicit VtxWithTagNodeBatch(const uint32_t capacity = batch_size)
//        : ids(capacity), tag_nodes(capacity) {}
//    VtxWithTagNodeBatch(VertexBatch&& vertex_batch, TagNodeBatch&& tag_node_batch)
//        : ids(std::move(vertex_batch)), tag_nodes(std::move(tag_node_batch)) {}
//    ~VtxWithTagNodeBatch() = default;
//
//    VtxWithTagNodeBatch(const VtxWithTagNodeBatch&) = delete;
//    VtxWithTagNodeBatch(VtxWithTagNodeBatch&& other) noexcept
//        : ids(std::move(other.ids)), tag_nodes(std::move(other.tag_nodes)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        tag_nodes.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        ids.dump_to(su.bufs[0]);
//        tag_nodes.dump_to(su.bufs[1]);
//    }
//
//    static VtxWithTagNodeBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithTagNodeBatch{VertexBatch{std::move(su.bufs[0])},
//                                   TagNodeBatch{std::move(su.bufs[1])}};
//    }
//};
//
//struct VtxWithTwoTagNodesBatch {
//    VertexBatch ids;
//    TagNodeBatch first_tag_nodes;
//    TagNodeBatch second_tag_nodes;
//
//    explicit VtxWithTwoTagNodesBatch(const uint32_t capacity = batch_size)
//        : ids(capacity), first_tag_nodes(capacity), second_tag_nodes(capacity) {}
//    VtxWithTwoTagNodesBatch(VertexBatch&& vertex_batch, TagNodeBatch&& first_tag_node_batch, TagNodeBatch&& second_tag_node_batch)
//        : ids(std::move(vertex_batch)), first_tag_nodes(std::move(first_tag_node_batch))
//        , second_tag_nodes(std::move(second_tag_node_batch)) {}
//    ~VtxWithTwoTagNodesBatch() = default;
//
//    VtxWithTwoTagNodesBatch(const VtxWithTwoTagNodesBatch&) = delete;
//    VtxWithTwoTagNodesBatch(VtxWithTwoTagNodesBatch&& other) noexcept
//        : ids(std::move(other.ids)), first_tag_nodes(std::move(other.first_tag_nodes))
//        , second_tag_nodes(std::move(other.second_tag_nodes)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        first_tag_nodes.reset(capacity);
//        second_tag_nodes.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 3;
//        ids.dump_to(su.bufs[0]);
//        first_tag_nodes.dump_to(su.bufs[1]);
//        second_tag_nodes.dump_to(su.bufs[2]);
//    }
//
//    static VtxWithTwoTagNodesBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithTwoTagNodesBatch{VertexBatch{std::move(su.bufs[0])},
//                                       TagNodeBatch{std::move(su.bufs[1])},
//                                       TagNodeBatch{std::move(su.bufs[2])}};
//    }
//};
//
//struct VtxWithDualIntBatch {
//    VertexBatch ids;
//    IntPropBatch first;
//    IntPropBatch second;
//
//    explicit VtxWithDualIntBatch(const uint32_t capacity = batch_size)
//            : ids(capacity), first(capacity), second(capacity) {}
//    VtxWithDualIntBatch(VertexBatch&& vertex_batch, IntPropBatch&& first_tag_node_batch, IntPropBatch&& second_tag_node_batch)
//            : ids(std::move(vertex_batch)), first(std::move(first_tag_node_batch))
//            , second(std::move(second_tag_node_batch)) {}
//    ~VtxWithDualIntBatch() = default;
//
//    VtxWithDualIntBatch(const VtxWithDualIntBatch&) = delete;
//    VtxWithDualIntBatch(VtxWithDualIntBatch&& other) noexcept
//            : ids(std::move(other.ids)), first(std::move(other.first))
//            , second(std::move(other.second)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        first.reset(capacity);
//        second.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 3;
//        ids.dump_to(su.bufs[0]);
//        first.dump_to(su.bufs[1]);
//        second.dump_to(su.bufs[2]);
//    }
//
//    static VtxWithDualIntBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithDualIntBatch{VertexBatch{std::move(su.bufs[0])},
//                                   IntPropBatch{std::move(su.bufs[1])},
//                                   IntPropBatch{std::move(su.bufs[2])}};
//    }
//};
//
//struct VtxWithTagNodeAndIntPropBatch {
//    VertexBatch ids;
//    TagNodeBatch tag_nodes;
//    IntPropBatch props;
//
//    explicit VtxWithTagNodeAndIntPropBatch(const uint32_t capacity = batch_size)
//        : ids(capacity), tag_nodes(capacity), props(capacity) {}
//    VtxWithTagNodeAndIntPropBatch(VertexBatch&& vertex_batch, TagNodeBatch&& tag_node_batch, IntPropBatch&& prop_batch)
//        : ids(std::move(vertex_batch)), tag_nodes(std::move(tag_node_batch)), props(std::move(prop_batch)) {}
//    ~VtxWithTagNodeAndIntPropBatch() = default;
//
//    VtxWithTagNodeAndIntPropBatch(const VtxWithTagNodeAndIntPropBatch&) = delete;
//    VtxWithTagNodeAndIntPropBatch(VtxWithTagNodeAndIntPropBatch&& other) noexcept
//        : ids(std::move(other.ids)), tag_nodes(std::move(other.tag_nodes)), props(std::move(other.props)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        ids.reset(capacity);
//        tag_nodes.reset(capacity);
//        props.reset(capacity);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 3;
//        ids.dump_to(su.bufs[0]);
//        tag_nodes.dump_to(su.bufs[1]);
//        props.dump_to(su.bufs[2]);
//    }
//
//    static VtxWithTagNodeAndIntPropBatch load_from(brane::serializable_unit &&su) {
//        return VtxWithTagNodeAndIntPropBatch{VertexBatch{std::move(su.bufs[0])},
//                                   TagNodeBatch{std::move(su.bufs[1])},
//                                   IntPropBatch{std::move(su.bufs[2])}};
//    }
//};
//
//struct TwoVtxWithStrPropBatch {
//    VertexBatch first_ids;
//    VertexBatch second_ids;
//    StringBatch props;
//
//    explicit TwoVtxWithStrPropBatch(const uint32_t capacity = batch_size)
//        : first_ids(capacity), second_ids(capacity), props(capacity, capacity * 32) {}
//    TwoVtxWithStrPropBatch(VertexBatch&& first_vertex_batch, VertexBatch&& second_vertex_batch,
//                           StringBatch&& prop_batch)
//        : first_ids(std::move(first_vertex_batch)), second_ids(std::move(second_vertex_batch))
//        , props(std::move(prop_batch)) {}
//    ~TwoVtxWithStrPropBatch() = default;
//
//    TwoVtxWithStrPropBatch(const TwoVtxWithStrPropBatch&) = delete;
//    TwoVtxWithStrPropBatch(TwoVtxWithStrPropBatch&& other) noexcept
//        : first_ids(std::move(other.first_ids)), second_ids(std::move(other.second_ids))
//        , props(std::move(other.props)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        first_ids.reset(capacity);
//        second_ids.reset(capacity);
//        props.reset(capacity, capacity * 32);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 4;
//        first_ids.dump_to(su.bufs[0]);
//        second_ids.dump_to(su.bufs[1]);
//        props.dump_to(su.bufs[2], su.bufs[3]);
//    }
//
//    static TwoVtxWithStrPropBatch load_from(brane::serializable_unit &&su) {
//        return TwoVtxWithStrPropBatch{VertexBatch{std::move(su.bufs[0])},
//                                      VertexBatch{std::move(su.bufs[1])},
//                                      StringBatch{std::move(su.bufs[2]), std::move(su.bufs[3])}};
//    }
//};
//
//
//struct VertexBatchWithBranch {
//    uint32_t owner;
//    VertexBatch ids;
//
//    explicit VertexBatchWithBranch(uint32_t owner_vertex, const uint32_t capacity = batch_size)
//            : owner(owner_vertex), ids(capacity) {}
//    VertexBatchWithBranch(uint32_t owner_vertex, VertexBatch&& vertex_batch)
//            : owner(owner_vertex), ids(std::move(vertex_batch)) {}
//    VertexBatchWithBranch(const VertexBatchWithBranch&) = delete;
//    VertexBatchWithBranch(VertexBatchWithBranch&& other) noexcept
//            : owner(other.owner), ids(std::move(other.ids)) {}
//    ~VertexBatchWithBranch() = default;
//
//    inline void reset(const uint32_t capacity = batch_size) { ids.reset(capacity); }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(uint32_t));
//        memcpy(su.bufs[0].get_write(), &owner, sizeof(uint32_t));
//        ids.dump_to(su.bufs[1]);
//    }
//
//    static VertexBatchWithBranch load_from(brane::serializable_unit &&su) {
//        auto new_owner = *reinterpret_cast<const uint32_t*>(su.bufs[0].get());
//        return VertexBatchWithBranch{new_owner, VertexBatch{std::move(su.bufs[1])}};
//    }
//
//};
//
//
//struct VertexBatchInWhere {
//    int64_t owner;
//    VertexBatch ids;
//
//    explicit VertexBatchInWhere(int64_t owner_vertex, const uint32_t capacity = batch_size)
//        : owner(owner_vertex), ids(capacity) {}
//    VertexBatchInWhere(int64_t owner_vertex, VertexBatch&& vertex_batch)
//        : owner(owner_vertex), ids(std::move(vertex_batch)) {}
//    VertexBatchInWhere(const VertexBatchInWhere&) = delete;
//    VertexBatchInWhere(VertexBatchInWhere&& other) noexcept
//        : owner(other.owner), ids(std::move(other.ids)) {}
//    ~VertexBatchInWhere() = default;
//
//    inline void reset(const uint32_t capacity = batch_size) { ids.reset(capacity); }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(int64_t));
//        memcpy(su.bufs[0].get_write(), &owner, sizeof(int64_t));
//        ids.dump_to(su.bufs[1]);
//    }
//
//    static VertexBatchInWhere load_from(brane::serializable_unit &&su) {
//        auto new_owner = *reinterpret_cast<const int64_t*>(su.bufs[0].get());
//        return VertexBatchInWhere{new_owner, VertexBatch{std::move(su.bufs[1])}};
//    }
//
//};
//
//struct VertexBatchWithBranchInfo {
//    unsigned branch_id;
//    unsigned owner_shard;
//    VertexBatch ids;
//
//    explicit VertexBatchWithBranchInfo(unsigned branch_id, unsigned owner_shard, const uint32_t capacity = batch_size)
//        : branch_id(branch_id), owner_shard(owner_shard), ids(capacity) {}
//    VertexBatchWithBranchInfo(unsigned branch_id, unsigned owner_shard, VertexBatch&& vertex_batch)
//        : branch_id(branch_id), owner_shard(owner_shard), ids(std::move(vertex_batch)) {}
//    VertexBatchWithBranchInfo(const VertexBatchWithBranchInfo&) = delete;
//    VertexBatchWithBranchInfo(VertexBatchWithBranchInfo&& other) noexcept
//        : branch_id(other.branch_id), owner_shard(other.owner_shard), ids(std::move(other.ids)) {}
//    ~VertexBatchWithBranchInfo() = default;
//
//    inline void reset(const uint32_t capacity = batch_size) { ids.reset(capacity); }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 3;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(unsigned));
//        memcpy(su.bufs[0].get_write(), &branch_id, sizeof(unsigned));
//
//        su.bufs[1] = seastar::temporary_buffer<char>(sizeof(unsigned));
//        memcpy(su.bufs[1].get_write(), &owner_shard, sizeof(unsigned));
//
//        ids.dump_to(su.bufs[2]);
//    }
//
//    static VertexBatchWithBranchInfo load_from(brane::serializable_unit &&su) {
//        auto new_branch_id = *reinterpret_cast<const unsigned*>(su.bufs[0].get());
//        auto new_owner_shard = *reinterpret_cast<const unsigned*>(su.bufs[1].get());
//        return VertexBatchWithBranchInfo{new_branch_id, new_owner_shard, VertexBatch{std::move(su.bufs[2])}};
//    }
//};
//
//struct EosInWhere {
//    int64_t owner;
//    xEos eos;
//
//    explicit EosInWhere(int64_t owner_vertex): owner(owner_vertex), eos() {}
//    EosInWhere(int64_t owner_vertex, xEos&& other_eos)
//        : owner(owner_vertex), eos(std::move(other_eos)) {}
//    EosInWhere(const EosInWhere&) = delete;
//    EosInWhere(EosInWhere&& other) noexcept
//        : owner(other.owner), eos(std::move(other.eos)) {}
//    ~EosInWhere() = default;
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(int64_t));
//        memcpy(su.bufs[0].get_write(), &owner, sizeof(int64_t));
//        eos.steps.dump_to(su.bufs[1]);
//    }
//
//    static EosInWhere load_from(brane::serializable_unit &&su) {
//        auto new_owner = *reinterpret_cast<const int64_t*>(su.bufs[0].get());
//        return EosInWhere{new_owner, xEos{brane::cb::column_batch<xEos::eos_step>{std::move(su.bufs[1])}} };
//    }
//};
//
//struct EosWithBranchId {
//    unsigned branch_id;
//    xEos eos;
//
//    EosWithBranchId(unsigned branch_id, xEos&& eos)
//        : branch_id(branch_id), eos(std::move(eos)) {}
//    EosWithBranchId(const EosWithBranchId&) = delete;
//    EosWithBranchId(EosWithBranchId&& other) noexcept
//        : branch_id(other.branch_id), eos(std::move(other.eos)) {}
//    ~EosWithBranchId() = default;
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(unsigned));
//        memcpy(su.bufs[0].get_write(), &branch_id, sizeof(unsigned));
//
//        eos.steps.dump_to(su.bufs[1]);
//    }
//
//    static EosWithBranchId load_from(brane::serializable_unit &&su) {
//        auto new_branch_id = *reinterpret_cast<const unsigned*>(su.bufs[0].get());
//        return EosWithBranchId{new_branch_id, xEos{brane::cb::column_batch<xEos::eos_step>{std::move(su.bufs[1])}} };
//    }
//};
//
//template <typename T>
//struct EosInWhereWithStructOwner {
//    T owner;
//    xEos eos;
//
//    explicit EosInWhereWithStructOwner(const T& owner): owner(owner), eos() {}
//    explicit EosInWhereWithStructOwner(T&& owner): owner(std::move(owner)), eos() {}
//    EosInWhereWithStructOwner(const T& owner, xEos&& other_eos)
//        : owner(owner), eos(std::move(other_eos)) {}
//    EosInWhereWithStructOwner(T&& owner, xEos&& other_eos)
//        : owner(std::move(owner)), eos(std::move(other_eos)) {}
//    EosInWhereWithStructOwner(const EosInWhereWithStructOwner&) = delete;
//    EosInWhereWithStructOwner(EosInWhereWithStructOwner&& other) noexcept
//        : owner(std::move(other.owner)), eos(std::move(other.eos)) {}
//    ~EosInWhereWithStructOwner() = default;
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(T));
//        memcpy(su.bufs[0].get_write(), &owner, sizeof(T));
//        eos.steps.dump_to(su.bufs[1]);
//    }
//
//    static EosInWhereWithStructOwner load_from(brane::serializable_unit &&su) {
//        auto new_owner = *reinterpret_cast<const T*>(su.bufs[0].get());
//        return EosInWhereWithStructOwner{new_owner, xEos{brane::cb::column_batch<xEos::eos_step>{std::move(su.bufs[1])}} };
//    }
//};
//
//struct EosWithTagNodeInWhereBranch {
//    uint32_t branch;
//    int64_t owner;
//    int64_t tag_node;
//    xEos eos;
//
//    EosWithTagNodeInWhereBranch(uint32_t bid, int64_t owner_vertex, int64_t tag_node_vertex)
//            : branch(bid), owner(owner_vertex), tag_node(tag_node_vertex), eos() {}
//    EosWithTagNodeInWhereBranch(uint32_t bid, int64_t owner_vertex, int64_t tag_node_vertex, xEos&& other_eos)
//            : branch(bid), owner(owner_vertex), tag_node(tag_node_vertex), eos(std::move(other_eos)) {}
//    EosWithTagNodeInWhereBranch(const EosWithTagNodeInWhereBranch&) = delete;
//    EosWithTagNodeInWhereBranch(EosWithTagNodeInWhereBranch&& other) noexcept
//            : branch(other.branch), owner(other.owner), tag_node(other.tag_node), eos(std::move(other.eos)) {}
//    ~EosWithTagNodeInWhereBranch() = default;
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(uint32_t) + sizeof(int64_t) * 2);
//        memcpy(su.bufs[0].get_write(), &branch, sizeof(uint32_t));
//        memcpy(su.bufs[0].get_write() + sizeof(uint32_t), &owner, sizeof(int64_t));
//        memcpy(su.bufs[0].get_write() + sizeof(uint32_t)+ sizeof(int64_t), &tag_node, sizeof(int64_t));
//        eos.steps.dump_to(su.bufs[1]);
//    }
//
//    static EosWithTagNodeInWhereBranch load_from(brane::serializable_unit &&su) {
//        auto new_branch = *reinterpret_cast<const uint32_t*>(su.bufs[0].get());
//        auto new_owner = *reinterpret_cast<const int64_t*>(su.bufs[0].get() + sizeof(uint32_t));
//        auto new_tag_node = *reinterpret_cast<const int64_t*>(su.bufs[0].get() + sizeof(uint32_t) + sizeof(int64_t));
//        return EosWithTagNodeInWhereBranch{new_branch, new_owner, new_tag_node,
//                                           xEos{brane::cb::column_batch<xEos::eos_step>{std::move(su.bufs[1])}} };
//    }
//};
//
//struct EosWithTagNodeInWhere {
//    int64_t owner;
//    int64_t tag_node;
//    xEos eos;
//
//    EosWithTagNodeInWhere(int64_t owner_vertex, int64_t tag_node_vertex)
//        : owner(owner_vertex), tag_node(tag_node_vertex), eos() {}
//    EosWithTagNodeInWhere(int64_t owner_vertex, int64_t tag_node_vertex, xEos&& other_eos)
//        : owner(owner_vertex), tag_node(tag_node_vertex), eos(std::move(other_eos)) {}
//    EosWithTagNodeInWhere(const EosWithTagNodeInWhere&) = delete;
//    EosWithTagNodeInWhere(EosWithTagNodeInWhere&& other) noexcept
//        : owner(other.owner), tag_node(other.tag_node), eos(std::move(other.eos)) {}
//    ~EosWithTagNodeInWhere() = default;
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 2;
//        su.bufs[0] = seastar::temporary_buffer<char>(sizeof(int64_t) * 2);
//        memcpy(su.bufs[0].get_write(), &owner, sizeof(int64_t));
//        memcpy(su.bufs[0].get_write() + sizeof(int64_t), &tag_node, sizeof(int64_t));
//        eos.steps.dump_to(su.bufs[1]);
//    }
//
//    static EosWithTagNodeInWhere load_from(brane::serializable_unit &&su) {
//        auto new_owner = *reinterpret_cast<const int64_t*>(su.bufs[0].get());
//        auto new_tag_node = *reinterpret_cast<const int64_t*>(su.bufs[0].get() + sizeof(int64_t));
//        return EosWithTagNodeInWhere{new_owner, new_tag_node,
//                                     xEos{brane::cb::column_batch<xEos::eos_step>{std::move(su.bufs[1])}} };
//    }
//};
//
//struct node_3 {
//    int64_t msg_id;
//    int64_t msg_creation_date;
//    int64_t liker_id;
//    int64_t like_happen_date;
//    int64_t is_friend;
//
//    node_3(int64_t msg_id, int64_t msg_creation_date, int64_t liker_id, int64_t like_happen_date, int64_t is_friend)
//            : msg_id(msg_id), msg_creation_date(msg_creation_date)
//            , liker_id(liker_id), like_happen_date(like_happen_date), is_friend(is_friend) {}
//    node_3(const node_3&) = default;
//    node_3(node_3&&) = default;
//    node_3& operator=(const node_3& other) = default;
//    node_3& operator=(node_3&& other) = default;
//};
//using node_3_batch = brane::cb::column_batch<node_3>;
//
//struct Q7PropBatch {
//    node_3_batch nodes;
//    StringBatch first_name;
//    StringBatch second_name;
//
//    explicit Q7PropBatch(const uint32_t capacity = batch_size)
//            : nodes(capacity), first_name(capacity, capacity * 16), second_name(capacity, capacity * 16) {}
//    Q7PropBatch(node_3_batch&& in_nodes, StringBatch&& first_name_batch, StringBatch&& second_name_batch)
//            : nodes(std::move(in_nodes)), first_name(std::move(first_name_batch)), second_name(std::move(second_name_batch)) {}
//    ~Q7PropBatch() = default;
//
//    Q7PropBatch(const Q7PropBatch&) = delete;
//    Q7PropBatch(Q7PropBatch&& other) noexcept
//            : nodes(std::move(other.nodes)), first_name(std::move(other.first_name)), second_name(std::move(other.second_name)) {}
//
//    inline void reset(const uint32_t capacity = batch_size) {
//        nodes.reset(capacity);
//        first_name.reset(capacity, capacity * 16);
//        second_name.reset(capacity, capacity * 16);
//    }
//
//    void dump_to(brane::serializable_unit &su) {
//        su.num_used = 5;
//        nodes.dump_to(su.bufs[0]);
//        first_name.dump_to(su.bufs[1], su.bufs[2]);
//        second_name.dump_to(su.bufs[1], su.bufs[2]);
//    }
//
//    static Q7PropBatch load_from(brane::serializable_unit &&su) {
//        return Q7PropBatch{node_3_batch{std::move(su.bufs[0])},
//                                   StringBatch{std::move(su.bufs[1]), std::move(su.bufs[2])},
//                                   StringBatch{std::move(su.bufs[3]), std::move(su.bufs[4])},};
//    }
//};
