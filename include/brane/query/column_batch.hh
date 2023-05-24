#pragma once

#include <brane/util/common-utils.hh>
#include <brane/actor/actor_message.hh>
#include <cstring>
#include <cassert>
#include <tuple>

#include "seastar/core/temporary_buffer.hh"

namespace brane {

namespace cb {

using byte_tmpbuf_t = seastar::temporary_buffer<char>;

template <typename T>
class fixed_column_batch {
    struct buf_wrapper;
    buf_wrapper _bw;
    uint32_t _size;
    uint32_t _capacity;
public:
    explicit fixed_column_batch(const uint32_t capacity)
        : _bw(capacity), _size(0), _capacity(capacity) {}

    explicit fixed_column_batch(byte_tmpbuf_t&& tb)
        : _bw(std::move(tb)), _size(_bw.used()), _capacity(_size) {}

    fixed_column_batch(const fixed_column_batch& x) = delete;
    fixed_column_batch(fixed_column_batch&& x) noexcept;
    ~fixed_column_batch() = default;

    fixed_column_batch& operator=(const fixed_column_batch& other) = delete;
    fixed_column_batch& operator=(fixed_column_batch&& other) noexcept;

    fixed_column_batch share();

    void dump_to(byte_tmpbuf_t &tmp_buf);
    void dump_to(serializable_unit &su);

    T& operator[](const size_t idx) {
        return _bw.data[idx];
    }

    const T& operator[](const size_t idx) const {
        return _bw.data[idx];
    }

    inline void push_back(const T& data) {
        _bw.data[_size++] = data;
    }

    template <typename... Args>
    inline void emplace_back(Args&&... args) {
        new (_bw.data + _size++) T(args...);
    }

    inline void erase_tail() {
        if(_size > 0) {
            _size--;
        }
    }

    inline void reset(const uint32_t capacity) {
        fixed_column_batch<T> tmp(capacity);
        _bw = std::move(tmp._bw);
        _size = tmp._size;
        _capacity = tmp._capacity;
        tmp._capacity = 0;
        tmp._size = 0;
    }

    inline uint32_t size() const {
        return _size;
    }

    inline uint32_t capacity() const {
        return _capacity;
    }

    static const bool fixed = true;

private:
    fixed_column_batch(buf_wrapper&& bw, uint32_t size, uint32_t cap)
        : _bw(std::move(bw)), _size(size), _capacity(cap) {}
};

template <typename T>
class dynamic_column_batch {
    struct buf_wrapper;
    struct metadata {
        uint32_t offset;
        uint32_t len;
    };
    using data_type = typename T::data_type;
    buf_wrapper _bw;
    uint32_t _size;
    uint32_t _capacity;
public:
    dynamic_column_batch(const uint32_t capacity, const uint32_t init_reserved)
        : _bw(capacity, init_reserved), _size(0), _capacity(capacity) {}

    explicit dynamic_column_batch(byte_tmpbuf_t&& tb1, byte_tmpbuf_t&& tb2)
        : _bw(std::move(tb1), std::move(tb2)), _size(_bw.used()), _capacity(_size) {}

    dynamic_column_batch(const dynamic_column_batch& x) = delete;
    dynamic_column_batch(dynamic_column_batch&& x) noexcept;
    ~dynamic_column_batch() = default;

    dynamic_column_batch& operator=(const dynamic_column_batch& other) = delete;
    dynamic_column_batch& operator=(dynamic_column_batch&& other) noexcept;

    dynamic_column_batch share();

    void dump_to(byte_tmpbuf_t &tb1, byte_tmpbuf_t &tb2);
    void dump_to(serializable_unit &su);

    const T operator[](const size_t idx) const {
        return T{_bw.data + _bw.meta[idx].offset, _bw.meta[idx].len};
    }

    T operator[](const size_t idx) {
        return T{_bw.data + _bw.meta[idx].offset, _bw.meta[idx].len};
    }

    void reset(const uint32_t capacity, const uint32_t init_reserved) {
        dynamic_column_batch<T> tmp(capacity, init_reserved);
        _bw = std::move(tmp._bw);
        _size = tmp._size;
        _capacity = tmp._capacity;
        tmp._size = 0;
        tmp._capacity = 0;
    }

    inline const size_t size() const {
        return _size;
    }

    inline const int capacity() const {
        return _capacity;
    }

    static const bool fixed = false;

protected:
    void push_back_helper(const data_type* src, uint32_t len);
    void push_back_helper(const data_type* src, uint32_t len, const data_type& val);

private:
    dynamic_column_batch(buf_wrapper&& bw, uint32_t size, uint32_t cap)
        : _bw(std::move(bw)), _size(size), _capacity(cap) {}
};

template <typename T>
struct fixed_column_batch<T>::buf_wrapper {
    byte_tmpbuf_t buf;
    T* data;
public:
    explicit buf_wrapper(const uint32_t capacity)
        : buf(buf_size(capacity)), data(data_ptr()) {}

    explicit buf_wrapper(byte_tmpbuf_t&& tb)
        : buf(std::move(tb)), data(data_ptr()) {}

    buf_wrapper(const buf_wrapper& x) = delete;
    buf_wrapper(buf_wrapper&& x) noexcept : buf(std::move(x.buf)), data(x.data) {
        x.data = nullptr;
    }
    ~buf_wrapper() = default;

    buf_wrapper& operator=(const buf_wrapper& other) = delete;
    buf_wrapper& operator=(buf_wrapper&& other) noexcept {
        if (this != &other) {
            buf = std::move(other.buf);
            data = other.data;
            other.data = nullptr;
        }
        return *this;
    }

    inline buf_wrapper share() {
        return buf_wrapper{buf.share(), data};
    }

    inline uint32_t get_length() {
        return used() * sizeof(T) + offset_of_data();
    }

    inline uint32_t& used() {
        return reinterpret_cast<layout*>(buf.get_write())->size;
    }

private:
    struct layout {
        uint32_t size;
        T fake_data;
    };

    explicit buf_wrapper(byte_tmpbuf_t&& tb, T* data)
        : buf(std::move(tb)), data(data) {}

    inline size_t buf_size(const uint32_t capacity) const {
        return offset_of_data() + sizeof(T) * capacity;
    }

    inline constexpr size_t offset_of_data() const {
        return offset_of(&layout::fake_data);
    }

    inline T* data_ptr() {
        return reinterpret_cast<T*>(buf.get_write() + offset_of_data());
    }
};

template <typename T>
struct dynamic_column_batch<T>::buf_wrapper {
    byte_tmpbuf_t meta_buf;
    byte_tmpbuf_t data_buf;
    data_type* data;
    metadata* meta;
    uint32_t occupied;
    uint32_t _reserved;
public:
    explicit buf_wrapper(const uint32_t cap, const uint32_t reserved)
        : meta_buf(metabuf_size(cap)), data_buf(databuf_size(reserved))
        , data(data_ptr()), meta(meta_ptr())
        , occupied(0), _reserved(reserved) {}

    explicit buf_wrapper(byte_tmpbuf_t&& tb1, byte_tmpbuf_t&& tb2)
        : meta_buf(std::move(tb1)), data_buf(std::move(tb2))
        , data(data_ptr()), meta(meta_ptr())
        , occupied(get_occupied()), _reserved(occupied) {}

    buf_wrapper(const buf_wrapper& x) = delete;
    buf_wrapper(buf_wrapper&& x) noexcept : meta_buf(std::move(x.meta_buf))
        , data_buf(std::move(x.data_buf)), data(x.data), meta(x.meta)
        , occupied(x.occupied), _reserved(x._reserved) {
        x.clear_all();
    }
    ~buf_wrapper() = default;

    buf_wrapper& operator=(const buf_wrapper& other) = delete;
    buf_wrapper& operator=(buf_wrapper&& other) noexcept {
        if (this != &other) {
            meta_buf = std::move(other.meta_buf);
            data_buf = std::move(other.data_buf);
            member_substitute(other);
            other.clear_all();
        }
        return *this;
    }

    inline buf_wrapper share() {
        return buf_wrapper{meta_buf.share(), data_buf.share(),
            data, meta, occupied, _reserved};
    }

    inline uint32_t get_meta_length() {
        return used() * sizeof(metadata) + offset_of_meta();
    }

    inline uint32_t get_data_length() {
        return occupied * sizeof(data_type);
    }

    inline bool empty() const {
        return meta_buf.empty() && data_buf.empty();
    }

    inline uint32_t& used() {
        return reinterpret_cast<layout*>(meta_buf.get_write())->size;
    }

    inline void ensure_space(const uint32_t len) {
        if (occupied + len > _reserved) {
            // double reserved size.
            auto new_databuf = byte_tmpbuf_t{databuf_size(2 * (occupied + len))};
            memcpy(new_databuf.get_write(), data_buf.get(), databuf_size(occupied));
            data_buf = std::move(new_databuf);
            _reserved = 2 * (occupied + len);
            meta = meta_ptr();
            data = data_ptr();
        }
    }

private:
    struct layout {
        uint32_t size;
        metadata fake_meta;
    };

    explicit buf_wrapper(byte_tmpbuf_t&& tb1, byte_tmpbuf_t&& tb2, data_type *data,
        metadata *meta, uint32_t occupied, uint32_t reserved)
        : meta_buf(std::move(tb1)), data_buf(std::move(tb2)), data(data)
        , meta(meta), occupied(occupied), _reserved(reserved) {}

    inline size_t metabuf_size(const uint32_t capacity) const {
        return offset_of_meta() + sizeof(metadata) * capacity;
    }

    inline size_t databuf_size(const uint32_t size) const {
        return sizeof(data_type) * size;
    }

    inline constexpr size_t offset_of_meta() const {
        return offset_of(&layout::fake_meta);
    }

    inline metadata* meta_ptr() {
        return reinterpret_cast<metadata*>(meta_buf.get_write() + offset_of_meta());
    }

    inline data_type* data_ptr() {
        return reinterpret_cast<data_type*>(data_buf.get_write());
    }

    inline uint32_t get_occupied() {
        auto &mt = meta[used() - 1];
        return mt.len + mt.offset;
    }

    inline void clear_all() {
        data = nullptr;
        meta = nullptr;
        occupied = 0;
        _reserved = 0;
    }

    inline void member_substitute(const buf_wrapper& other) {
        data = other.data;
        meta = other.meta;
        occupied = other.occupied;
        _reserved = other._reserved;
    }
};

template <typename T>
inline
fixed_column_batch<T>::fixed_column_batch(fixed_column_batch&& x) noexcept
    : _bw(std::move(x._bw)), _size(x._size), _capacity(x._capacity) {
    x._size = 0;
    x._capacity = 0;
}

template <typename T>
inline
fixed_column_batch<T>& fixed_column_batch<T>::operator=(fixed_column_batch<T>&& other) noexcept {
    if (this != &other) {
        _bw = std::move(other._bw);
        _size = other._size;
        _capacity = other._capacity;
        other._capacity = 0;
        other._size = 0;
    }
    return *this;
}

template <typename T>
inline
fixed_column_batch<T> fixed_column_batch<T>::share() {
    return fixed_column_batch{_bw.share(), _size, _capacity};
}

template <typename T>
inline
void fixed_column_batch<T>::dump_to(byte_tmpbuf_t &tmp_buf) {
    if (_bw.buf.get()) {
        _bw.used() = _size;
        _capacity = 0;
        _size = 0;
        buf_wrapper bw = std::move(_bw);
        tmp_buf = bw.buf.share(0, bw.get_length());
    }
}

template <typename T>
inline
void fixed_column_batch<T>::dump_to(serializable_unit &su) {
    su.num_used = 1;
    dump_to(su.bufs[0]);
}

template <typename T>
inline
dynamic_column_batch<T>::dynamic_column_batch(dynamic_column_batch&& x) noexcept
    : _bw(std::move(x._bw)), _size(x._size), _capacity(x._capacity) {
    x._size = 0;
    x._capacity = 0;
}

template <typename T>
inline
dynamic_column_batch<T>& dynamic_column_batch<T>::operator=(dynamic_column_batch&& other) noexcept {
    if (this != &other) {
        _bw = std::move(other._bw);
        _size = other._size;
        _capacity = other._capacity;
        other._size = 0;
        other._capacity = 0;
    }
    return *this;
}

template <typename T>
inline
dynamic_column_batch<T> dynamic_column_batch<T>::share() {
    return dynamic_column_batch{_bw.share(), _size, _capacity};
}

template <typename T>
inline
void dynamic_column_batch<T>::dump_to(byte_tmpbuf_t &tb1, byte_tmpbuf_t &tb2) {
    if (!_bw.empty()) {
        _bw.used() = _size;
        _capacity = 0;
        _size = 0;
        auto bw = std::move(_bw);
        tb1 = bw.meta_buf.share(0, bw.get_meta_length());
        tb2 = bw.data_buf.share(0, bw.get_data_length());
    }
}

template <typename T>
inline
void dynamic_column_batch<T>::dump_to(serializable_unit &su) {
    su.num_used = 2;
    dump_to(su.bufs[0], su.bufs[1]);
}

template <typename T>
inline
void dynamic_column_batch<T>::push_back_helper(const data_type* src, const uint32_t len) {
    _bw.ensure_space(len);
    _bw.meta[_size++] = metadata{_bw.occupied, len};
    memcpy(_bw.data + _bw.occupied, src, len * sizeof(data_type));
    _bw.occupied += len;
}

template <typename T>
inline
void dynamic_column_batch<T>::push_back_helper(const data_type* src, const uint32_t len, const data_type& val) {
    auto total_len = len + 1;
    _bw.ensure_space(total_len);
    _bw.meta[_size++] = metadata{_bw.occupied, total_len};
    memcpy(_bw.data + _bw.occupied, src, len * sizeof(data_type));
    _bw.occupied += total_len;
    _bw.data[_bw.occupied - 1] = val;
}

struct string {
    using data_type = char;
    data_type* ptr;
    uint32_t len;
    string(char* p, uint32_t l) : ptr(p), len(l) {}
};

struct path {
    using data_type = int64_t;
    data_type* ptr;
    uint32_t len;
    path(int64_t* p, uint32_t l) : ptr(p), len(l) {}
};

template <typename T>
struct column_batch : public fixed_column_batch<T> {
    explicit column_batch(const uint32_t capacity) : fixed_column_batch<T>(capacity) {}
    explicit column_batch(byte_tmpbuf_t&& tb)
        : fixed_column_batch<T>(std::move(tb)) {}
    column_batch(column_batch&& x) noexcept
        : fixed_column_batch<T>(std::move(x)) {}
    ~column_batch() = default;

    column_batch& operator=(const column_batch&) = delete;
    column_batch& operator=(column_batch&& other) noexcept {
        fixed_column_batch<T>::operator=(std::move(other));
        return *this;
    }

    static column_batch<T> load_from(serializable_unit&& su) {
        return column_batch<T>(std::move(su.bufs[0]));
    }

};

template <>
struct column_batch<string> : public dynamic_column_batch<string> {
    column_batch(const uint32_t capacity, const uint32_t reserved)
        : dynamic_column_batch<string>(capacity, reserved) {}
    explicit column_batch(byte_tmpbuf_t&& tb1, byte_tmpbuf_t&& tb2)
        : dynamic_column_batch<string>(std::move(tb1), std::move(tb2)) {}
    column_batch(column_batch&& x) noexcept
        : dynamic_column_batch<string>(std::move(x)) {}
    ~column_batch() = default;

    static column_batch<string> load_from(serializable_unit&& su) {
        return column_batch<string>(std::move(su.bufs[0]), std::move(su.bufs[1]));
    }

    inline void push_back(string& str) {
        push_back_helper(str.ptr, str.len);
    }

    inline void push_back(string&& str) {
        push_back_helper(str.ptr, str.len);
    }

    inline void push_back(std::string& str) {
        push_back_helper(str.data(), static_cast<uint32_t>(str.size()));
    }

    inline void push_back(std::string&& str) {
        push_back_helper(str.data(), static_cast<uint32_t>(str.size()));
    }

    inline void push_back(string& str, string::data_type& element) {
        push_back_helper(str.ptr, str.len, element);
    }

    inline void push_back(string&& str, string::data_type&& element) {
        push_back_helper(str.ptr, str.len, element);
    }
};

template <>
struct column_batch<path> : public dynamic_column_batch<path> {
    column_batch(const uint32_t capacity, const uint32_t reserved)
        : dynamic_column_batch<path>(capacity, reserved) {}
    explicit column_batch(byte_tmpbuf_t&& tb1, byte_tmpbuf_t&& tb2)
        : dynamic_column_batch<path>(std::move(tb1), std::move(tb2)) {}
    column_batch(column_batch&& x) noexcept
        : dynamic_column_batch<path>(std::move(x)) {}
    ~column_batch() = default;

    static column_batch<path> load_from(serializable_unit&& su) {
        return column_batch<path>(std::move(su.bufs[0]), std::move(su.bufs[1]));
    }

    inline void push_back(path& pth) {
        push_back_helper(pth.ptr, pth.len);
    }

    inline void push_back(path&& pth) {
        push_back_helper(pth.ptr, pth.len);
    }

    inline void push_back(path& pth, path::data_type& element) {
        push_back_helper(pth.ptr, pth.len, element);
    }

    inline void push_back(path&& pth, path::data_type&& element) {
        push_back_helper(pth.ptr, pth.len, element);
    }
};

} // namespace cb
} // namespace brane
