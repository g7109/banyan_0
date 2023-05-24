#pragma once

#include <utility>

namespace brane {

template <typename T>
struct entry {
    union optional {
        optional() {}
        ~optional() {}
        template <typename... As>
        void init(As&&... args) {
            new (&t) T{std::forward<As>(args)...};
        }
        T t;
    } o;
    entry() : _empty(true) {}
    ~entry() = default;

    template <typename... Args> void init(Args&&... args);
    void finalize();

    inline bool empty() const {
        return _empty;
    }
private:
    bool _empty;
};

template <typename T>
struct entries_deleter {
    unsigned count;
    entries_deleter(unsigned c = 0) : count(c) {}
    void operator()(T* es) const;
};

template <typename T>
template <typename... Args>
inline void entry<T>::init(Args&&... args) {
    if (_empty) {
        _empty = false;
        o.init(std::forward<Args>(args)...);
    }
}

template <typename T>
inline void entry<T>::finalize() {
    if (!_empty) {
        _empty = true;
        o.t.~T();
    }
}

template <typename T>
inline
void entries_deleter<T>::operator()(T* es) const {
    for (unsigned i = 0; i < count; i++) {
        es[i].~T();
    }
    ::operator delete[](es);
}

} // namespace brane
