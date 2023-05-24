#pragma once

#include <brane/query/column_batch.hh>

namespace brane {
namespace cb {

/**
 * Table is a wrapper to orchestrate column batches.
 * 1. Table is not shareable
 * 2. Column batch is the basic unit to share.
 */
template <typename... Columns>
struct table {
    std::tuple<Columns...> cols;
    table(Columns&&... cols) : cols(std::forward<Columns>(cols)...) {}
    ~table() = default;

    table(const table&) = delete;
    table(table&& x) : cols(std::move(x.cols)) {}

    void dump_to(serializable_unit &su) {
        foreach_dump_to(su);
    }

private:
    template<size_t I = 0>
    inline std::enable_if_t<I == sizeof...(Columns), void>
    foreach_dump_to(serializable_unit &su, size_t pos = I) {}

    template<size_t I = 0>
    inline std::enable_if_t<I < sizeof...(Columns), void>
    foreach_dump_to(serializable_unit &su, size_t pos = I) {
        if constexpr (std::remove_reference_t<decltype(std::get<I>(cols))>::fixed) {
            std::get<I>(cols).dump_to(su.bufs[pos]);
            foreach_dump_to<I + 1>(su, pos + 1);
        } else {
            std::get<I>(cols).dump_to(su.bufs[pos], su.bufs[pos + 1]);
            foreach_dump_to<I + 1>(su, pos + 2);
        }
    }
};

template <typename First, typename... Rest>
struct all_is_rvalue_reference {
    static const bool value = std::is_rvalue_reference_v<First&&>
        && all_is_rvalue_reference<Rest...>::value;
};

template <typename First>
struct all_is_rvalue_reference<First> {
    static const bool value = std::is_rvalue_reference_v<First&&>;
};


template <typename... Columns, typename =
    std::enable_if_t<all_is_rvalue_reference<Columns...>::value>>
inline
table<Columns...> make_table(Columns&&... cols) {
    return table<Columns...>{std::forward<Columns>(cols)...};
}

} // namespace cb
} // namespace brane
