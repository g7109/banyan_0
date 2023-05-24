//
// Created by everlighting on 2020/8/4.
//
#pragma once

#include <vector>
#include <unordered_set>
#include "berkeley_db/storage/bdb_graph_store.hh"
#include "berkeley_db/storage/bdb_graph_utils.hh"

template <bool ExpectEqual = true>
struct StringPropCheck {
    std::string checker;

    StringPropCheck() = default;
    explicit StringPropCheck(const std::string& prop_value)
        : checker(prop_value) {}
    explicit StringPropCheck(std::string&& prop_value)
        : checker(std::move(prop_value)) {}
    explicit StringPropCheck(const char* prop_value)
        : checker(prop_value) {}
    ~StringPropCheck() = default;

    static StringPropCheck get_from(std::string&& prop_value) {
        return StringPropCheck(std::move(prop_value));
    }
    static StringPropCheck get_from(const char* prop_value) {
        return StringPropCheck(prop_value);
    }

    bool operator()(char *value, int &position) const {
        return ExpectEqual == storage::berkeleydb_helper::compareString(value, position, checker);
    }
};

template <bool Within = true>
struct StringSetPropCheck {
    std::unordered_set<std::string> checkers;

    StringSetPropCheck() = default;

    template <typename... A>
    explicit StringSetPropCheck(A... checker_strings) {
        auto checker_list = std::forward_as_tuple(checker_strings...);
        foreach_init_checkers(checker_list);
    }
    ~StringSetPropCheck() = default;

    template <typename... A>
    static StringSetPropCheck get_from(A... checker_strings) {
        return StringSetPropCheck(checker_strings...);
    }

    bool operator()(char *value, int &position) const {
        auto prop = storage::berkeleydb_helper::readString(value, position);
        return Within == (checkers.find(prop) != checkers.end());
    }

    template<size_t I = 0, typename... A>
    inline std::enable_if_t<I == sizeof...(A), void>
    foreach_init_checkers(const std::tuple<A...>& checker_strings, size_t layer = I) {}

    template<size_t I = 0, typename... A>
    inline std::enable_if_t<I < sizeof...(A), void>
    foreach_init_checkers(const std::tuple<A...>& checker_strings, size_t layer = I) {
        checkers.emplace(std::get<I>(checker_strings));
        foreach_init_checkers<I + 1>(checker_strings, layer + 1);
    }
};
using WithinStringSetPropCheck = StringSetPropCheck<true>;
using WithoutStringSetPropCheck = StringSetPropCheck<false>;

template <typename Comparator>
struct LongPropCheck {
    long checker;
    Comparator compare;

    LongPropCheck() : checker(0) {}
    explicit LongPropCheck(long prop_value)
            : checker(prop_value) {}
    ~LongPropCheck() = default;

    static LongPropCheck get_from(long prop_value) {
        return LongPropCheck(prop_value);
    }

    bool operator()(char *value, int &position) const {
        if(!value) return false;
        auto prop_value = storage::berkeleydb_helper::readLong(value, position);
        return compare(prop_value, checker);
    }
};
template struct LongPropCheck<std::less<>>;
template struct LongPropCheck<std::less_equal<>>;
template struct LongPropCheck<std::greater<>>;
template struct LongPropCheck<std::greater_equal<>>;
template struct LongPropCheck<std::equal_to<>>;
template struct LongPropCheck<std::not_equal_to<>>;

template <bool Inside = true>
struct LongPropIntervalCheck {
    long lower;
    long upper;

    LongPropIntervalCheck() : lower(0), upper(0) {}
    LongPropIntervalCheck(long lower_value, long upper_value)
        : lower(lower_value), upper(upper_value) {}
    ~LongPropIntervalCheck() = default;

    static LongPropIntervalCheck get_from(long lower_value, long upper_value) {
        return LongPropIntervalCheck(lower_value, upper_value);
    }

    bool operator()(char *value, int &position) const {
        auto prop_value = storage::berkeleydb_helper::readLong(value, position);
        return (prop_value >= lower && prop_value < upper) == Inside;
    }
};
using LongPropInsideIntervalCheck = LongPropIntervalCheck<true>;
using LongPropOutsideIntervalCheck = LongPropIntervalCheck<false>;
