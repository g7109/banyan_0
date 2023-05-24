//
// Created by everlighting on 2021/4/13.
//

#include <brane/util/common-utils.hh>
#include <brane/query/column_batch.hh>

const uint32_t max_eos_step_num = 15;

struct PathEos {
    struct eos_step {
        uint32_t ds_num;
        uint32_t ds_id;
        eos_step() = default;
        eos_step(uint32_t downstream_num, uint32_t downstream_id)
                : ds_num(downstream_num), ds_id(downstream_id) {}
    };
    brane::cb::column_batch<eos_step> steps;

    PathEos() : steps(max_eos_step_num) {}
    explicit PathEos(brane::cb::column_batch<eos_step>&& other_steps) : steps(std::move(other_steps)) {}
    ~PathEos() = default;

    PathEos(const PathEos&) = delete;
    PathEos(PathEos&& other) noexcept : steps(std::move(other.steps)) {}

    void dump_to(brane::serializable_unit &su) {
        su.num_used = 1;
        steps.dump_to(su.bufs[0]);
    }

    static PathEos load_from(brane::serializable_unit &&su) {
        return PathEos{brane::cb::column_batch<eos_step>{std::move(su.bufs[0])} };
    }

    inline PathEos get_copy() {
        PathEos new_copy;
        for (unsigned i = 0; i < steps.size(); i++) {
            new_copy.steps.push_back(steps[i]);
        }
        return new_copy;
    }

    inline void back_to_prev_scope(uint32_t prev_scope_offset) {
        while (steps.size() > prev_scope_offset) {
            steps.erase_tail();
        }
    }

    inline void append_step(uint32_t ds_num, uint32_t ds_id) {
        steps.emplace_back(ds_num, ds_id);
    }
};

class PathEosCheckTree final {
    struct node {
        unsigned expect_num;
        unsigned current_num;
        std::vector<node> next{};
        node* parent;

        node(): expect_num(0), current_num(0), next(std::vector<node>{}), parent(nullptr) {}

        void set(unsigned expect_number, node* parent_ptr) {
            expect_num = expect_number;
            current_num = 0;
            next.clear();
            next.reserve(expect_num);
            for (unsigned i = 0; i < expect_num; i++) {
                next.emplace_back();
            }
            parent = parent_ptr;
        }
    };
    node _head;
public:
    PathEosCheckTree() = default;

    inline bool insert_and_merge(const PathEos& eos, unsigned scope_offset = 0) {
        auto step_num = eos.steps.size();
        auto* head = &_head;
        node* parent = nullptr;
        for (unsigned i = scope_offset; i < step_num; i++) {
            auto& step_i = eos.steps[i];
            if (head->next.empty()) {
                head->set(step_i.ds_num, parent);
            }
            parent = head;
            head = &(head->next[step_i.ds_id]);
        }
        auto backtrack_head = parent;
        while (backtrack_head != nullptr && ((++(backtrack_head->current_num) == backtrack_head->expect_num))) {
            backtrack_head->next.clear();
            backtrack_head = backtrack_head->parent;
        }
        return backtrack_head == nullptr;
    }
};
