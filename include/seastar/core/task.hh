/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <memory>
#include <seastar/core/scheduling.hh>
#include <chrono>
#include <climits>
#include <seastar/core/print.hh>

namespace brane {

class actor_base;

} // namespace brane

namespace seastar {

const uint32_t _default_priority = 1;
const uint32_t _default_task_id = UINT32_MAX;

class task {
    scheduling_group _sg;
protected:
    // [DAIL]
    brane::actor_base* _exec_ctx;
    uint32_t _priority = _default_priority;
    uint32_t _id = _default_task_id;
public:
    // [DAIL] Modify constructors to adapt with execution context.
    explicit task(scheduling_group sg = current_scheduling_group());
    explicit task(brane::actor_base* exec_ctx);
    virtual ~task() noexcept {}
    virtual void run_and_dispose() noexcept = 0;
    scheduling_group group() const { return _sg; }
    // [DAIL]
    brane::actor_base* get_execution_context() { return _exec_ctx; }
    virtual void cancel() {}
    uint32_t get_priority() const { return _priority; }
    void set_priority(uint32_t p) { _priority = p; }
    uint32_t get_task_id() const { return _id; }
    void set_task_id(uint32_t id) { _id = id; }
};

void schedule(std::unique_ptr<task> t);
void schedule_urgent(std::unique_ptr<task> t);

template <typename Func>
class lambda_task final : public task {
    Func _func;
public:
    lambda_task(scheduling_group sg, const Func& func) : task(sg), _func(func) {}
    lambda_task(scheduling_group sg, Func&& func) : task(sg), _func(std::move(func)) {}
    virtual void run_and_dispose() noexcept override {
        _func();
        delete this;
    }
};

template <typename Func>
inline
std::unique_ptr<task>
make_task(Func&& func) {
    return std::make_unique<lambda_task<Func>>(current_scheduling_group(), std::forward<Func>(func));
}

template <typename Func>
inline
std::unique_ptr<task>
make_task(scheduling_group sg, Func&& func) {
    return std::make_unique<lambda_task<Func>>(sg, std::forward<Func>(func));
}

}
