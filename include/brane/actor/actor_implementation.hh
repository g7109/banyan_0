/**
 * DAIL in Alibaba Group
 *
 */

#pragma once

#include <brane/actor/actor_core.hh>
#include <brane/actor/actor_factory.hh>
#include <brane/core/root_actor_group.hh>
#include <brane/util/radix_tree_index.hh>

using namespace std::chrono_literals;

namespace brane {

const uint16_t GActorGroupTypeId = UINT16_MAX;
const uint16_t GBFSActorGroupTypeId = UINT16_MAX - 1;
const uint16_t GDFSActorGroupTypeId = UINT16_MAX - 2;

struct fifo_cmp {
	bool operator() (const std::unique_ptr<seastar::task>& a, const std::unique_ptr<seastar::task>& b) {
		return a->get_task_id() > b->get_task_id();
	}
	static uint16_t get_type_id() {return GActorGroupTypeId;}
};

struct small_better {
    bool operator() (const std::unique_ptr<seastar::task>& a, const std::unique_ptr<seastar::task>& b) {
        return a->get_priority() > b->get_priority();
    }
    static uint16_t get_type_id() {return GBFSActorGroupTypeId;}
};

struct large_better {
    bool operator() (const std::unique_ptr<seastar::task>& a, const std::unique_ptr<seastar::task>& b) {
        return a->get_priority() < b->get_priority();
    }
	static uint16_t get_type_id() {return GDFSActorGroupTypeId;}
};

typedef void (*cancel_func)(void*, void*);



template<class ComparatorType = fifo_cmp>
class actor_group : public actor_base {
	uint32_t _num_actors_managed = 0;
	radix_tree_index<actor_base *> _actor_tree;
	seastar::semaphore _actor_activation_sem{GMaxActiveChildNum};
	/// temporarily used in gqs
	uint32_t _cancel_src_sid = UINT32_MAX;
	uint32_t _cancel_pr_id;
	ComparatorType _comparator;
private:
	void process_message(actor_message *);
	void schedule_task();
	void stop_all_children(cancel_func func);
protected:
	~actor_group() override = default;
public:
	explicit actor_group(actor_base *exec_ctx, const byte_t *addr) : actor_base(exec_ctx, addr) {}
	void run_and_dispose() noexcept override;
	void stop(bool force) override;
	void stop_child_actor(byte_t *child_addr, bool force) override;
	void notify_child_stopped() override;
	actor_base* get_actor_local(actor_message::header &hdr) override;
	static uint16_t get_type_id() {return ComparatorType::get_type_id();}
};

/***
 * Each stateless operation inherits this class.
 *
 * Requirement of child class:
 *      the do_work operation must return a stop_reaction type future.
 */

// TODO[DAIL]: adjust the default time slice of each run
const uint32_t GDefaultMaxConcurrency = UINT32_MAX;

class stateless_actor : public actor_base {
	uint32_t _max_concurrency = GDefaultMaxConcurrency;
	uint32_t _cur_concurrency = 0;
private:
	void clean_task_queue();
protected:
	virtual void initialize() override {}
    virtual void finalize() override {}
	~stateless_actor() override = default;
public:
	stateless_actor(actor_base *exec_ctx, const byte_t *addr, uint32_t max_cur = GDefaultMaxConcurrency)
		: actor_base(exec_ctx, addr), _max_concurrency(max_cur) {}

	void run_and_dispose() noexcept override;
	void stop(bool force) override;
	virtual seastar::future<stop_reaction> do_work(actor_message* msg) = 0;
	actor_base* get_execution_context() { return this; }
};

/***
 * A stateful_actor runs the operation that manipulates at least one centralized "states".
 *      Each operation implementation inherits this class.
 *      The "states" of the operator are stored in child class.
 *
 * Requirement of child class:
 *      the do_work operation must return a stop_reaction type future to show stopping condition.
 *
 */
class stateful_actor : public actor_base {
	bool busy = false;
private:
	void clean_task_queue();
protected:
	virtual void initialize() override {}
    virtual void finalize() override {}
    ~stateful_actor() override = default;
public:
	stateful_actor(actor_base *exec_ctx, const byte_t *addr) : actor_base(exec_ctx, addr) {}

	void run_and_dispose() noexcept override;
	void stop(bool force) override;
	virtual seastar::future<stop_reaction> do_work(actor_message* msg) = 0;
	actor_base* get_execution_context() { return this; }
};

} // namespace brane
