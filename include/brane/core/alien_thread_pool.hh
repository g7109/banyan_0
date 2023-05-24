/**
 * DAIL in Alibaba Group
 *
 */
#pragma once

#include <queue>
#include <pthread.h>
#include <assert.h>
#include <iostream>

namespace brane {

class alien_task {
public:
    alien_task() = default;
    virtual ~alien_task() = default;
    virtual void run() = 0;
};

class task_queue {
    std::queue<alien_task *> _tasks;
    pthread_mutex_t _qmtx;
public:
    task_queue();
    ~task_queue();

    alien_task* next_task();
    void add_task(alien_task *nt);
};

class alien_thread_pool {
    task_queue _queue;
    const uint32_t _num_threads;
    std::vector<pthread_t> _threads;
public:
    alien_thread_pool(uint32_t num_threads);
    ~alien_thread_pool();

    // Allocate a thread pool and set them to work trying to get tasks
    void initialize_and_run();

    inline void add_task(alien_task *t) {
        _queue.add_task(t);
    }
};

} // namespace brane
