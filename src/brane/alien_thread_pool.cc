/**
 * DAIL in Alibaba Group
 *
 */

#include <brane/core/alien_thread_pool.hh>

namespace brane {

task_queue::task_queue() {
    pthread_mutex_init(&_qmtx, 0);
}

task_queue::~task_queue() {
    #ifdef SEASTAR_DEBUG
        assert (_tasks.empty() == false);
    #endif
    pthread_mutex_destroy(&_qmtx);
}

alien_task* task_queue::next_task() {
    pthread_mutex_lock(&_qmtx);
    if (_tasks.empty() == true) {
        pthread_mutex_unlock(&_qmtx);
        return nullptr;
    }
    auto next = _tasks.front();
    _tasks.pop();
    pthread_mutex_unlock(&_qmtx);
    return next;
}

void task_queue::add_task(alien_task *nt) {
    pthread_mutex_lock(&_qmtx);
    _tasks.push(nt);
    pthread_mutex_unlock(&_qmtx);
}

void* thread_routine(void *arg) {
    task_queue *queue = (task_queue *) arg;
    alien_task *task = queue->next_task();
    while (task != nullptr) {
        task->run();
        delete task;
        task = queue->next_task();
    }
    pthread_exit(NULL);
}

alien_thread_pool::alien_thread_pool(uint32_t num_threads)
    : _num_threads(num_threads), _threads(num_threads) {}

alien_thread_pool::~alien_thread_pool() {
    for (uint32_t i = 0; i < _num_threads; ++i) {
        pthread_join(_threads[i], NULL);
    }
}

void alien_thread_pool::initialize_and_run() {
    #ifdef SEASTAR_DEBUG
        std::cout << "Creating an alien thread pool with " << _num_threads << " threads ..." << "\n";
    #endif
    for (uint32_t i = 0; i < _num_threads; ++i) {
        auto rc = pthread_create(&_threads[i], NULL, thread_routine, (void *)(&_queue));
        if (rc) {
            std::cout << "Error:unable to create alien thread," << rc << std::endl;
            exit(-1);
        }
    }
}

} // namespace brane
