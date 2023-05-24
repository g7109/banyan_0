/**
 * DAIL in Alibaba Group
 *
 */

#include <iostream>
#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <thread>
#include <pthread.h>
#include <brane/core/alien_thread_pool.hh>
#include <seastar/core/sleep.hh>

using namespace seastar;
using namespace std::chrono_literals;

class my_task : public brane::alien_task {
public:
    unsigned shard_id;
    my_task(unsigned id) : brane::alien_task(), shard_id(id) {};

    void run() override {
        auto id = shard_id;
        alien::submit_to(id, [id] {
            auto this_id = std::this_thread::get_id();
            std::cout << "Seastar thread " << this_id << " executed task "<< id << "\n";
            return seastar::make_ready_future<int>(id);
        });
    }
};

void install_alien_threads() {
    brane::alien_thread_pool *thread_pool = new brane::alien_thread_pool(4);
    thread_pool->add_task(new my_task(0));
    thread_pool->add_task(new my_task(1));
    thread_pool->add_task(new my_task(2));
    thread_pool->add_task(new my_task(3));
    thread_pool->initialize_and_run();
    delete thread_pool;
}

int main(int argc, char** argv)
{
    seastar::app_template app;
    app.run(argc, argv, [&] {
        return seastar::now()
        .then([] {
                install_alien_threads();
                return make_ready_future();
        }).then([] {
            return sleep(1s);
        });
    });
    std::cout << "Test completed ..." << "\n";
}