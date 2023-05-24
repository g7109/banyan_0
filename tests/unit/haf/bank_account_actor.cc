#include "bank_account_actor.hh"
#include <seastar/core/print.hh>

seastar::future<bool> bank_account::withdraw(MyInteger &&amount) {
    balance -= amount.val;
    fmt::print("Withdraw: current balance is {} on shard {}\n",
        balance, seastar::engine().cpu_id());
    return seastar::make_ready_future<bool>();
}

seastar::future<bool> bank_account::deposit(MyInteger &&amount) {
    balance += amount.val;
    fmt::print("Deposit: current balance is {} on shard {}\n",
        balance, seastar::engine().cpu_id());
    return seastar::make_ready_future<bool>();
}

seastar::future<bool> bank_account::display() {
    fmt::print("Display: current balance is {}\n", balance);
    return seastar::make_ready_future<bool>();
}