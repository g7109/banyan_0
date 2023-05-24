#pragma once

#include <brane/actor/actor_message.hh>

namespace brane {

struct simple_cstring {
    char* c_str;
    size_t len;
    simple_cstring(const char* data, size_t len) : c_str(new char[len]), len(len) {
        memcpy(c_str, data, len);
    }
    simple_cstring(simple_cstring&& x) noexcept : c_str(x.c_str), len(x.len) {
        x.c_str = nullptr;
        x.len = 0;
    }
    simple_cstring(const simple_cstring& x) : simple_cstring(x.c_str, x.len) {}
    simple_cstring& operator=(simple_cstring&& x) noexcept {
        if (this != &x) {
            c_str = x.c_str;
            len = x.len;
            x.c_str = nullptr;
            x.len = 0;
        }
        return *this;
    }
    ~simple_cstring() { delete[] c_str; }

    void dump_to(serializable_unit &bufs) {}
};

template <typename T, size_t N>
struct simple_array {
    T arr[N];
    simple_array() = default;
    simple_array(simple_array&& x) = default;
    simple_array(const simple_array& x) = default;

    void dump_to(serializable_unit &bufs) {}
};

} // namespace brane

struct MyInteger {
	int val;
	MyInteger(int v) : val(v) {}

    void dump_to(brane::serializable_unit &su) {
       su.num_used = 1;
	   su.bufs[0] = seastar::temporary_buffer<char>(sizeof(val));
	   memcpy(su.bufs[0].get_write(), &val, sizeof(val));
	}

	static MyInteger load_from(brane::serializable_unit&& su) {
		auto new_val = *reinterpret_cast<const int*>(su.bufs[0].get());
		return MyInteger{new_val};
	}
};

