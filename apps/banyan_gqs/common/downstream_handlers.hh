#pragma once

#include <functional>
#include <vector>
#include <brane/actor/reference_base.hh>
#include "common/data_unit_type.hh"

template <typename OpRefT, typename InDataT>
class downstream_group_handler {
    std::vector<OpRefT*> _refs{};
    std::vector<InDataT> _data_buf{};
    unsigned _batch_sz;
    bool _have_send_data;

public:
    downstream_group_handler(brane::scope_builder& builder, unsigned ds_op_id, const unsigned batch_sz = batch_size)
            : _batch_sz(batch_sz), _have_send_data(false) {
        _refs.reserve(brane::global_shard_count());
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            builder.reset_shard(i);
            _refs.emplace_back(builder.new_ref<OpRefT>(ds_op_id));
        }
        _data_buf.reserve(brane::global_shard_count());
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            _data_buf.emplace_back(_batch_sz);
        }
    }
    ~downstream_group_handler() {
        for (auto ref : _refs) { delete ref; }
    }

    inline OpRefT* Get(unsigned i) { return _refs.at(i); }

    inline bool have_send_data() { return _have_send_data; }

    template< void(OpRefT::*Func)(InDataT&&) = &OpRefT::process >
    inline void process(unsigned dst_shard, InDataT&& data) {
        (_refs[dst_shard]->*Func)(std::move(data));
        if (!_have_send_data) { _have_send_data = true; }
    }

    inline void receive_eos() {
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            _refs[i]->receive_eos(Eos{_have_send_data});
        }
    }

    template < void(OpRefT::*Func)(InDataT&&) = &OpRefT::process, typename... Args>
    inline void emplace_data(unsigned dst_shard, Args&&... args) {
        _data_buf[dst_shard].emplace_back(std::forward<Args>(args)...);
        if (_data_buf[dst_shard].size() == _data_buf[dst_shard].capacity()) {
            process<Func>(dst_shard, std::move(_data_buf[dst_shard]));
            _data_buf[dst_shard].reset(_batch_sz);
        }
    }

    template< void(OpRefT::*Func)(InDataT&&) = &OpRefT::process >
    inline void flush() {
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            if (_data_buf[i].size() > 0) {
                process<Func>(i, std::move(_data_buf[i]));
                _data_buf[i].reset(_batch_sz);
            }
        }
    }
};

template <typename OpRefT, bool IfSync = false, typename... InDataT>
class downstream_handler {
    OpRefT* _ds_op;
    bool _have_send_data;

public:
    downstream_handler(brane::scope_builder& builder, unsigned ds_op_id) : _have_send_data(false) {
        _ds_op = builder.new_ref<OpRefT>(ds_op_id);
    }
    ~downstream_handler() { delete _ds_op; }

    inline OpRefT* Get() { return _ds_op; }
    inline void process(InDataT&&... data) {
        _ds_op->process(std::forward<InDataT>(data)...);
        if (!_have_send_data) { _have_send_data = true; }
    }
    inline void receive_eos(unsigned times = 1) {
        for (unsigned i = 0; i < times; i++) {
            _ds_op->receive_eos(Eos{IfSync | _have_send_data});
        }
    }
};

template <typename DsRefT, typename InDataT, typename SyncRefT>
class compressed_downstream_group_handler {
    std::vector<DsRefT*> _ds_refs{};
    std::vector<InDataT> _data_buf{};
    std::vector<bool> _ds_flags{};
    unsigned _cur_found_ds_num;
    unsigned _batch_sz;
    SyncRefT* _sync_ref;

public:
    compressed_downstream_group_handler(brane::scope_builder& ds_builder, unsigned ds_op_id,
                                        const unsigned batch_sz = batch_size)
            : _cur_found_ds_num(0), _batch_sz(batch_sz), _sync_ref(nullptr) {
        _ds_refs.reserve(brane::global_shard_count());
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            ds_builder.reset_shard(i);
            _ds_refs.emplace_back(ds_builder.new_ref<DsRefT>(ds_op_id));
        }
        _data_buf.reserve(brane::global_shard_count());
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            _data_buf.emplace_back(_batch_sz);
        }
        _ds_flags.resize(brane::global_shard_count(), false);
    }
    ~compressed_downstream_group_handler() {
        for (auto ref : _ds_refs) { delete ref; }
        delete _sync_ref;
    }

    inline void set_sync_op(brane::scope_builder& sync_builder, unsigned sync_op_id) {
        if (_sync_ref) { delete _sync_ref; }
        _sync_ref = sync_builder.new_ref<SyncRefT>(sync_op_id);
    }

    inline void receive_eos_by(PathEos&& cur_eos) {
        if (_cur_found_ds_num == 0) {
            _sync_ref->receive_eos(std::move(cur_eos));
            return;
        }
        unsigned cur_ds_id = 0;
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            if (_ds_flags[i]) {
                auto ds_eos = cur_eos.get_copy();
                ds_eos.append_step(_cur_found_ds_num, cur_ds_id++);
                _ds_refs[i]->receive_eos(std::move(ds_eos));
                _ds_flags[i] = false;
            }
        }
        _cur_found_ds_num = 0;
    }

    template< void(DsRefT::*Func)(InDataT&&) = &DsRefT::process >
    inline void process(unsigned dst_shard, InDataT&& data) {
        if (!_ds_flags[dst_shard]) {
            _ds_flags[dst_shard] = true;
            _cur_found_ds_num++;
        }
        (_ds_refs[dst_shard]->*Func)(std::move(data));
    }

    template < void(DsRefT::*Func)(InDataT&&) = &DsRefT::process, typename... Args>
    inline void emplace_data(unsigned dst_shard, Args&&... args) {
        _data_buf[dst_shard].emplace_back(std::forward<Args>(args)...);
        if (_data_buf[dst_shard].size() == _data_buf[dst_shard].capacity()) {
            process<Func>(dst_shard, std::move(_data_buf[dst_shard]));
            _data_buf[dst_shard].reset(_batch_sz);
        }
    }

    template< void(DsRefT::*Func)(InDataT&&) = &DsRefT::process>
    inline void flush() {
        for (unsigned i = 0; i < brane::global_shard_count(); i++) {
            if (_data_buf[i].size() > 0) {
                process<Func>(i, std::move(_data_buf[i]));
                _data_buf[i].reset(_batch_sz);
            }
        }
    }
};

template <typename DsRefT1, typename InDataT1, typename DsRefT2, typename InDataT2, typename SyncRefT>
class compressed_bi_downstream_handler {
    DsRefT1* _ds_ref_1;
    bool _ds_1_flag;
    DsRefT2* _ds_ref_2;
    bool _ds_2_flag;
    SyncRefT* _sync_ref;

public:
    compressed_bi_downstream_handler()
        : _ds_ref_1(nullptr), _ds_1_flag(false), _ds_ref_2(nullptr), _ds_2_flag(false), _sync_ref(nullptr) {}
    ~compressed_bi_downstream_handler() {
        delete _ds_ref_1;
        delete _ds_ref_2;
        delete _sync_ref;
    }

    inline void set_ds_ref_1(brane::scope_builder& ds_1_builder, unsigned ds_1_op_id) {
        if (_ds_ref_1) { delete _ds_ref_1; }
        _ds_ref_1 = ds_1_builder.new_ref<DsRefT1>(ds_1_op_id);
    }
    inline void set_ds_ref_2(brane::scope_builder& ds_2_builder, unsigned ds_2_op_id) {
        if (_ds_ref_2) { delete _ds_ref_2; }
        _ds_ref_2 = ds_2_builder.new_ref<DsRefT2>(ds_2_op_id);
    }
    inline void set_sync_op(brane::scope_builder& sync_builder, unsigned sync_op_id) {
        if (_sync_ref) { delete _sync_ref; }
        _sync_ref = sync_builder.new_ref<SyncRefT>(sync_op_id);
    }

    inline DsRefT1* get_ds1() { return _ds_ref_1; }
    inline DsRefT2* get_ds2() { return _ds_ref_2; }

    template< void(DsRefT1::*Func)(InDataT1&&) = &DsRefT1::process >
    inline void ds_1_process(InDataT1&& data) {
        _ds_1_flag = true;
        (_ds_ref_1->*Func)(std::move(data));
    }

    template< void(DsRefT2::*Func)(InDataT2&&) = &DsRefT2::process >
    inline void ds_2_process(InDataT2&& data) {
        _ds_2_flag = true;
        (_ds_ref_2->*Func)(std::move(data));
    }

    inline void receive_eos_by(PathEos&& cur_eos) {
        if (!_ds_1_flag && !_ds_2_flag) {
            _sync_ref->receive_eos(std::move(cur_eos));
        } else if (_ds_1_flag && !_ds_2_flag) {
            _ds_ref_1->receive_eos(std::move(cur_eos));
            _ds_1_flag = false;
        } else if (!_ds_1_flag && _ds_2_flag) {
            _ds_ref_2->receive_eos(std::move(cur_eos));
            _ds_2_flag = false;
        } else {
            auto ds_1_eos = cur_eos.get_copy();
            ds_1_eos.append_step(2, 0);
            _ds_ref_1->receive_eos(std::move(ds_1_eos));
            _ds_1_flag = false;

            auto ds_2_eos = cur_eos.get_copy();
            ds_2_eos.append_step(2, 1);
            _ds_ref_2->receive_eos(std::move(ds_2_eos));
            _ds_2_flag = false;
        }
    }

};
