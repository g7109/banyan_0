#define BOOST_TEST_MODULE column_batch

#include <boost/test/included/unit_test.hpp>
#include <seastar/haf/query/column_batch.hh>
#include <seastar/haf/query/cb_table.hh>
#include <seastar/haf/actor_message.hh>

using namespace seastar;

BOOST_AUTO_TEST_SUITE(test_column_batch)

BOOST_AUTO_TEST_CASE(test_cb_dump) {
    const int batch_size = 10;
    serializable_unit su;
    /// fixed column batch
    cb::column_batch<int64_t> fcb{batch_size};
    for (int i = 0; i < 6; ++i) {
        fcb.push_back(5102 + i);
    }
    fcb.dump_to(su.bufs[0]);
    cb::column_batch<int64_t> r_fcb{std::move(su.bufs[0])};
    BOOST_REQUIRE_EQUAL(r_fcb.size(), 6);
    for (size_t i = 0; i < r_fcb.size(); ++i) {
        BOOST_REQUIRE_EQUAL(r_fcb[i], 5102 + i);
    }
    /// dynamic column batch
    cb::column_batch<cb::path> dcb{batch_size, 10};
    std::vector<cb::path::data_type> path_content{0, 0, 0};
    cb::path::data_type counter = 0;
    for (int i = 0; i < 6; ++i) {
        path_content[0] = ++counter;
        path_content[1] = ++counter;
        path_content[2] = ++counter;
        dcb.push_back(cb::path{
            path_content.data(),
            static_cast<uint32_t>(path_content.size())
        });
    }

//    for (size_t i = 0; i < dcb.size(); ++i) {
//        BOOST_REQUIRE_EQUAL(dcb[i].len, path_content.size());
//        for (uint32_t j = 0; j < dcb[i].len; ++j) {
//            std::cout << dcb[i].ptr[j] << ", ";
//        }
//        std::cout << std::endl;
//    }

    dcb.dump_to(su.bufs[1], su.bufs[2]);
    cb::column_batch<cb::path> r_dcb{std::move(su.bufs[1]), std::move(su.bufs[2])};

    counter = 0;
    BOOST_REQUIRE_EQUAL(r_dcb.size(), 6);
    for (size_t i = 0; i < r_dcb.size(); ++i) {
        BOOST_REQUIRE_EQUAL(r_dcb[i].len, path_content.size());
        for (uint32_t j = 0; j < r_dcb[i].len; ++j) {
            BOOST_REQUIRE_EQUAL(++counter, r_dcb[i].ptr[j]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_cb_ref_count) {
    // fixed column batch.
    cb::column_batch<int32_t> ib_0{10};
    for (int i = 0; i < 6; ++i) {
        ib_0.push_back(i + 10);
    }
    // share a fcb
    const auto ib_1 = ib_0.share();
    BOOST_REQUIRE_EQUAL(ib_1.size(), ib_0.size());
    BOOST_REQUIRE_EQUAL(ib_1.capacity(), ib_0.capacity());
    for (size_t i = 0; i < ib_1.size(); ++i) {
        BOOST_REQUIRE_EQUAL(ib_0[i], ib_1[i]);
    }

    // dynamic column batch.
    cb::column_batch<cb::path> pb_0{10, 20};
    std::vector<cb::path::data_type> path_content{0, 0, 0};
    cb::path::data_type counter = 0;
    for (int i = 0; i < 6; ++i) {
        path_content[0] = ++counter;
        path_content[1] = ++counter;
        path_content[2] = ++counter;
        pb_0.push_back(cb::path{
            path_content.data(),
            static_cast<uint32_t>(path_content.size())
        });
    }
    // share a dcb
    auto pb_1 = pb_0.share();
    BOOST_REQUIRE_EQUAL(pb_1.size(), pb_0.size());
    BOOST_REQUIRE_EQUAL(pb_1.capacity(), pb_0.capacity());
    for (size_t i = 0; i < pb_1.size(); ++i) {
        BOOST_REQUIRE_EQUAL(pb_1[i].len, pb_0[i].len);
        for (uint32_t j = 0; j < pb_1[i].len; ++j) {
            BOOST_REQUIRE_EQUAL(pb_1[i].ptr[j], pb_0[i].ptr[j]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_cb_as_actor_data) {
    // fixed column batch.
    cb::column_batch<int32_t> fcb{10};
    for (int i = 0; i < 6; ++i) {
        fcb.push_back(i + 10);
    }

    // dynamic column batch
    cb::column_batch<cb::path> dcb{10, 10};
    std::vector<cb::path::data_type> path_content{0, 0, 0};
    cb::path::data_type counter = 0;
    for (int i = 0; i < 6; ++i) {
        path_content[0] = ++counter;
        path_content[1] = ++counter;
        path_content[2] = ++counter;
        dcb.push_back(cb::path{
            path_content.data(),
            static_cast<uint32_t>(path_content.size())
        });
    }

    address addr{};
    auto data = cb::make_table(std::move(fcb), std::move(dcb));
    auto *msg = make_actor_message(0, message_type::USER, addr, std::move(data));
    // check first column
    auto &col_1 = std::get<0>(msg->data.cols);
    BOOST_REQUIRE_EQUAL(col_1.size(), 6);
    for (size_t i = 0; i < col_1.size(); ++i) {
        BOOST_REQUIRE_EQUAL(col_1[i], 10 + i);
    }

    // check second column
    auto &col_2 = std::get<1>(msg->data.cols);
    counter = 0;
    BOOST_REQUIRE_EQUAL(col_2.size(), 6);
    for (size_t i = 0; i < col_2.size(); ++i) {
        BOOST_REQUIRE_EQUAL(col_2[i].len, path_content.size());
        for (uint32_t j = 0; j < col_2[i].len; ++j) {
            BOOST_REQUIRE_EQUAL(++counter, col_2[i].ptr[j]);
        }
    }

    serializable_unit su;
    msg->data.dump_to(su);

    // recover fcb
    cb::column_batch<int32_t> recv_fcb{std::move(su.bufs[0])};
    // recover dcb
    cb::column_batch<cb::path> recv_dcb{std::move(su.bufs[1]), std::move(su.bufs[2])};

    // check first column
    BOOST_REQUIRE_EQUAL(recv_fcb.size(), 6);
    for (size_t i = 0; i < recv_fcb.size(); ++i) {
        BOOST_REQUIRE_EQUAL(recv_fcb[i], 10 + i);
    }

    // check second column
    counter = 0;
    BOOST_REQUIRE_EQUAL(recv_dcb.size(), 6);
    for (size_t i = 0; i < recv_dcb.size(); ++i) {
        BOOST_REQUIRE_EQUAL(recv_dcb[i].len, path_content.size());
        for (uint32_t j = 0; j < recv_dcb[i].len; ++j) {
            BOOST_REQUIRE_EQUAL(++counter, recv_dcb[i].ptr[j]);
        }
    }

    delete msg;
}

BOOST_AUTO_TEST_SUITE_END()




