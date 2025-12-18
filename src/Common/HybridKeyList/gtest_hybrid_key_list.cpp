#include <IO/WriteHelpers.h>
#include <Common/HybridKeyList/HybridKeyList.h>

#include <gtest/gtest.h>

#include <ranges>

namespace
{


void forEach(
    const DB::HybridKeyList<std::string> & key_list, const std::vector<std::string> & keys, const std::vector<int64_t> & timestamps)
{
    std::vector<std::string> all_keys;
    std::vector<int64_t> all_key_timestamps;
    auto errcode = key_list.forEach([&](const std::string & k, int64_t ts) {
        all_keys.push_back(k);
        all_key_timestamps.push_back(ts);
    });

    ASSERT_EQ(errcode, DB::ErrorCodes::OK);
    ASSERT_EQ(all_keys.size(), keys.size());
    ASSERT_EQ(all_key_timestamps.size(), timestamps.size());

    for (auto [lhs, rhs] : std::views::zip(all_keys, keys))
        ASSERT_EQ(lhs, rhs);

    if (!all_key_timestamps.empty())
    {
        for (size_t i = 0; i < all_key_timestamps.size() - 1; ++i)
            ASSERT_LE(all_key_timestamps[i], all_key_timestamps[i + 1]);
    }

    for (auto [lhs, rhs] : std::views::zip(all_key_timestamps, timestamps))
        ASSERT_EQ(lhs, rhs);
}

void forEachPersistent(
    const DB::HybridKeyList<std::string> & key_list, const std::vector<std::string> & keys, const std::vector<int64_t> & timestamps)
{
    std::vector<std::string> all_keys;
    std::vector<int64_t> all_key_timestamps;
    auto errcode = key_list.forEachPersistent([&](const std::string & k, int64_t ts) {
        all_keys.push_back(k);
        all_key_timestamps.push_back(ts);
    });

    ASSERT_EQ(errcode, DB::ErrorCodes::OK);

    for (auto [lhs, rhs] : std::views::zip(all_keys, keys))
        ASSERT_EQ(lhs, rhs);

    if (!all_key_timestamps.empty())
    {
        for (size_t i = 0; i < all_key_timestamps.size() - 1; ++i)
            ASSERT_LE(all_key_timestamps[i], all_key_timestamps[i + 1]);
    }

    for (auto [lhs, rhs] : std::views::zip(all_key_timestamps, timestamps))
        ASSERT_EQ(lhs, rhs);
}

}

TEST(HybridKeyList, Ops)
{
    DB::HybridConfig config{
        .spill_dir_path = "/tmp/hybrid_key_list_ops",
        .kv_options = "",
        .max_hot_key_count = 3,
        .cf_handle_id = "",
        .rocks_cf_handler_getter = {},
    };

    auto key_serializer = [](const std::string & k, DB::WriteBuffer & wb) {
        DB::writeStringBinary(k, wb);
        return DB::ErrorCodes::OK;
    };

    auto key_deserializer = [](std::string & k, DB::ReadBuffer & rb) {
        DB::readStringBinary(k, rb);
        return DB::ErrorCodes::OK;
    };

    DB::HybridKeyList<std::string> key_list(
        std::move(config), std::move(key_serializer), std::move(key_deserializer), getLogger("HybridKeyList"));

    /// emplaceKey
    {
        auto [ts5, err5] = key_list.emplaceKey("key5");
        ASSERT_EQ(err5, DB::ErrorCodes::OK);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        auto [ts4, err4] = key_list.emplaceKey("key4");
        ASSERT_EQ(err4, DB::ErrorCodes::OK);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        auto [ts3, err3] = key_list.emplaceKey("key3");
        ASSERT_EQ(err3, DB::ErrorCodes::OK);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        auto [ts2, err2] = key_list.emplaceKey("key2");
        ASSERT_EQ(err2, DB::ErrorCodes::OK);
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        auto [ts1, err1] = key_list.emplaceKey("key1");
        ASSERT_EQ(err1, DB::ErrorCodes::OK);

        forEach(key_list, std::vector<std::string>{"key5", "key4", "key3", "key2", "key1"}, std::vector{ts5, ts4, ts3, ts2, ts1});
        forEachPersistent(key_list, std::vector<std::string>{"key2", "key1"}, std::vector{ts2, ts1});

        std::this_thread::sleep_for(std::chrono::milliseconds{20});

        absl::flat_hash_set<std::string> handled_expires{"key4"};
        auto remove_result = key_list.removeTimedOutKeys(10, handled_expires);
        ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
        ASSERT_TRUE(key_list.empty());
        ASSERT_EQ(remove_result.first.size(), 4);
        ASSERT_EQ(remove_result.first[0], "key5");
        ASSERT_EQ(remove_result.first[1], "key3");
        ASSERT_EQ(remove_result.first[2], "key2");
        ASSERT_EQ(remove_result.first[3], "key1");

        key_list.forEach([](const std::string & /*k*/, int64_t /*ts*/) { ASSERT_TRUE(false); });
    }

    /// emplaceKeys
    {
        std::vector<std::string> keys = {"key18", "key17", "key16", "key15", "key14", "key13"};
        std::vector<std::string> keys2 = keys;
        auto [ts, errcode] = key_list.emplaceKeys(keys2);
        ASSERT_EQ(errcode, DB::ErrorCodes::OK);

        {
            auto remove_result = key_list.removeTimedOutKeys(10'000, absl::flat_hash_set<std::string>{});
            ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
            ASSERT_TRUE(!key_list.empty());
            ASSERT_TRUE(remove_result.first.empty());

            /// key18, key107 key16 are sorted in memory to key16, key17, key18 and
            /// key15, key14 key13 are sorted in persistent store key13, key14, key15

            std::vector<std::string> partial_sorted{"key16", "key17", "key18", "key13", "key14", "key15"};

            forEach(key_list, partial_sorted, std::vector{ts, ts, ts, ts, ts, ts});
            forEachPersistent(key_list, std::vector<std::string>{"key13", "key14", "key15"}, std::vector{ts, ts, ts});
        }

        /// flush
        {
            ASSERT_EQ(key_list.flush(), DB::ErrorCodes::OK);
            ASSERT_TRUE(!key_list.empty());

            std::vector<std::string> partial_sorted{"key16", "key17", "key18", "key13", "key14", "key15"};

            /// forEach will loop in-memory key first and then persistent layer
            forEach(key_list, partial_sorted, std::vector{ts, ts, ts, ts, ts, ts});

            /// Since the batch of the keys have the same timestamp, after flush, the persistent layer will sort it
            std::ranges::sort(keys);
            forEachPersistent(key_list, keys, std::vector{ts, ts, ts, ts, ts, ts});
        }

        /// removeKeys
        {
            std::this_thread::sleep_for(std::chrono::milliseconds{5});

            auto remove_result = key_list.removeTimedOutKeys(1, absl::flat_hash_set<std::string>{}, /*max_keys_to_remove=*/2);
            ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
            ASSERT_TRUE(!key_list.empty());
            ASSERT_EQ(remove_result.first.size(), 2);

            ASSERT_EQ(remove_result.first[0], "key16");
            ASSERT_EQ(remove_result.first[1], "key17");

            /// Backfill key13, key14 from persist store to in-memory
            std::vector<std::string> partial_sorted2{"key13", "key14", "key18", "key15"};

            /// forEach will loop in-memory key first and then persistent layer
            forEach(key_list, partial_sorted2, std::vector{ts, ts, ts, ts});
            std::ranges::sort(partial_sorted2);
            forEachPersistent(key_list, partial_sorted2, std::vector{ts, ts, ts, ts});
        }

        /// emplaceKey
        int64_t ts19 = 0;
        {
            auto [ts19_1, errcode2] = key_list.emplaceKey("key19"); /// Commit to persistent tier
            ASSERT_EQ(errcode2, DB::ErrorCodes::OK);
            ts19 = ts19_1;

            std::this_thread::sleep_for(std::chrono::milliseconds{5});

            auto remove_result = key_list.removeTimedOutKeys(1, absl::flat_hash_set<std::string>{}, /*max_keys_to_remove=*/3);
            ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
            ASSERT_TRUE(!key_list.empty());
            ASSERT_EQ(remove_result.first.size(), 3);

            ASSERT_EQ(remove_result.first[0], "key13");
            ASSERT_EQ(remove_result.first[1], "key14");
            ASSERT_EQ(remove_result.first[2], "key18");

            /// Backfill key19 from persist store to in-memory
            std::vector<std::string> sorted{"key15", "key19"};

            /// forEach will loop in-memory key first and then persistent layer
            forEach(key_list, sorted, std::vector{ts, ts19});
            forEachPersistent(key_list, sorted, std::vector{ts, ts19});
        }

        /// emplaceKey
        int64_t ts20 = 0;
        {
            auto [ts20_1, errcode2] = key_list.emplaceKey("key20"); /// insert to memory only
            ASSERT_EQ(errcode2, DB::ErrorCodes::OK);
            ts20 = ts20_1;

            std::this_thread::sleep_for(std::chrono::milliseconds{5});

            std::vector<std::string> sorted{"key15", "key19"};
            std::vector<std::string> sorted2{"key15", "key19", "key20"};

            /// forEach will loop in-memory key first and then persistent layer
            forEach(key_list, sorted2, std::vector{ts, ts19, ts20});
            forEachPersistent(key_list, sorted, std::vector{ts, ts19});
        }

        /// removeTimedOutKeys
        {
            auto remove_result = key_list.removeTimedOutKeys(1, absl::flat_hash_set<std::string>{}, /*max_keys_to_remove=*/1);
            ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
            ASSERT_TRUE(!key_list.empty());
            ASSERT_EQ(remove_result.first.size(), 1);
            ASSERT_EQ(remove_result.first[0], "key15");

            std::vector<std::string> sorted2{"key19", "key20"};
            std::vector<std::string> sorted{"key19"};

            /// forEach will loop in-memory key first and then persistent layer
            forEach(key_list, sorted2, std::vector{ts19, ts20});
            forEachPersistent(key_list, sorted, std::vector{ts19});
        }

        /// removeTimedOutKeys
        {
            auto remove_result = key_list.removeTimedOutKeys(1, absl::flat_hash_set<std::string>{}, /*max_keys_to_remove=*/10);
            ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
            ASSERT_TRUE(key_list.empty());
            ASSERT_EQ(remove_result.first.size(), 2);
            ASSERT_EQ(remove_result.first[0], "key19");
            ASSERT_EQ(remove_result.first[1], "key20");

            /// forEach will loop in-memory key first and then persistent layer
            forEach(key_list, std::vector<std::string>{}, std::vector<int64_t>{});
            forEachPersistent(key_list, std::vector<std::string>{}, std::vector<int64_t>{});
        }
    }
}
