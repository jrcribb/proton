#include <base/sleep.h>
#include <Common/Logger.h>
#include <Common/Rocks/RocksDBTTLCompactionFilter.h>

#include <iostream>

std::string appendTimestamp(std::string payload)
{
    auto now = static_cast<int32_t>(DB::UTCSeconds::now());
    std::string val(sizeof(int32_t), '\0');
    std::memcpy(&val[0], &now, sizeof(int32_t));
    payload += val;
    return payload;
}

int main()
{
    const std::string kDBPath = "/tmp/rocksdb_multi_cf_ttl";

    rocksdb::DB * db;
    rocksdb::Options base_opts;
    base_opts.create_if_missing = true;
    base_opts.create_missing_column_families = true;

    // List CF descriptors: name + options per CF
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs
        = {rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions(base_opts)),
           rocksdb::ColumnFamilyDescriptor("hot_data", rocksdb::ColumnFamilyOptions(base_opts)),
           rocksdb::ColumnFamilyDescriptor("cold_data", rocksdb::ColumnFamilyOptions(base_opts))};

    auto logger = getLogger("ttl-compaction");

    // Assign TTL filters
    cf_descs[0].options.compaction_filter_factory = std::make_shared<DB::RocksDBTTLCompactionFilterFactory>(30, "default", logger);
    cf_descs[1].options.compaction_filter_factory = std::make_shared<DB::RocksDBTTLCompactionFilterFactory>(10, "hot_data", logger);
    cf_descs[2].options.compaction_filter_factory = std::make_shared<DB::RocksDBTTLCompactionFilterFactory>(120, "cold_data", logger);

    // Open DB with multiple CFs
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    auto s = rocksdb::DB::Open(rocksdb::DBOptions(base_opts), kDBPath, cf_descs, &handles, &db);
    if (!s.ok())
    {
        std::cerr << "Open failed: " << s.ToString() << "\n";
        return 1;
    }

    // Put some test keys
    db->Put(rocksdb::WriteOptions(), handles[1], "k_hot", appendTimestamp("foo_hot"));
    db->Put(rocksdb::WriteOptions(), handles[2], "k_cold", appendTimestamp("foo_cold"));

    std::cout << "Inserted hot+cold data. Sleeping 15s...\n";
    sleepForSeconds(15);

    std::cout << "Trigger manual compaction...\n";
    for (auto * h : handles)
        db->CompactRange(rocksdb::CompactRangeOptions(), h, nullptr, nullptr);

    std::string val;
    if (db->Get(rocksdb::ReadOptions(), handles[1], "k_hot", &val).IsNotFound())
        std::cout << "hot_data expired (TTL=10s)\n";
    else
        std::cout << "hot_data still there\n";

    if (db->Get(rocksdb::ReadOptions(), handles[2], "k_cold", &val).IsNotFound())
        std::cout << "cold_data expired\n";
    else
        std::cout << "cold_data still there (TTL=120s)\n";

    for (auto * h : handles)
        db->DestroyColumnFamilyHandle(h);

    delete db;

    return 0;
}
