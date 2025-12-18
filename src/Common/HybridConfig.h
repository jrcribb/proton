#pragma once

#include <Common/Exception.h>
#include <Common/Rocks/RocksDB.h>

#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

struct HybridConfig
{
    void validate()
    {
        if (spill_dir_path.empty())
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "HybridHashTable: spill to disk folder is not configured");

        if (max_hot_key_count == 0)
            max_hot_key_count = std::numeric_limits<size_t>::max();
    }

    HybridConfig getSubConfig(std::string_view sub_id, bool unshared = false) const
    {
        chassert(!sub_id.empty());

        HybridConfig new_config = *this;

        /// If `rocks_cf_handler_getter` is set, use a different cf handler with `sub_id`; otherwise, use a new one with `spill_dir_path`.
        if (rocks_cf_handler_getter)
        {
            new_config.cf_handle_id = cf_handle_id.empty() ? sub_id : fmt::format("{}-{}", cf_handle_id, sub_id);

            if (unshared)
            {
                new_config.spill_dir_path = fmt::format("{}-{}", spill_dir_path, new_config.cf_handle_id);
                new_config.cf_handle_id = "";
                new_config.rocks_cf_handler_getter = {};
            }
        }
        else
        {
            new_config.spill_dir_path = fmt::format("{}-{}", spill_dir_path, sub_id);
        }

        return new_config;
    }

    rocksdb::Options getRocksOptions() const
    {
        rocksdb::DBOptions merged_db_options;
        {
            rocksdb::DBOptions db_options;
            db_options.atomic_flush = true;
            /// options.num_levels = 3;
            db_options.create_if_missing = true;
            db_options.create_missing_column_families = true;
            /// options.statistics = rocksdb::CreateDBStatistics();
            db_options.info_log_level = rocksdb::ERROR_LEVEL;

            rocksdb::ConfigOptions db_conf_options;
            db_conf_options.ignore_unknown_options = true;

            if (auto status = rocksdb::GetDBOptionsFromString(db_conf_options, db_options, kv_options, &merged_db_options); !status.ok())
                merged_db_options = std::move(db_options);
        }

        rocksdb::ColumnFamilyOptions merged_cf_options;
        {
            rocksdb::ColumnFamilyOptions cf_options;
            cf_options.compression = rocksdb::CompressionType::kLZ4Compression;

            rocksdb::BlockBasedTableOptions table_options;

            if (use_hash_index)
            {
                table_options.data_block_hash_table_util_ratio = 0.75;
                table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
            }
            else
            {
                table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch;
            }

            table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

            cf_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

            rocksdb::ConfigOptions cf_conf_options;
            cf_conf_options.ignore_unknown_options = true;

            if (auto status = rocksdb::GetColumnFamilyOptionsFromString(cf_conf_options, cf_options, kv_options, &merged_cf_options);
                !status.ok())
                merged_cf_options = std::move(cf_options);
        }

        return rocksdb::Options(merged_db_options, merged_cf_options);
    }

    /// spill_dir_path_ file system path which holds for spill-to-disk key / values
    std::string spill_dir_path;
    /// kv_options_ spill-to-disk (rocks) db options
    std::string kv_options;

    /// When ttl <= 0, it means infinity
    int32_t ttl = -1;

    /// If \use_hash_index is true, Binary & hash index will be used, otherwise only binary search will be used.
    /// Hash index usually have better point query perf but will occupy more space
    bool use_hash_index = false;

    /// When in-memory keys count exceed this threshold, spill to disk
    size_t max_hot_key_count = 10'000;
    /// cleanup_on_disk_data_ if true, during dtor, cleanup spill-to-disk data, otherwise keep it around
    bool cleanup_on_disk_data = true;

    /// If `rocks_cf_handler_getter` is set, we will get rocks cf handler by it
    std::string cf_handle_id;
    std::function<RocksDBColumnFamilyHandlerPtr(const HybridConfig & /*config*/)> rocks_cf_handler_getter;
};

}
