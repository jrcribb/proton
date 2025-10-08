#pragma once

#include <base/ClockUtils.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include <cstring>
#include <memory>

namespace DB
{

class RocksDBTTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    RocksDBTTLCompactionFilter(uint32_t ttl_sec_, std::string cf_name_, LoggerPtr logger_)
        : ttl_sec(ttl_sec_), cf_name(std::move(cf_name_)), logger(std::move(logger_))
    {
    }

    const char * Name() const override { return "RocksDBTTLCompactionFilter"; }

    /// \return true if the key needs to be filtered out (delete), otherwise return false
    bool Filter(
        int /*level*/,
        const rocksdb::Slice & key,
        const rocksdb::Slice & existing_value,
        std::string * /*new_value*/,
        bool * /*value_changed*/) const override
    {
        if (existing_value.size() < sizeof(uint32_t))
        {
            LOG_ERROR(logger, "Too short value with length={} for TTL compaction key={}", existing_value.size(), key.ToStringView());
            return false; /// skip invalid
        }

        uint32_t ts = 0;
        {
            DB::ReadBufferFromMemory rb{existing_value.data() + existing_value.size() - sizeof(uint32_t), sizeof(uint32_t)};
            DB::readIntBinary(ts, rb);
        }

        auto now = static_cast<UInt64>(DB::UTCSeconds::now());
        if (ts + ttl_sec <= now)
        {
            /// LOG_DEBUG(
            ///    logger, "Removing key for column_family={} because TTL reaches, ttl_sec={} ts={} ts_now={}", cf_name, ttl_sec, ts, now);
            return true;
        }

        return false;
    }

private:
    const uint32_t ttl_sec;
    const std::string cf_name;
    LoggerPtr logger;
};

class RocksDBTTLCompactionFilterFactory : public rocksdb::CompactionFilterFactory
{
public:
    RocksDBTTLCompactionFilterFactory(uint32_t ttl_sec_, std::string cf_name_, LoggerPtr logger_)
        : ttl_sec(ttl_sec_), cf_name(std::move(cf_name_)), logger(std::move(logger_))
    {
    }

    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(const rocksdb::CompactionFilter::Context &) override
    {
        return std::make_unique<RocksDBTTLCompactionFilter>(ttl_sec, cf_name, logger);
    }

    const char * Name() const override { return "RocksDBTTLCompactionFilterFactory"; }

private:
    const uint32_t ttl_sec;
    const std::string cf_name;
    LoggerPtr logger;
};

}
