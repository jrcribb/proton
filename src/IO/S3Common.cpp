#include <IO/S3Common.h>

#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "config.h"

#if USE_AWS_S3

#    include <IO/HTTPHeaderEntries.h>
#    include <IO/S3/Client.h>
#    include <IO/S3/Requests.h>
#    include <Common/quoteString.h>
#    include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event S3GetObjectAttributes;
    extern const Event S3GetObjectMetadata;
    extern const Event S3HeadObject;
    extern const Event DiskS3GetObjectAttributes;
    extern const Event DiskS3GetObjectMetadata;
    extern const Event DiskS3HeadObject;
}

namespace DB
{

bool S3Exception::isRetryableError() const
{
    /// Looks like these list is quite conservative, add more codes if you wish
    static const std::unordered_set<Aws::S3::S3Errors> unretryable_errors = {
        Aws::S3::S3Errors::NO_SUCH_KEY,
        Aws::S3::S3Errors::ACCESS_DENIED,
        Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID,
        Aws::S3::S3Errors::INVALID_SIGNATURE,
        Aws::S3::S3Errors::NO_SUCH_UPLOAD,
        Aws::S3::S3Errors::NO_SUCH_BUCKET,
    };

    return !unretryable_errors.contains(code);
}

}

namespace DB::ErrorCodes
{
    extern const int S3_ERROR;
}

#endif

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

namespace S3
{

HTTPHeaderEntries getHTTPHeaders(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    HTTPHeaderEntries headers;
    Poco::Util::AbstractConfiguration::Keys subconfig_keys;
    config.keys(config_elem, subconfig_keys);
    for (const std::string & subkey : subconfig_keys)
    {
        if (subkey.starts_with("header"))
        {
            auto header_str = config.getString(config_elem + "." + subkey);
            auto delimiter = header_str.find(':');
            if (delimiter == std::string::npos)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Malformed s3 header value");
            headers.emplace_back(header_str.substr(0, delimiter), header_str.substr(delimiter + 1, String::npos));
        }
    }
    return headers;
}

ServerSideEncryptionKMSConfig getSSEKMSConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    ServerSideEncryptionKMSConfig sse_kms_config;

    if (config.has(config_elem + ".server_side_encryption_kms_key_id"))
        sse_kms_config.key_id = config.getString(config_elem + ".server_side_encryption_kms_key_id");

    if (config.has(config_elem + ".server_side_encryption_kms_encryption_context"))
        sse_kms_config.encryption_context = config.getString(config_elem + ".server_side_encryption_kms_encryption_context");

    if (config.has(config_elem + ".server_side_encryption_kms_bucket_key_enabled"))
        sse_kms_config.bucket_key_enabled = config.getBool(config_elem + ".server_side_encryption_kms_bucket_key_enabled");

    return sse_kms_config;
}

AuthSettings AuthSettings::loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
{
        auto access_key_id = config.getString(config_elem + ".access_key_id", "");
        auto secret_access_key = config.getString(config_elem + ".secret_access_key", "");
        auto session_token = config.getString(config_elem + ".session_token", ""); /// proton: added
        auto region = config.getString(config_elem + ".region", "");
        auto server_side_encryption_customer_key_base64 = config.getString(config_elem + ".server_side_encryption_customer_key_base64", "");

        std::optional<bool> use_environment_credentials;
        if (config.has(config_elem + ".use_environment_credentials"))
            use_environment_credentials = config.getBool(config_elem + ".use_environment_credentials");

        std::optional<bool> use_insecure_imds_request;
        if (config.has(config_elem + ".use_insecure_imds_request"))
            use_insecure_imds_request = config.getBool(config_elem + ".use_insecure_imds_request");

        std::optional<uint64_t> expiration_window_seconds;
        if (config.has(config_elem + ".expiration_window_seconds"))
            expiration_window_seconds = config.getUInt64(config_elem + ".expiration_window_seconds");

        std::optional<bool> no_sign_request;
        if (config.has(config_elem + ".no_sign_request"))
            no_sign_request = config.getBool(config_elem + ".no_sign_request");

        auto role_arn = config.getString(config_elem + ".role_arn", "");
        auto role_session_name = config.getString(config_elem + ".role_session_name", "");
        auto http_client = config.getString(config_elem + ".http_client", "");
        auto service_account = config.getString(config_elem + ".service_account", "");
        auto metadata_service = config.getString(config_elem + ".metadata_service", "");
        auto request_token_path = config.getString(config_elem + ".request_token_path", "");

        HTTPHeaderEntries headers = getHTTPHeaders(config_elem, config);
        ServerSideEncryptionKMSConfig sse_kms_config = getSSEKMSConfig(config_elem, config);

        return AuthSettings
            {
                .access_key_id = std::move(access_key_id),
                .secret_access_key = std::move(secret_access_key),
                .session_token = std::move(session_token),
                .region = std::move(region),
                .server_side_encryption_customer_key_base64 = std::move(server_side_encryption_customer_key_base64),
                .server_side_encryption_kms_config = std::move(sse_kms_config),
                .role_arn = std::move(role_arn),
                .role_session_name = std::move(role_session_name),
                .http_client = std::move(http_client),
                .service_account = std::move(service_account),
                .metadata_service = std::move(metadata_service),
                .request_token_path = std::move(request_token_path),
                .headers = std::move(headers),
                .use_environment_credentials = use_environment_credentials,
                .use_insecure_imds_request = use_insecure_imds_request,
                .expiration_window_seconds = expiration_window_seconds,
                .no_sign_request = no_sign_request,
            };
}

bool AuthSettings::hasUpdates(const AuthSettings & other) const
{
    AuthSettings copy = *this;
    copy.updateFrom(other);
    return *this != copy;
}

void AuthSettings::updateFrom(const AuthSettings & from)
{
        /// Update with check for emptyness only parameters which
        /// can be passed not only from config, but via ast.

        if (!from.access_key_id.empty())
            access_key_id = from.access_key_id;
        if (!from.secret_access_key.empty())
            secret_access_key = from.secret_access_key;
        if (!from.session_token.empty())
            session_token = from.session_token;
        if (!from.role_arn.empty())
            role_arn = from.role_arn;
        if (!from.role_session_name.empty())
            role_session_name = from.role_session_name;
        if (!from.http_client.empty())
            http_client = from.http_client;
        if (!from.service_account.empty())
            service_account = from.service_account;
        if (!from.metadata_service.empty())
            metadata_service = from.metadata_service;
        if (!from.request_token_path.empty())
            request_token_path = from.request_token_path;

        headers = from.headers;
        region = from.region;
        server_side_encryption_customer_key_base64 = from.server_side_encryption_customer_key_base64;
        server_side_encryption_kms_config = from.server_side_encryption_kms_config;

        if (from.use_environment_credentials.has_value())
            use_environment_credentials = from.use_environment_credentials;

        if (from.use_insecure_imds_request.has_value())
            use_insecure_imds_request = from.use_insecure_imds_request;

        if (from.expiration_window_seconds.has_value())
            expiration_window_seconds = from.expiration_window_seconds;

        if (from.no_sign_request.has_value())
            no_sign_request = from.no_sign_request;
}

}
}
