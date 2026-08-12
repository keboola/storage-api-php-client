<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use Aws\S3\S3Client;

class S3ClientFactory
{
    public const CONNECT_TIMEOUT_SECONDS = 10;

    public const STALL_TIMEOUT_SECONDS = 60;

    public const MIN_TRANSFER_RATE_BYTES_PER_SECOND = 1024;

    public static function createClient(
        string $region,
        string $accessKeyId,
        string $secretAccessKey,
        string $sessionToken,
        int $retries,
    ): S3Client {
        return new S3Client(self::createClientOptions(
            $region,
            $accessKeyId,
            $secretAccessKey,
            $sessionToken,
            $retries,
        ));
    }

    /**
     * @internal Single source of truth for download transfer settings, exposed so that the policy
     *  can be asserted without a network call.
     * @return array{
     *     version: string,
     *     region: string,
     *     retries: int,
     *     credentials: array{key: string, secret: string, token: string},
     *     http: array{
     *         decode_content: bool,
     *         connect_timeout: int,
     *         timeout: int,
     *         read_timeout: int,
     *         curl: array<int, int>,
     *     },
     * }
     */
    public static function createClientOptions(
        string $region,
        string $accessKeyId,
        string $secretAccessKey,
        string $sessionToken,
        int $retries,
    ): array {
        return [
            'version' => '2006-03-01',
            'region' => $region,
            'retries' => $retries,
            'credentials' => [
                'key' => $accessKeyId,
                'secret' => $secretAccessKey,
                'token' => $sessionToken,
            ],
            'http' => [
                'decode_content' => false,
                'connect_timeout' => self::CONNECT_TIMEOUT_SECONDS,
                // 0 = no total transfer deadline: a download must be bounded by throughput, not by
                // object size. Set explicitly rather than omitted, because the AWS SDK injects a
                // default timeout for the key when AWS_DEFAULTS_MODE is not "legacy".
                'timeout' => 0,
                // honoured only by Guzzle's StreamHandler
                'read_timeout' => self::STALL_TIMEOUT_SECONDS,
                // the cURL handler's equivalent: abort a transfer that has effectively stalled.
                // Without ext-curl Guzzle falls back to the StreamHandler, where read_timeout
                // applies instead and the CURLOPT_* constants would not even be defined.
                'curl' => extension_loaded('curl') ? [
                    CURLOPT_LOW_SPEED_LIMIT => self::MIN_TRANSFER_RATE_BYTES_PER_SECOND,
                    CURLOPT_LOW_SPEED_TIME => self::STALL_TIMEOUT_SECONDS,
                ] : [],
            ],
        ];
    }
}
