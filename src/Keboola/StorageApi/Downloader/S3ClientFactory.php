<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use Aws\S3\S3Client;

class S3ClientFactory
{
    public const CONNECT_TIMEOUT_SECONDS = 10;

    public const STALL_TIMEOUT_SECONDS = 60;

    public const MIN_TRANSFER_RATE_BYTES_PER_SECOND = 1024;

    /**
     * Liveness backstop, deliberately not a size cap: the stall detection below only aborts a
     * transfer that drops under MIN_TRANSFER_RATE_BYTES_PER_SECOND, so a link that crawls just
     * above it would otherwise run for months (40 GB at 1 KB/s is over a year). At 80 MB/s this
     * ceiling is ~3.4 TB, orders of magnitude above any exported file.
     */
    public const MAX_TRANSFER_SECONDS = 12 * 3600;

    public static function createClient(
        string $region,
        string $accessKeyId,
        string $secretAccessKey,
        string $sessionToken,
        int $retries,
    ): S3Client {
        $options = self::transferOptions($retries);
        $options['region'] = $region;
        $options['credentials'] = [
            'key' => $accessKeyId,
            'secret' => $secretAccessKey,
            'token' => $sessionToken,
        ];

        return new S3Client($options);
    }

    /**
     * @internal Single source of truth for download transfer settings, exposed so that the policy
     *  can be asserted without a network call. Deliberately credential-free, so that no caller and
     *  no failing assertion on the returned array can surface download credentials.
     * @return array{
     *     version: string,
     *     retries: int,
     *     http: array{
     *         decode_content: bool,
     *         connect_timeout: int,
     *         timeout: int,
     *         read_timeout: int,
     *         curl: array<int, int>,
     *     },
     * }
     */
    public static function transferOptions(int $retries): array
    {
        return [
            'version' => '2006-03-01',
            'retries' => $retries,
            'http' => [
                'decode_content' => false,
                'connect_timeout' => self::CONNECT_TIMEOUT_SECONDS,
                // Sized so that object size never decides the outcome; see MAX_TRANSFER_SECONDS.
                // Set explicitly rather than omitted, because the AWS SDK injects a default
                // timeout for the key when AWS_DEFAULTS_MODE is not "legacy".
                'timeout' => self::MAX_TRANSFER_SECONDS,
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
