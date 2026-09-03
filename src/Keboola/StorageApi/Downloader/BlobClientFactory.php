<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use GuzzleHttp\Handler\CurlHandler;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Handler\Proxy;
use GuzzleHttp\HandlerStack;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

class BlobClientFactory
{
    public const CONNECT_TIMEOUT_SECONDS = 10;

    public const STALL_TIMEOUT_SECONDS = 60;

    public const MIN_TRANSFER_RATE_BYTES_PER_SECOND = 1024;

    // Liveness backstop, not a size cap: ~3.4 TB at 80 MB/s.
    public const MAX_TRANSFER_SECONDS = 12 * 3600;

    /**
     * Upload client. Downloads use createDownloadClient().
     *
     * @param string $connectionString
     * @return BlobRestProxy
     */
    public static function createClientFromConnectionString(
        $connectionString,
    ) {
        $client = BlobRestProxy::createBlobService($connectionString, [
            'http' => [
                'connect_timeout' => 10,
                'timeout' => 120,
            ],
        ]);
        $client->pushMiddleware(BlobStorageRetryMiddleware::create());

        return $client;
    }

    public static function createDownloadClient(string $connectionString): BlobRestProxy
    {
        $http = self::downloadTransferOptions();
        // getBlob() requests a streamed body, which Guzzle's default handler sends to the
        // StreamHandler: there the options above are ignored and a stall truncates silently.
        $http['handler'] = HandlerStack::create(Proxy::wrapSync(new CurlMultiHandler(), new CurlHandler()));

        $client = BlobRestProxy::createBlobService($connectionString, ['http' => $http]);
        $client->pushMiddleware(BlobStorageRetryMiddleware::create());

        return $client;
    }

    /**
     * Download transfer policy, mirroring S3ClientFactory::transferOptions()['http'].
     *
     * @return array{
     *     connect_timeout: int,
     *     timeout: int,
     *     curl: array<int, int>,
     * }
     */
    public static function downloadTransferOptions(): array
    {
        return [
            'connect_timeout' => self::CONNECT_TIMEOUT_SECONDS,
            'timeout' => self::MAX_TRANSFER_SECONDS,
            'curl' => [
                CURLOPT_LOW_SPEED_LIMIT => self::MIN_TRANSFER_RATE_BYTES_PER_SECOND,
                CURLOPT_LOW_SPEED_TIME => self::STALL_TIMEOUT_SECONDS,
            ],
        ];
    }
}
