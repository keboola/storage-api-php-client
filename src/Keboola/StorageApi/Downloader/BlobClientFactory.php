<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Http\Message\RequestInterface;

class BlobClientFactory
{
    public const CONNECT_TIMEOUT_SECONDS = 10;

    public const STALL_TIMEOUT_SECONDS = 60;

    public const MIN_TRANSFER_RATE_BYTES_PER_SECOND = 1024;

    /**
     * Liveness backstop, deliberately not a size cap: the stall detection below only aborts a
     * transfer that drops under MIN_TRANSFER_RATE_BYTES_PER_SECOND, so a link that crawls just
     * above it would otherwise run for months. At 80 MB/s this ceiling is ~3.4 TB.
     *
     * It bounds one attempt, not the whole download: BlobStorageRetryMiddleware retries a timeout
     * like any other failure, so the worst case is DEFAULT_NUMBER_OF_RETRIES + 1 of these windows.
     */
    public const MAX_TRANSFER_SECONDS = 12 * 3600;

    /**
     * A real total deadline for uploads, which never request a streamed body: it applies per
     * 4 MiB block (ABSUploader::CHUNK_SIZE), or per blob for a small single-request upload.
     */
    public const UPLOAD_TIMEOUT_SECONDS = 120;

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
                'connect_timeout' => self::CONNECT_TIMEOUT_SECONDS,
                'timeout' => self::UPLOAD_TIMEOUT_SECONDS,
            ],
        ]);
        $client->pushMiddleware(BlobStorageRetryMiddleware::create());

        return $client;
    }

    public static function createDownloadClient(string $connectionString): BlobRestProxy
    {
        $client = BlobRestProxy::createBlobService($connectionString, [
            'http' => self::downloadTransferOptions(),
        ]);
        $client->pushMiddleware(self::clearStreamOption());
        $client->pushMiddleware(BlobStorageRetryMiddleware::create());

        return $client;
    }

    /**
     * getBlobAsync() asks for the body with Guzzle's stream option, which Proxy::wrapStreaming()
     * routes to the StreamHandler: there the policy below is ignored and a stalled read ends the
     * copy without an exception, so the retry middleware never sees a failure and the caller gets
     * a truncated file. The option is per-request, so client config cannot override it; clearing
     * it in a middleware can, and unlike a hand-built handler it leaves the handler choice (cURL
     * availability, TLS fallback, transport sharing) to Guzzle.
     *
     * @internal Public so that the middleware can be exercised against a stalling server without
     *  waiting out STALL_TIMEOUT_SECONDS.
     * @return callable(callable): callable
     */
    public static function clearStreamOption(): callable
    {
        return static fn (callable $handler): callable
            => static fn (RequestInterface $request, array $options)
                => $handler($request, ['stream' => false] + $options);
    }

    /**
     * Download transfer policy, mirroring S3ClientFactory::transferOptions()['http'].
     *
     * @return array{
     *     connect_timeout: int,
     *     timeout: int,
     *     read_timeout: int,
     *     curl: array<int, int>,
     * }
     */
    public static function downloadTransferOptions(): array
    {
        return [
            'connect_timeout' => self::CONNECT_TIMEOUT_SECONDS,
            // Sized so that object size never decides the outcome; see MAX_TRANSFER_SECONDS.
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
        ];
    }
}
