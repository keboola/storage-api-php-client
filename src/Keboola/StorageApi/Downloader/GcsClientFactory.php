<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;

class GcsClientFactory
{
    public const CONNECT_TIMEOUT_SECONDS = 10;

    public const STALL_TIMEOUT_SECONDS = 60;

    public const MIN_TRANSFER_RATE_BYTES_PER_SECOND = 1024;

    /**
     * Liveness backstop, deliberately not a size cap: the stall detection below only aborts a
     * transfer that drops under MIN_TRANSFER_RATE_BYTES_PER_SECOND, so a link that crawls just
     * above it would otherwise run forever. At 80 MB/s this ceiling is ~3.4 TB.
     */
    public const MAX_TRANSFER_SECONDS = 12 * 3600;

    /**
     * @param array $fileResponse
     * @return StorageClient
     */
    public static function createClientFromCredentialsArray(
        $fileResponse,
    ) {
        $options = [
            'credentials' => [
                'access_token' => $fileResponse['gcsCredentials']['access_token'],
                'expires_in' => $fileResponse['gcsCredentials']['expires_in'],
                'token_type' => $fileResponse['gcsCredentials']['token_type'],
            ],
            'projectId' => $fileResponse['gcsCredentials']['projectId'],
        ];

        $fetchAuthToken = new class ($options['credentials']) implements FetchAuthTokenInterface {
            private array $creds;

            public function __construct(
                array $creds,
            ) {
                $this->creds = $creds;
            }

            public function fetchAuthToken(?callable $httpHandler = null)
            {
                return $this->creds;
            }

            public function getCacheKey()
            {
                return '';
            }

            public function getLastReceivedToken()
            {
                return $this->creds;
            }
        };
        return new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);
    }

    /**
     * Transfer policy for a single object download, mirroring S3ClientFactory::transferOptions().
     *
     * Must be passed per call rather than to the client: Rest::downloadObject() always sets its own
     * restOptions['on_headers'], and RequestWrapper::getRequestOptions() picks per-request
     * restOptions over the client-level ones with ?? instead of merging them, so anything
     * configured on the StorageClient is silently dropped for downloads.
     *
     * @return array{
     *     restOptions: array{
     *         connect_timeout: int,
     *         timeout: int,
     *         read_timeout: int,
     *         curl: array<int, int>,
     *     },
     * }
     */
    public static function downloadOptions(): array
    {
        return [
            'restOptions' => [
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
            ],
        ];
    }
}
