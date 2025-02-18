<?php

namespace Keboola\StorageApi\Downloader;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;

class GcsClientFactory
{
    /**
     * @param array $fileResponse
     * @return StorageClient
     */
    public static function createClientFromCredentialsArray(
        $fileResponse
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
                array $creds
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
}
