<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;

class GCSUploader
{
    public const STORAGE_CLASS_PERMANENT = 'NEARLINE';
    public const STORAGE_CLASS_STANDARD = 'STANDARD';

    private GoogleStorageClient $gcsClient;

    private FetchAuthTokenInterface $fetchAuthToken;

    public function __construct(array $options)
    {
        $this->fetchAuthToken = new class ($options['credentials']) implements FetchAuthTokenInterface {
            private array $creds;

            public function __construct(
                array $creds
            ) {
                $this->creds = $creds;
            }

            public function fetchAuthToken(callable $httpHandler = null)
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
        $this->gcsClient = new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $this->fetchAuthToken,
        ]);
    }

    public function uploadFile(string $bucket, string $filePath, string $fileName, bool $isPermanent)
    {
        $retBucket = $this->gcsClient->bucket($bucket);
        $retBucket->upload(
            fopen($fileName, 'rb'),
            [
                'name' => $filePath,
                'metadata' => [
                    // in gcs is not possible set life cycles to directory, it must set by storageClass mapped to life cycle when file is uploaded
                    'storageClass' => $isPermanent ? self::STORAGE_CLASS_PERMANENT : self::STORAGE_CLASS_STANDARD,
                ],
            ]
        );
    }

    }
}
