<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;

class GCSUploader
{
    public const STORAGE_CLASS_PERMANENT = 'COLDLINE';
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

    public function uploadFile(string $bucket, string $filePath, string $fileName, bool $isPermanent): void
    {
        $retBucket = $this->gcsClient->bucket($bucket);
        $file = fopen($fileName, 'rb');
        if (!$file) {
            throw new ClientException("Cannot open file {$file}");
        }
        $retBucket->upload(
            $file,
            [
                'name' => $filePath,
                'metadata' => [
                    // in gcs is not possible set life cycles to directory, it must set by storageClass mapped to life cycle when file is uploaded
                    'storageClass' => $this->getLifeCycleStorageClass($isPermanent),
                ],
            ]
        );
    }

    public function uploadSlicedFile(string $bucket, string $key, array $slices, bool $isPermanent): void
    {
        $retBucket = $this->gcsClient->bucket($bucket);

        $manifest = [
            'entries' => [],
        ];
        $promises = [];
        foreach ($slices as $gcsFilePath) {
            $fileToUpload = fopen($gcsFilePath, 'rb');
            if (!$fileToUpload) {
                throw new ClientException("Cannot open file {$gcsFilePath}");
            }

            $blobName = sprintf(
                '%s%s',
                $key,
                basename($gcsFilePath)
            );

            $manifest['entries'][] = [
                'url' => sprintf(
                    'gs://%s/%s',
                    $bucket,
                    $blobName
                ),
            ];

            if (filesize($gcsFilePath) === 0) {
                $retBucket->upload(
                    $fileToUpload,
                    [
                        'name' => $blobName,
                        'metadata' => [
                            // in gcs is not possible set life cycles to directory, it must set by storageClass mapped to life cycle when file is uploaded
                            'storageClass' => $this->getLifeCycleStorageClass($isPermanent),
                        ],
                    ]
                );
                continue;
            }

            $promises[$gcsFilePath] = $retBucket->uploadAsync(
                $fileToUpload,
                [
                    'name' => $blobName,
                    'metadata' => [
                        // in gcs is not possible set life cycles to directory, it must set by storageClass mapped to life cycle when file is uploaded
                        'storageClass' => $this->getLifeCycleStorageClass($isPermanent),
                    ],
                ]
            );
        }

        \GuzzleHttp\Promise\settle($promises)->wait();

        /** @var resource $stream */
        $stream = fopen('data://application/json,' . json_encode($manifest), 'r');

        $retBucket->upload($stream, [
            'name' => $key . 'manifest',
        ]);
    }

    private function getLifeCycleStorageClass(bool $isPermanent): string
    {
        return $isPermanent ? self::STORAGE_CLASS_PERMANENT : self::STORAGE_CLASS_STANDARD;
    }
}
