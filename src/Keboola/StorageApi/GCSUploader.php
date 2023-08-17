<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use Keboola\StorageApi\GCSUploader\PromiseHandler;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\StorageApi\S3Uploader\Chunker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class GCSUploader
{
    private GoogleStorageClient $gcsClient;

    private FetchAuthTokenInterface $fetchAuthToken;

    private LoggerInterface $logger;

    private FileUploadTransferOptions $transferOptions;

    public function __construct(
        array $options,
        LoggerInterface $logger = null,
        FileUploadTransferOptions $transferOptions = null
    ) {
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

        if (!$transferOptions) {
            $this->transferOptions = new FileUploadTransferOptions();
        } else {
            $this->transferOptions = $transferOptions;
        }

        if (!$logger) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
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
            ]
        );
    }

    public function uploadSlicedFile(string $bucket, string $key, array $slices): void
    {
        $preparedSlices = [];
        $manifest = [
            'entries' => [],
        ];

        foreach ($slices as $filePath) {
            $blobName = sprintf(
                '%s%s',
                $key,
                basename($filePath)
            );

            $preparedSlices[$filePath] = $blobName;

            $manifest['entries'][] = [
                'url' => sprintf(
                    'gs://%s/%s',
                    $bucket,
                    $blobName
                ),
            ];
        }
        $chunker = new Chunker($this->transferOptions->getChunkSize());
        $chunks = $chunker->makeChunks($preparedSlices);

        foreach ($chunks as $chunk) {
            $this->upload($bucket, $key, $chunk);
        }

        $this->gcsClient->bucket($bucket)->upload((string) json_encode($manifest), [
            'name' => $key . 'manifest',
        ]);
    }

    private function upload(string $bucket, string $key, array $chunk): void
    {
        $retBucket = $this->gcsClient->bucket($bucket);
        $promises = [];
        foreach ($chunk as $filePath => $blobName) {
            $fileToUpload = fopen($filePath, 'rb');
            if (!$fileToUpload) {
                throw new ClientException("Cannot open file {$filePath}");
            }

            if (filesize($filePath) === 0) {
                $retBucket->upload(
                    $fileToUpload,
                    [
                        'name' => $blobName,
                    ]
                );
                continue;
            }

            $promises[$filePath] = $retBucket->uploadAsync(
                $fileToUpload,
                [
                    'name' => $blobName,
                ]
            );
        }
        $retries = 0;
        while (true) {
            if ($retries >= $this->transferOptions->getMaxRetriesPerChunk()) {
                throw new ClientException('Exceeded maximum number of retries per chunk upload');
            }
            $results = \GuzzleHttp\Promise\Utils::settle($promises)->wait();
            if (!is_array($results)) {
                throw new ClientException('Wrong response.');
            }
            $rejected = PromiseHandler::getRejected($results);
            if (count($rejected) == 0) {
                break;
            }
            $retries++;
            /**
             * @var string $filePath
             * @var ServiceException $reason
             */
            foreach ($rejected as $filePath => $reason) {
                $blobName = sprintf(
                    '%s%s',
                    $key,
                    basename($filePath)
                );
                $this->logger->notice(sprintf(sprintf('Uploadfailed: %s, %s, %s, %s', $filePath, $reason->getMessage(), $reason->getCode(), $blobName)));
                $promise = $retBucket->uploadAsync(
                    $filePath,
                    [
                        'name' => $blobName,
                    ]
                );

                $promises[$filePath] = $promise;
            }
        }
    }
}
