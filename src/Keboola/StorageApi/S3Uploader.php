<?php

namespace Keboola\StorageApi;

use Aws\Multipart\UploadState;
use Aws\S3\Exception\S3MultipartUploadException;
use Aws\S3\S3Client;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\StorageApi\S3Uploader\Chunker;
use Keboola\StorageApi\S3Uploader\PromiseHandler;
use Keboola\StorageApi\S3Uploader\PromiseResultHandler;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/** @internal class used by the client for uploading files to file storage */
class S3Uploader
{
    /**
     * @var S3Client
     */
    private $s3Client;

    /**
     * @var FileUploadTransferOptions
     */
    private $transferOptions;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * S3Uploader constructor.
     * @param S3Client $s3Client
     * @param FileUploadTransferOptions|null $transferOptions
     */
    public function __construct(S3Client $s3Client, FileUploadTransferOptions $transferOptions = null, LoggerInterface $logger = null)
    {
        $this->s3Client = $s3Client;
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

    /**
     * @param $bucket
     * @param $key
     * @param $acl
     * @param $file
     * @param string $name
     * @param string $encryption
     * @throws ClientException
     */
    public function uploadFile($bucket, $key, $acl, $file, $name, $encryption = null)
    {
        $this->upload($bucket, $acl, [$file => $key], $name, $encryption);
    }

    /**
     * @param $bucket
     * @param $key
     * @param $acl
     * @param $slices
     * @param null $encryption
     * @throws ClientException
     */
    public function uploadSlicedFile($bucket, $key, $acl, $slices, $encryption = null)
    {
        $preparedSlices = [];
        foreach ($slices as $filePath) {
            $preparedSlices[$filePath] = $key . basename($filePath);
        }
        $chunker = new Chunker($this->transferOptions->getChunkSize());
        $chunks = $chunker->makeChunks($preparedSlices);
        foreach ($chunks as $chunk) {
            $this->upload($bucket, $acl, $chunk, null, $encryption);
        }
    }

    /**
     * @param $bucket
     * @param $acl
     * @param $key
     * @param $filePath
     * @param string $name
     * @param string $encryption
     * @throws ClientException
     */
    private function putFile($bucket, $key, $acl, $filePath, $name = null, $encryption = null)
    {
        $fh = @fopen($filePath, 'r');
        if ($fh === false) {
            throw new ClientException('Error on file upload to S3: ' . $filePath, null, null, 'fileNotReadable');
        }
        $options = [
            'Bucket' => $bucket,
            'Key' => $key,
            'ACL' => $acl,
            'Body' => $fh,
            'ContentDisposition' => sprintf('attachment; filename=%s;', $name ? $name : basename($filePath)),
        ];

        if ($encryption) {
            $options ['ServerSideEncryption'] = $encryption;
        }
        $this->s3Client->putObject($options);
        if (is_resource($fh)) {
            fclose($fh);
        }
    }

    /**
     * @param $bucket
     * @param $acl
     * @param array $files
     * @param string $name
     * @param string $encryption
     * @throws ClientException
     */
    private function upload($bucket, $acl, $files, $name = null, $encryption = null)
    {
        $promises = [];
        foreach ($files as $filePath => $key) {
            /*
             * Cannot upload empty files using multipart: https://github.com/aws/aws-sdk-php/issues/1429
             * Upload them directly immediately and continue to next part in the chunk.
             */
            if (filesize($filePath) === 0) {
                $this->putFile($bucket, $key, $acl, $filePath, $name, $encryption);
                continue;
            }
            $uploader = $this->multipartUploaderFactory(
                $filePath,
                $bucket,
                $key,
                $acl,
                count($files) > 1 ? $this->transferOptions->getMultiFileConcurrency() : $this->transferOptions->getSingleFileConcurrency(),
                $encryption ? $encryption : null,
                $name ? $name : basename($filePath),
            );
            $promises[$filePath] = $uploader->promise();
        }

        $retries = 0;
        while (true) {
            if ($retries >= $this->transferOptions->getMaxRetriesPerChunk()) {
                throw new ClientException('Exceeded maximum number of retries per chunk upload');
            }
            $results = \GuzzleHttp\Promise\Utils::settle($promises)->wait();
            $rejected = PromiseResultHandler::getRejected($results);
            if (count($rejected) == 0) {
                break;
            }
            $retries++;
            /**
             * @var string $filePath
             * @var S3MultipartUploadException $reason
             */
            foreach ($rejected as $filePath => $reason) {
                $this->logger->notice(sprintf('Uploadfailed: %s, %s, %s, %s', $filePath, $reason->getMessage(), $reason->getCode(), $reason->getKey()));
                $uploader = $this->multipartUploaderFactory(
                    $filePath,
                    $bucket,
                    $reason->getKey(),
                    $acl,
                    count($rejected) > 1 ? $this->transferOptions->getMultiFileConcurrency() : $this->transferOptions->getSingleFileConcurrency(),
                    $encryption ? $encryption : null,
                    $name ? $name : basename($filePath),
                    $reason->getState(),
                );
                $promises[$filePath] = $uploader->promise();
            }
        }
    }

    /**
     * @param string $filePath
     * @param string $bucket
     * @param string $key
     * @param string $acl
     * @param int $concurrency
     * @param string $encryption
     * @param string $name
     * @param UploadState|null $state
     * @return \Aws\S3\MultipartUploader
     */
    private function multipartUploaderFactory(
        $filePath,
        $bucket,
        $key,
        $acl,
        $concurrency,
        $encryption = null,
        $name = null,
        UploadState $state = null
    ) {
        $uploaderOptions = [
            'Bucket' => $bucket,
            'Key' => $key,
            'ACL' => $acl,
            'concurrency' => $concurrency,
            'before_upload' => function (\Aws\Command $command) {
                gc_collect_cycles();
            },
        ];
        if (!empty($state)) {
            $uploaderOptions['state'] = $state;
        }
        $beforeInitiateCommands = [];
        if (!empty($name)) {
            $beforeInitiateCommands['ContentDisposition'] = sprintf('attachment; filename=%s;', $name);
        }
        if (!empty($encryption)) {
            $beforeInitiateCommands['ServerSideEncryption'] = $encryption;
        }
        if ($beforeInitiateCommands) {
            $uploaderOptions['before_initiate'] = function (\Aws\Command $command) use ($beforeInitiateCommands) {
                foreach ($beforeInitiateCommands as $key => $value) {
                    $command[$key] = $value;
                }
            };
        }

        return new \Aws\S3\MultipartUploader($this->s3Client, $filePath, $uploaderOptions);
    }
}
