<?php

namespace Keboola\StorageApi\Downloader;

use Aws\S3\S3Client;

class S3Downloader implements DownloaderInterface
{
    /**
     * @var S3Client
     */
    private $client;

    /**
     * S3Downloader constructor.
     */
    public function __construct(S3Client $client)
    {
        $this->client = $client;
    }

    /**
     * @inheritDoc
     */
    public function downloadFileFromFileResponse($fileResponse, $tmpFilePath)
    {
        $this->client->getObject([
            'Bucket' => $fileResponse["s3Path"]["bucket"],
            'Key' => $fileResponse["s3Path"]["key"],
            'SaveAs' => $tmpFilePath,
        ]);
    }

    /**
     * @inheritDoc
     */
    public function downloadManifestEntry($fileResponse, $entry, $tmpFilePath)
    {
        $fileKey = substr($entry["url"], strpos($entry["url"], '/', 5) + 1);
        $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));

        $this->client->getObject([
            'Bucket' => $fileResponse["s3Path"]["bucket"],
            'Key' => $fileKey,
            'SaveAs' => $filePath,
        ]);

        return $filePath;
    }
}
