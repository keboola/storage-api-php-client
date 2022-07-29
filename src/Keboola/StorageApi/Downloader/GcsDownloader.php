<?php

namespace Keboola\StorageApi\Downloader;

use Google\Cloud\Storage\StorageClient;

class GcsDownloader implements DownloaderInterface
{
    /** @var StorageClient $client */
    private $client;

    public function __construct(StorageClient $client)
    {
        $this->client = $client;
    }

    public function downloadFileFromFileResponse($fileResponse, $tmpFilePath)
    {
        $bucket = $this->client->bucket($fileResponse['gcsPath']['bucket']);
        $object = $bucket->object($fileResponse['gcsPath']['key']);
        file_put_contents($tmpFilePath, $object->downloadAsString());
    }

    public function downloadManifestEntry($fileResponse, $entry, $tmpFilePath)
    {
        $fileKey = substr($entry['url'], strpos($entry['url'], '/', 5) + 1);
        $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));

        $bucket = $this->client->bucket($fileResponse['gcsPath']['bucket']);
        $object = $bucket->object($fileKey);
        file_put_contents($filePath, $object->downloadAsString());

        return $filePath;
    }
}
